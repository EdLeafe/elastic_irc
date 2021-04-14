import datetime
import sys

import click
from elasticsearch.helpers import bulk

import utils

# Get messages in batches of 256
BATCH_SIZE = 256
es_client = utils.get_elastic_client()


# My original names suck, so...
FIELD_MAP = {
    "imsg": "msg_num",
    "clist": "list_name",
    "csubject": "subject",
    "cfrom": "from",
    "tposted": "posted",
    "cmessageid": "message_id",
    "creplytoid": "replyto_id",
    "mtext": "body",
}


def _get_start_msg(arg):
    if not arg:
        return 0
    try:
        return int(arg)
    except ValueError:
        pass
    # Not a message number; see if it's a date.
    try:
        dt = datetime.datetime.strptime(arg, "%Y-%m-%d")
    except ValueError as e:
        raise e(f"'{arg}' is not a valid date")
    # Find the highest date earlier than the given date
    crs = utils.get_cursor()
    sql = "select imsg from archive where tposted > %s order by imsg limit 1"
    crs.execute(sql, (arg,))
    rec = crs.fetchone()
    return rec["imsg"]


def get_data(currmsg=0, verbose=False):
    crs = utils.get_cursor()
    while True:
        if verbose:
            print("CURR", currmsg)
        crs.execute(
            "SELECT * FROM archive WHERE imsg > %s ORDER BY imsg LIMIT %s",
            (currmsg, BATCH_SIZE),
        )
        recs = crs.fetchall()
        if not recs:
            break
        # Set the current message to the highest imsg in the set.
        currmsg = recs[-1]["imsg"]
        for rec in recs:
            if rec["clist"] in "avsgjstm":
                # Old lists we don't need
                continue
            doc = {FIELD_MAP[fld]: val for fld, val in rec.items()}
            doc["fulltext_subject"] = doc["subject"]
            doc["id"] = utils.gen_key(doc)
            yield {
                "_index": "email",
                "_op_type": "index",
                "_id": doc["id"],
                "_source": doc,
            }


@click.command()
@click.option(
    "--start", "-s", help="Start importing messages after this msgnum or date"
)
def main(start):
    currmsg = _get_start_msg(start)
    success, failures = bulk(es_client, get_data(currmsg=currmsg, verbose=True))
    print("SUCCESS:", success)
    print("FAILED:", failures)


if __name__ == "__main__":
    main()
