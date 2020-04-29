from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import utils

# Get messages in batches of 100
BATCH_SIZE = 100
HOST = "dodata"
es_client = Elasticsearch(host=HOST)

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


def get_data(currmsg=0, verbose=False):
    crs = utils.get_cursor()
    while True:
        if verbose:
            print("CURR", currmsg)
        crs.execute("SELECT * FROM archive WHERE imsg > %s ORDER BY imsg "
                "LIMIT %s", (currmsg, BATCH_SIZE))
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
            yield {"_index": "email",
                    "_type": "mail",
                    "_op_type": "index",
                    "_id": doc["id"],
                    "_source": doc}


if __name__ == "__main__":
    success, failures = bulk(es_client, get_data(verbose=True)) 
    print("SUCCESS:", success)
    print("FAILED:", failures)
