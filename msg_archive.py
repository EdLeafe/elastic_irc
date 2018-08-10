from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import utils

# Get messages in batches of 50
BATCH_SIZE = 50
HOST = "dodb"
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


def get_data(currmsg=0):
    crs = utils.get_cursor()
    # Get max msg
    crs.execute("SELECT MAX(imsg) as maxmsg FROM archive;")
    maxmsg = crs.fetchone()["maxmsg"] + 10

    while currmsg < maxmsg:
        print("CURR", currmsg)
        crs.execute("SELECT * FROM archive WHERE imsg >= %s AND imsg < %s",
                (currmsg, currmsg+BATCH_SIZE))
        currmsg += BATCH_SIZE
        recs = crs.fetchall()
        for rec in recs:
            if rec["clist"] in "avsgjstm":
                # Old lists we don't need
                continue
            doc = {FIELD_MAP[fld]: val for fld, val in rec.items()}
            yield {"_index": "email",
                    "_type": "mail",
                    "_source": doc}


if __name__ == "__main__":
    success, failures = bulk(es_client, get_data()) 
    print("SUCCESS:", success)
    print("FAILED:", failures)
