from datetime import datetime
import sys

from elasticsearch import Elasticsearch
import utils

HOST = "dodata"


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


def get_latest(num):
    es = Elasticsearch(host=HOST)
    r = es.search("email", size=num, sort="posted.keyword:desc")
    records = extract_records(r)
    return records


if __name__ == "__main__":
    recs = get_latest(10)
    print("NUM", len(recs))
    for rec in recs:
        print(rec["id"], rec["msg_num"], rec["list_name"], rec["subject"],
                rec["from"], rec["posted"], rec["message_id"],
                rec["replyto_id"], "\n")
