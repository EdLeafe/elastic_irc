from datetime import datetime
import sys

from elasticsearch import Elasticsearch

HOST = "dodb"


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


def get_latest(num, chan):
    es = Elasticsearch(host=HOST)
    if chan:
        body = {"query": {"match" : {"channel" : chan}}}
        r = es.search("irclog", body=body, size=num, sort="posted:desc")
    else:
        r = es.search("irclog", size=num, sort="posted:desc")

    records = extract_records(r)
    return records


if __name__ == "__main__":
    chan = sys.argv[1] if len(sys.argv) > 1 else ""
    recs = get_latest(5, chan)
    for rec in recs:
        print(rec["posted"], rec["nick"], rec["remark"])
