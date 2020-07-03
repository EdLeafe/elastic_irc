from datetime import datetime
import sys

from elasticsearch import Elasticsearch

#import trace

def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


HOST = "dodata"
es = Elasticsearch(host=HOST)

chan = sys.argv[1] if len(sys.argv) > 1 else ""
if chan:
    body = {
        "query": {
            "term": {"channel": chan}
            }
        }
    r = es.count(index="irclog", body=body)
else:
    r = es.count(index="irclog")
total = r["count"]
if chan:
    print("There are %s documents in the %s channel." % (total, chan))
else:
    print("There are %s documents in the index." % total)
