import datetime as dt
import sys

from elasticsearch import Elasticsearch

MAX_RECS = 10000
HOST = "dodata"
es = Elasticsearch(host=HOST)


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


delete = False
start = sys.argv[1]
args = sys.argv[2:]
if args:
    if "-d" in args:
        delete = True
        args.remove("-d")
mthd = es.delete_by_query if delete else es.search

kwargs = {"body": {
            "query": {
                "range" : {"posted" : {"gte": start}}
            }
        },
    }

if not delete:
    kwargs["sort"] = ["posted:asc"]
    kwargs["size"] = MAX_RECS

r = mthd("email", **kwargs)
if delete:
    print("%s records have been deleted." % r.get("deleted"))
else:
    numrecs = len(extract_records(r))
    if numrecs == MAX_RECS:
        print("There are at least %s records since %s." % (numrecs, start))
    else:
        print("There are %s records since %s." % (numrecs, start))
