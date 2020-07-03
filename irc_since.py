import datetime as dt
import sys

from elasticsearch import Elasticsearch

HOST = "dodata"
es = Elasticsearch(host=HOST)


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


delete = False
chan = None
start = sys.argv[1]
args = sys.argv[2:]
if args:
    if "-d" in args:
        delete = True
        args.remove("-d")
    if args:
        chan = args[0]
mthd = es.delete_by_query if delete else es.count

if chan:
    kwargs = {"body": {
                "query": {
                    "bool": {
                        "filter": {
                            "term": {
                                "channel": chan}
                            },
                        "must": {
                            "range" : {
                                "posted" : {"gte": start}}
                            }
                        }
                    }
                }
            }
else:
    kwargs = {"body": {
                "query": {
                    "range" : {"posted" : {"gte": start}}
                }
            },
        }

r = mthd(index="irclog", **kwargs)
if delete:
    print("%s records have been deleted." % r.get("deleted"))
else:
    numrecs = r["count"]
    print("There are %s records since %s." % (numrecs, start))
