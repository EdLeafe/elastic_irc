import datetime as dt
import sys

from elasticsearch import Elasticsearch

MAX_RECS = 10000
HOST = "dodata"
es = Elasticsearch(host=HOST)


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]

delete = False
chan = None
logdate = sys.argv[1]
conv_date = dt.datetime.strptime(logdate, "%Y-%m-%d")
conv_next = conv_date + dt.timedelta(days=1)
nextdate = conv_next.strftime("%Y-%m-%d")
args = sys.argv[2:]
if args:
    if "-d" in args:
        delete = True
        args.remove("-d")
    if args:
        chan = args[0]
mthd = es.delete_by_query if delete else es.search

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
                                "posted" : {"gte": logdate, "lt": nextdate}}
                            }
                        }
                    }
                }
            }
else:
    kwargs = {"body": {
                "query": {
                    "range" : {"posted" : {"gte": logdate, "lt": nextdate}}
                }
            },
        }

kwargs["size"] = MAX_RECS
if not delete:
    kwargs["sort"] = ["posted:asc"]

r = mthd("irclog", **kwargs)
if delete:
    print("%s records have been deleted." % r.get("deleted"))
else:
    numrecs = len(extract_records(r))
    if numrecs == MAX_RECS:
        print("There are at least %s records on %s." % (numrecs, logdate))
    else:
        print("There are %s records on %s." % (numrecs, logdate))
