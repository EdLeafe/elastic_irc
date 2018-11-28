import sys

from elasticsearch import Elasticsearch

HOST = "dodb"
es = Elasticsearch(host=HOST)


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]

delete = False
msgnum = sys.argv[1]
args = sys.argv[2:]
if args:
    if "-d" in args:
        delete = True
        args.remove("-d")
mthd = es.delete_by_query if delete else es.search

kwargs = {"body": {"query": {"match" : {"msg_num" : msgnum}}}}

r = mthd("email", **kwargs)
if delete:
    print("%s records have been deleted." % r.get("deleted"))
else:
    recs = extract_records(r)
    print(recs)
