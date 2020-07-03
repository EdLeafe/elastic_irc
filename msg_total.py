from datetime import datetime
from elasticsearch import Elasticsearch


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


HOST = "dodata"
es = Elasticsearch(host=HOST)

r = es.count(index="email")
total = r["count"]
print("There are %s documents in the index." % total)
