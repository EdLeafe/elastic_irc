from datetime import datetime
from elasticsearch import Elasticsearch


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


HOST = "dodb"
es = Elasticsearch(host=HOST)

r = es.search("email", doc_type="mail", size=0)
total = "{:,}".format(r["hits"]["total"])
print("There are %s documents in the index." % total)
