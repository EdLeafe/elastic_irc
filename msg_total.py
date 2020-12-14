from datetime import datetime
from elasticsearch import Elasticsearch


HOST = "dodata"
es = Elasticsearch(host=HOST)

r = es.count(index="email")
total = r["count"]
print("There are %s documents in the index." % total)
