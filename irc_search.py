import sys

from elasticsearch import Elasticsearch

import utils

HOST = "dodata"
es = Elasticsearch(host=HOST)

field_map = {
        "channel": "keyword",
        "nick": "keyword",
        "posted": "keyword",
        "remark": "text",
        "id": "text",
        }

if len(sys.argv) != 3:
    print("You must supply a field to search and a value")
    sys.exit()

field = sys.argv[1]
val = sys.argv[2]
if field not in field_map:
    print("Unknown field '%s'. Valid field names are: %s" %
            (field, str(list(field_map.keys()))))
    sys.exit()

if field_map[field] == "keyword":
    kwargs = {"body": {"query": {"match" : {field: val}}}}
else:
    kwargs = {"body": {"query": {"match_phrase" : {field: val}}}}
kwargs["size"] = 10
kwargs["sort"] = ["posted:asc"]

r = es.search("irclog", **kwargs)
print(r)
recs = utils.extract_records(r)
print(recs)
print("\nThere were %s records found" % len(recs))
print(kwargs)
