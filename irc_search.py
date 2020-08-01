import sys

import utils

es = utils.get_elastic_client()

field_map = {
        "channel": "keyword",
        "nick": "keyword",
        "posted": "keyword",
        "remark": "text",
        "id": "text",
        }

if len(sys.argv) < 3:
    print("You must supply a field to search and a value")
    sys.exit()

field = sys.argv[1]
val = sys.argv[2]
if field not in field_map:
    print("Unknown field '%s'. Valid field names are: %s" %
            (field, str(list(field_map.keys()))))
    sys.exit()

size = sys.argv[3] if len(sys.argv) > 3 else 10

if field_map[field] == "keyword":
    kwargs = {"body": {"query": {"match" : {field: val}}}}
else:
    kwargs = {"body": {"query": {"match_phrase" : {field: val}}}}
kwargs["size"] = size
kwargs["sort"] = ["posted:desc"]

r = es.search(index="irclog", **kwargs)
total = r["hits"]["total"]["value"]
relation = r["hits"]["total"]["relation"]
#print(r)
recs = utils.extract_records(r)
if relation == "eq":
    print("\nThere were {} records found".format(total))
elif relation == "gte":
    print("\nThere were more than {} records found".format(total))
if recs:
    print("Here are the {} most recent:".format(size))
    for rec in recs:
        print(rec)

#print(kwargs)
