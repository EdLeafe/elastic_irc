import sys

import click
from elasticsearch import Elasticsearch

from utils import extract_records


HOST = "dodata"
es = Elasticsearch(host=HOST)

field_map = {
    "msg_num": "keyword",
    "list_name": "keyword",
    "subject": "text",
    "from": "keyword",
    "posted": "keyword",
    "message_id": "keyword",
    "replyto_id": "keyword",
    "body": "text",
    "id": "id",
}


@click.command()
@click.argument("field", type=click.Choice(field_map.keys()), nargs=1)
@click.argument("value", nargs=1)
def main(field, value):
    if field_map[field] == "keyword":
        kwargs = {"body": {"query": {"match": {field: value}}}}
    else:
        kwargs = {"body": {"query": {"match_phrase": {field: value}}}}
    kwargs["size"] = 10000
    kwargs["sort"] = ["msg_num:asc"]

    print("KWARGS", kwargs)
    r = es.search(index="email", **kwargs)
    recs = extract_records(r)
    print(recs)
    print("\nThere were %s records found" % len(recs))


if __name__ == "__main__":
    main()
