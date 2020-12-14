from datetime import datetime
import sys

import click
from elasticsearch import Elasticsearch

from utils import extract_records


HOST = "dodata"
ABBREV_MAP = {"p": "profox", "l": "prolinux", "y": "propython", "d": "dabo-dev", "u": "dabo-users"}
LIST_MAP = {val: key for key, val in ABBREV_MAP.items()}


def get_latest(num, list_name):
    es = Elasticsearch(host=HOST)
    if list_name:
        list_abbrev = LIST_MAP.get(list_name, list_name[0])
        body = {"query": {"match": {"list_name": list_abbrev}}}
        r = es.search(index="email", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="email", size=num, sort="posted:desc")

    records = extract_records(r)
    return records


@click.command()
@click.option("--list_name", "-l", default="", help="Only return records for the specified list")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
def main(list_name, number):
    recs = get_latest(number, list_name)
    print("NUM", len(recs))
    for rec in recs:
        print("     ID:", rec["id"])
        print("MSG_NUM:", rec["msg_num"])
        print("   LIST:", ABBREV_MAP.get(rec["list_name"]))
        print("SUBJECT:", rec["subject"])
        print("   FROM:", rec["from"])
        print(" POSTED:", rec["posted"])
        print(" MSG_ID:", rec["message_id"])
        print("")


if __name__ == "__main__":
    main()
