from datetime import datetime
import sys

import click
from elasticsearch import Elasticsearch
import utils

HOST = "dodata"


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


def get_latest(num, chan):
    es = Elasticsearch(host=HOST)
    if chan:
        body = {"query": {"match": {"channel": chan}}}
        r = es.search(index="irclog", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="irclog", size=num, sort="posted:desc")

    records = extract_records(r)
    return records


@click.command()
@click.option("--channel", "-c", default="", help="Only return records for the specified channel")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
def main(channel, number):
    recs = get_latest(number, channel)
    print("NUM", len(recs))
    for rec in recs:
        print(rec["id"], rec["channel"], rec["posted"], rec["nick"], rec["remark"])


if __name__ == "__main__":
    main()
