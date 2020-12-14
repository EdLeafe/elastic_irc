from datetime import datetime
import sys

import click
from elasticsearch import Elasticsearch


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


HOST = "dodata"
es = Elasticsearch(host=HOST)


@click.command()
@click.option("--chan", "-c", help="Only count records for the specified channel")
def main(chan):
    if chan:
        body = {"query": {"term": {"channel.keyword": chan}}}
        r = es.count(index="irclog", body=body)
    else:
        r = es.count(index="irclog")
    total = r["count"]
    if chan:
        print("There are %s documents in the %s channel." % (total, chan))
    else:
        print("There are %s documents in the index." % total)


if __name__ == "__main__":
    main()
