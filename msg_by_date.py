import datetime as dt
import sys

import click
from elasticsearch import Elasticsearch
from utils import extract_records

MAX_RECS = 10000
HOST = "dodata"
es = Elasticsearch(host=HOST)


@click.command()
# @click.option("--delete", "-d", help="Delete the records on the specified date")
@click.argument("logdate")
def main(logdate, delete=False):
    conv_date = dt.datetime.strptime(logdate, "%Y-%m-%d")
    conv_next = conv_date + dt.timedelta(days=1)
    nextdate = conv_next.strftime("%Y-%m-%d")
    mthd = es.delete_by_query if delete else es.search

    kwargs = {
        "body": {"query": {"range": {"posted": {"gte": logdate, "lt": nextdate}}}},
    }
    kwargs["size"] = MAX_RECS
    r = mthd(index="email", **kwargs)
    if delete:
        print(f"{r.get('deleted')} records have been deleted.")
    else:
        numrecs = len(extract_records(r))
        if numrecs == MAX_RECS:
            print(f"There are at least {numrecs} records on {logdate}.")
        else:
            print(f"There are {numrecs} records on {logdate}.")


if __name__ == "__main__":
    main()
