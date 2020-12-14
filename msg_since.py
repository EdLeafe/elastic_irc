import datetime as dt
import sys

import click
from elasticsearch import Elasticsearch

from utils import extract_records


MAX_RECS = 10000
HOST = "dodata"
es = Elasticsearch(host=HOST)


@click.command()
@click.argument("start")
@click.option("--delete", "-d", is_flag=True, help="Delete all records since the given date")
def main(start, delete):
    mthd = es.delete_by_query if delete else es.search
    kwargs = {"query": {"range": {"posted": {"gte": start}}}}
    if not delete:
        kwargs["sort"] = [{"posted": "asc"}]
        kwargs["size"] = MAX_RECS

    r = mthd(index="email", body=kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        numrecs = len(extract_records(r))
        if numrecs == MAX_RECS:
            print("There are at least %s records since %s." % (numrecs, start))
        else:
            print("There are %s records since %s." % (numrecs, start))


if __name__ == "__main__":
    main()
