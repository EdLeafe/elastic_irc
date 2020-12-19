import datetime as dt
import sys

import click

import utils

es = utils.get_elastic_client()


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


@click.command()
@click.option("--chan", "-c", help="Only show records for the specified channel")
@click.option("--delete", "-d", default=False, help="Delete the found records")
@click.argument("start")
def main(start, chan, delete):
    delete = False
    chan = None
    start = sys.argv[1]
    args = sys.argv[2:]
    if args:
        if "-d" in args:
            delete = True
            args.remove("-d")
        if args:
            chan = args[0]
    mthd = es.delete_by_query if delete else es.count

    if chan:
        kwargs = {
            "body": {
                "query": {
                    "bool": {
                        "filter": {"term": {"channel": chan}},
                        "must": {"range": {"posted": {"gte": start}}},
                    }
                }
            }
        }
    else:
        kwargs = {
            "body": {"query": {"range": {"posted": {"gte": start}}}},
        }

    r = mthd(index="irclog", **kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        numrecs = r["count"]
        print("There are %s records since %s." % (numrecs, start))


if __name__ == "__main__":
    main()
