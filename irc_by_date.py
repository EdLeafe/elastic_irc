import datetime as dt
import sys

import click

import utils

MAX_RECS = 10000
es = utils.get_elastic_client()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--chan", "-c", help="Only count record for the specified channel")
@click.option("--delete", "-d", help="Delete the records on the specified date")
@click.argument("logdate")
def main(logdate, chan="", delete=False):
    conv_date = dt.datetime.strptime(logdate, "%Y-%m-%d")
    conv_next = conv_date + dt.timedelta(days=1)
    nextdate = conv_next.strftime("%Y-%m-%d")
    mthd = es.delete_by_query if delete else es.search

    if chan:
        kwargs = {
            "body": {
                "query": {
                    "bool": {
                        "filter": {"term": {"channel": chan}},
                        "must": {"range": {"posted": {"gte": logdate, "lt": nextdate}}},
                    }
                }
            }
        }
    else:
        kwargs = {
            "body": {"query": {"range": {"posted": {"gte": logdate, "lt": nextdate}}}},
        }

    kwargs["size"] = MAX_RECS
    if not delete:
        kwargs["sort"] = ["posted:asc"]
        kwargs["_source"] = ["id"]

    r = mthd(index="irclog", **kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        recs = utils.extract_records(r)
        numrecs = len(recs)
        if numrecs == MAX_RECS:
            print("There are at least %s records on %s." % (numrecs, logdate))
        else:
            print("There are %s records on %s." % (numrecs, logdate))


if __name__ == "__main__":
    main()
