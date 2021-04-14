import datetime as dt
import sys

import click

import utils

MAX_RECS = 10000
es = utils.get_elastic_client()


@click.command()
@click.option("--delete", "-d", help="Delete the records on the specified date")
@click.argument("logdate")
@click.option(
    "--show",
    "-s",
    is_flag=True,
    help="Show the message information instead of just the counts",
)
def main(logdate, delete=False, show=False):
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
        recs = utils.extract_records(r)
        numrecs = len(recs)
        if numrecs == MAX_RECS:
            print(f"There are at least {numrecs} records on {logdate}.")
        else:
            print(f"There are {numrecs} records on {logdate}.")
        if show:
            utils.print_messages(recs)


if __name__ == "__main__":
    main()
