from datetime import datetime
import sys

import click
from rich.console import Console
from rich.table import Table

import utils

HOST = "dodata"


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


def get_latest(num, chan):
    es = utils.get_elastic_client()
    if chan:
        body = {"query": {"match": {"channel": chan}}}
        r = es.search(index="irclog", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="irclog", size=num, sort="posted:desc")

    records = extract_records(r)
    return records


def print_output(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
#    table.add_column("ID", style="dim", width=13)
    table.add_column("Channel")
    table.add_column("Posted", justify="right")
    table.add_column("Nick", justify="right", style="bold")
    table.add_column("Remark")
    for rec in recs:
#        table.add_row(rec["id"], rec["channel"], rec["posted"], rec["nick"], rec["remark"])
        table.add_row(rec["channel"], rec["posted"], rec["nick"], rec["remark"])
    console.print(table)


@click.command()
@click.option("--channel", "-c", default="", help="Only return records for the specified channel")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
def main(channel, number):
    recs = get_latest(number, channel)
    print_output(recs)
#    print("NUM", len(recs))
#    for rec in recs:
#        print(rec["id"], rec["channel"], rec["posted"], rec["nick"], rec["remark"])


if __name__ == "__main__":
    main()
