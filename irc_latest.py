from datetime import datetime
import sys

import click
from rich.console import Console
from rich.table import Table

import utils


def get_latest(num, chan, gerrit=True):
    es = utils.get_elastic_client()
    if chan or gerrit:
        body = {"query": {"bool": {}}}
        if chan:
            body["query"]["bool"]["must"] = {"match": {"channel": chan}}
        if gerrit:
            body["query"]["bool"]["must_not"] = {
                "bool": {
                    "should": [
                        {"match": {"nick": "openstackgerrit"}},
                        {"match": {"nick": "opendevreview"}},
                    ]
                }
            }
        r = es.search(index="irclog", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="irclog", size=num, sort="posted:desc")

    records = utils.extract_records(r)
    utils.massage_date_records(records, "posted")
    return records


def print_output(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    #    table.add_column("ID", style="dim", width=13)
    table.add_column("Channel")
    table.add_column("Posted")
    table.add_column("Nick", justify="right", style="bold")
    table.add_column("Remark")
    for rec in recs:
        #        table.add_row(rec["id"], rec["channel"], rec["posted"], rec["nick"], rec["remark"])
        table.add_row(rec["channel"], rec["posted"], rec["nick"], rec["remark"])
    console.print(table)


@click.command()
@click.option("--channel", "-c", default="", help="Only return records for the specified channel")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
@click.option("--gerrit", "-g", is_flag=True, help="Show messages from openstackgerrit bot")
def main(channel, number, gerrit):
    recs = get_latest(number, channel, gerrit)
    print_output(recs)


if __name__ == "__main__":
    main()
