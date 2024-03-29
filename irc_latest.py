from datetime import datetime
import json as json_lib
import sys

import click
from rich.console import Console
from rich.table import Table

import utils


def get_latest(num, chan, gerrit=False):
    es = utils.get_elastic_client()
    sort = {"posted": {"order": "desc"}}
    query = {}
    if chan or gerrit:
        query = {"bool": {}}
        if chan:
            query["bool"]["must"] = {"match": {"channel": chan}}
        if gerrit:
            query["bool"]["must_not"] = {
                "bool": {
                    "should": [
                        {"match": {"nick": "openstackgerrit"}},
                        {"match": {"nick": "opendevreview"}},
                        {"match": {"nick": "rdogerrit"}},
                    ]
                }
            }
        r = es.search(index="irclog", query=query, sort=sort, size=num)
    else:
        r = es.search(index="irclog", sort=sort, size=num)

    records = utils.extract_records(r)
    utils.massage_date_records(records, "posted")
    return records


def print_output(recs, plain=False, json=False):
    if plain:
        print(recs)
    elif json:
        print(json_lib.dumps(recs))
    else:
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


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--channel", "-c", default="", help="Only return records for the specified channel")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
@click.option("--gerrit", "-g", is_flag=True, help="Show messages from openstackgerrit bot")
@click.option("--plain", "-p", is_flag=True, help="Output plain ASCII text")
@click.option("--json", "-j", is_flag=True, help="Output JSON")
def main(channel, number, gerrit, plain=False, json=False):
    recs = get_latest(number, channel, gerrit)
    print_output(recs, plain, json)


if __name__ == "__main__":
    main()
