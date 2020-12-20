from datetime import datetime
import sys

import click
from rich.console import Console
from rich.table import Table

import utils


ABBREV_MAP = {
    "p": "profox",
    "l": "prolinux",
    "y": "propython",
    "d": "dabo-dev",
    "u": "dabo-users",
}
LIST_MAP = {val: key for key, val in ABBREV_MAP.items()}


def get_latest(num, list_name):
    es = utils.get_elastic_client()
    if list_name:
        list_abbrev = LIST_MAP.get(list_name, list_name[0])
        body = {"query": {"match": {"list_name": list_abbrev}}}
        r = es.search(index="email", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="email", size=num, sort="posted:desc")

    records = utils.extract_records(r)
    return records


def print_output(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold blue_violet")
    #    table.add_column("ID", style="dim", width=13)
    table.add_column("MSG #")
    table.add_column("List")
    table.add_column("Posted", justify="right")
    table.add_column("From")
    table.add_column("Subject")
    for rec in recs:
        table.add_row(
            str(rec["msg_num"]),
            ABBREV_MAP.get(rec["list_name"]),
            rec["posted"],
            rec["from"],
            rec["subject"],
        )
    console.print(table)


@click.command()
@click.option("--list_name", "-l", default="", help="Only return records for the specified list")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
def main(list_name, number):
    recs = get_latest(number, list_name)
    print_output(recs)


if __name__ == "__main__":
    main()
