import sys

import click
from rich.console import Console
from rich.table import Table

import utils


ABBREV_MAP = {"p": "profox", "l": "prolinux", "y": "propython", "d": "dabo-dev", "u": "dabo-users", "c": "codebook"}
es = utils.get_elastic_client()

field_map = {
    "msg_num": "keyword",
    "list_name": "keyword",
    "subject": "text",
    "from": "keyword",
    "posted": "keyword",
    "message_id": "keyword",
    "replyto_id": "keyword",
    "body": "text",
    "id": "id",
}


def print_output(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold blue_violet")
#    table.add_column("ID", style="dim", width=13)
    table.add_column("MSG #", justify="right")
    table.add_column("List")
    table.add_column("Posted", justify="right")
    table.add_column("From")
    table.add_column("Subject")
    for rec in recs:
#        table.add_row(rec["id"], str(rec["msg_num"]), ABBREV_MAP.get(rec["list_name"]), rec["posted"], rec["from"], rec["subject"])
        table.add_row(str(rec["msg_num"]), ABBREV_MAP.get(rec["list_name"]), rec["posted"], rec["from"], rec["subject"])
    console.print(table)


@click.command()
@click.argument("field", type=click.Choice(field_map.keys()), nargs=1)
@click.argument("value", nargs=1)
@click.option("--num", "-n", default=10, help="Maximum number of records to return")
def main(field, value, num):
    if field_map[field] == "keyword":
        kwargs = {"body": {"query": {"match": {field: value}}}}
    else:
        field = "fulltext_subject" if field == "subject" else field
        kwargs = {"body": {"query": {"match_phrase": {field: value}}}}
    kwargs["size"] = num
    kwargs["body"]["sort"] = {"msg_num" : "desc"}

    r = es.search(index="email", **kwargs)
    recs = utils.extract_records(r)
    print("RETURNED:", len(recs))
    if recs:
        print("Here are the {} most recent:".format(min(num, len(recs))))
        print_output(recs)
    else:
        print(f"No matches for '{value}' in field '{field}'")


if __name__ == "__main__":
    main()
