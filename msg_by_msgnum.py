import click
from rich import box
from rich.console import Console
from rich.table import Table

import utils


ABBREV_MAP = {
    "p": "ProFox",
    "l": "ProLinux",
    "y": "ProPython",
    "d": "Dabo-dev",
    "u": "Dabo-users",
    "c": "Codebook",
}
es = utils.get_elastic_client()


def print_rec(rec):
    console = Console()
    table = Table()
    table = Table(show_header=False, box=box.HEAVY)
    table.add_column("", justify="right", style="bold")
    table.add_column("")
    table.add_row("Message #", str(rec["msg_num"]))
    table.add_row("List", ABBREV_MAP[rec["list_name"]])
    table.add_row("From", rec["from"])
    table.add_row("Posted", utils.massage_date(rec["posted"]))
    table.add_row("Subject", rec["subject"])
    table.add_row("", "=" * 66, style="cyan")
    table.add_row("", rec["body"])
    console.print(table)


@click.command()
@click.argument("msg_num")
@click.option(
    "--delete",
    "-d",
    "delete",
    default=False,
    is_flag=True,
    help="Delete the record with the supplied message number",
)
def main(msg_num, delete=False):
    mthd = es.delete_by_query if delete else es.search
    kwargs = {"body": {"query": {"match": {"msg_num": msg_num}}}}

    r = mthd(index="email", **kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        recs = utils.extract_records(r)
        if not recs:
            print(f"No match found for msgnum '{msg_num}'")
            return
        print_rec(recs[0])


if __name__ == "__main__":
    main()
