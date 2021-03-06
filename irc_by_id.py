import click
from rich import box
from rich.console import Console
from rich.table import Table

import utils

es = utils.get_elastic_client()


@click.command()
@click.argument("log_id")
@click.option(
    "--delete", "-d", "delete", default=False, help="Delete the record with the supplied ID"
)
def main(log_id, delete=False):
    mthd = es.delete_by_query if delete else es.search

    kwargs = {"body": {"query": {"match": {"id": log_id}}}}

    r = mthd(index="irclog", **kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        recs = utils.extract_records(r)
        console = Console()
        table = Table()
        table = Table(show_header=False, box=box.HEAVY)
        table.add_column("", justify="right")
        table.add_column("")
        for rec in recs:
            # Should only be 1 record, but still...
            table.add_row("Posted", utils.massage_date(rec["posted"]))
            table.add_row("Channel", rec["channel"])
            table.add_row("Nick", rec["nick"])
            table.add_row("Remark", rec["remark"])
        console.print(table)


if __name__ == "__main__":
    main()
