import sys

import click
from rich.console import Console
from rich.table import Table

import utils


@click.command()
@click.argument("field", nargs=-1)
@click.argument("val", nargs=1)
@click.option("--num", "-n", default=10, help="Maximum number of records to return")
@click.option("--chan", "-c", help="Only search records for the specified channel")
def main(field, val, num, chan):
    field = field or "remark"
    if isinstance(field, (tuple, list)):
        field = field[0]
    es = utils.get_elastic_client()

    field_map = {
        "channel": "keyword",
        "nick": "keyword",
        "posted": "keyword",
        "remark": "text",
        "id": "text",
    }

    if field not in field_map:
        print(
            "Unknown field '%s'. Valid field names are: %s"
            % (field, str(list(field_map.keys())))
        )
        sys.exit()

    if field_map[field] == "keyword":
        expr = {"match": {field: val}}
    else:
        expr = {"match_phrase": {field: val}}
    if chan:
        chan_expr = {"term": {"channel": chan}}
        kwargs = {
            "body": {
                "query": {
                    "bool": {
                        "must": expr,
                        "filter": chan_expr,
                    }
                }
            }
        }
    else:
        kwargs = {"body": {"query": expr}}
    kwargs["size"] = num
    kwargs["sort"] = ["posted:desc"]

    import pprint

    pprint.pprint(kwargs)
    r = es.search(index="irclog", **kwargs)
    total = r["hits"]["total"]["value"]
    relation = r["hits"]["total"]["relation"]
    recs = utils.extract_records(r)
    if relation == "eq":
        print("\nThere were {} records found".format(total))
    elif relation == "gte":
        print("\nThere were more than {} records found".format(total))
    if recs:
        print("Here are the {} most recent:".format(min(num, len(recs))))
        console = Console()
        table = Table(show_header=True, header_style="bold cyan")
        #    table.add_column("ID", style="dim", width=13)
        table.add_column("ID", justify="right")
        table.add_column("Posted", justify="right")
        table.add_column("Channel", style="red")
        table.add_column("Nick", justify="right", style="bold yellow")
        table.add_column("Remark")
        for rec in recs:
            #        table.add_row(rec["id"], rec["posted"], rec["channel"], rec["nick"], rec["remark"])
            table.add_row(
                rec["id"],
                utils.massage_date(rec["posted"]),
                rec["channel"],
                rec["nick"],
                rec["remark"],
            )
        console.print(table)


if __name__ == "__main__":
    main()
