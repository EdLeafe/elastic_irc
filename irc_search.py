import sys

import click
from rich.console import Console
from rich.table import Table

import utils


def write_to_file(recs, output_file):
    data = ["\t".join([str(item) for item in rec.values()]) for rec in recs]
    with open(output_file, "w") as ff:
        ff.write("\n".join(data))


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument("field", nargs=-1)
@click.argument("val", nargs=1)
@click.option("--num", "-n", default=10, help="Maximum number of records to return")
@click.option("--chan", "-c", help="Only search records for the specified channel")
@click.option(
    "--column",
    "-m",
    multiple=True,
    help="Only include this column. Use multiple times for more than one column.",
)
@click.option("--output", "-o", "output_file")
def main(field, val, num, chan, column, output_file):
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
            "Unknown field '%s'. Valid field names are: %s" % (field, str(list(field_map.keys())))
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
        kwargs = {"query": expr}
    kwargs["size"] = num
    kwargs["sort"] = [{"posted": "desc"}]
    if column:
        kwargs["_source"] = column

    r = es.search(index="irclog", **kwargs)
    total = r["hits"]["total"]["value"]
    relation = r["hits"]["total"]["relation"]
    recs = utils.extract_records(r)
    if output_file:
        write_to_file(recs, output_file)
        return
    if relation == "eq":
        print("\nThere were {} records found".format(total))
    elif relation == "gte":
        kwargs["size"] = 0
        tot = es.search(index="irclog", track_total_hits=True, **kwargs)
        actual_hits = tot["hits"]["total"]["value"]
        print(f"\nThere were exactly {actual_hits} records found")
    if recs:
        print("Here are the {} most recent:".format(min(num, len(recs))))
        console = Console()
        table = Table(show_header=True, header_style="bold cyan")
        # See if there are columns specified
        allow_cols = column if column else ("id", "posted", "channel", "nick", "remark")
        title_dict = {
            "id": "ID",
            "posted": "Posted",
            "channel": "Channel",
            "nick": "Nick",
            "remark": "Remark",
        }
        for col in allow_cols:
            justify = "left" if col == "channel" else "right"
            style = "red" if col == "channel" else "bold yellow" if col == "nick" else None
            table.add_column(title_dict.get(col), justify=justify, style=style)
        for rec in recs:
            row = []
            for col in allow_cols:
                if col == "posted":
                    row.append(utils.massage_date(rec["posted"]))
                else:
                    row.append(rec[col])
            table.add_row(*row)
        console.print(table)


if __name__ == "__main__":
    main()
