import datetime as dt
import sys

import click

import irc_latest
import utils

es = utils.get_elastic_client()


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


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--chan", "-c", help="Only show records for the specified channel")
@click.option("--delete", "-d", default=False, help="Delete the found records")
@click.option("--show", "-s", is_flag=True, help="Show the records and not just the total")
@click.argument("start")
def main(start, chan, delete, show):
    delete = False
    chan = None
    mthd = es.delete_by_query if delete else es.count

    if chan:
        kwargs = {
            "body": {
                "query": {
                    "bool": {
                        "filter": {"term": {"channel": chan}},
                        "must": {"range": {"posted": {"gte": start}}},
                    }
                }
            }
        }
    else:
        kwargs = {
            "body": {"query": {"range": {"posted": {"gte": start}}}},
        }

    r = mthd(index="irclog", **kwargs)
    if delete:
        print("%s records have been deleted." % r.get("deleted"))
    else:
        numrecs = r["count"]
        print("There are %s records since %s." % (numrecs, start))
        if show:
            recs = irc_latest.get_latest(numrecs, "", False)
            irc_latest.print_output(recs)


if __name__ == "__main__":
    main()
