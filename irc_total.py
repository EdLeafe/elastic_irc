from datetime import datetime

import click
from rich.console import Console

import utils


LOGFILE = "irc_counts.txt"
TIMEOUT = 5
es = utils.get_elastic_client()
console = Console()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--chan", "-c", help="Only count records for the specified channel")
@click.option("--numeric", "-n", is_flag=True, help="Just print the total with no additional text")
def main(chan, numeric):
    if chan:
        body = {"query": {"term": {"channel": chan}}}
        r = es.count(index="irclog", body=body, params={"request_timeout": TIMEOUT})
    else:
        r = es.count(index="irclog", params={"request_timeout": TIMEOUT})
    total = r["count"]
    if numeric:
        print(total)
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if chan:
        console.print(
            f"There are [magenta bold]{utils.format_number(total)}[/magenta bold] documents in the "
            f"[bold]{chan}[/bold] channel at [yellow bold]{now}[/yellow bold]."
        )
    else:
        console.print(
            f"There are [magenta bold]{utils.format_number(total)}[/magenta bold] documents in the "
            f"index at [yellow bold]{now}[/yellow bold]."
        )


#        with open(LOGFILE, "a") as ff:
#            tstamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#            ff.write(f"{tstamp}\t{total}\n")


if __name__ == "__main__":
    main()
