from datetime import datetime

import click
from rich.console import Console

import utils

es = utils.get_elastic_client()
console = Console()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
def main():
    r = es.count(index="email")
    total = r["count"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    console.print(
        f"There are [magenta bold]{utils.format_number(total)}[/magenta bold] documents in the "
        f"index at [yellow bold]{now}[/yellow bold]."
    )


if __name__ == "__main__":
    main()
