from datetime import datetime

import click

import utils

es = utils.get_elastic_client()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
def main():
    r = es.count(index="email")
    total = r["count"]
    print(
        f"There are {utils.format_number(total)} documents in the index at {datetime.now().strftime('%H:%M:%S')}."
    )


if __name__ == "__main__":
    main()
