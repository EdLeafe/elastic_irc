from datetime import datetime

import click

import utils


TIMEOUT = 5
es = utils.get_elastic_client()


@click.command()
@click.option("--chan", "-c", help="Only count records for the specified channel")
def main(chan):
    if chan:
        body = {"query": {"term": {"channel.keyword": chan}}}
        r = es.count(index="irclog", body=body, params={"request_timeout": TIMEOUT})
    else:
        r = es.count(index="irclog", params={"request_timeout": TIMEOUT})
    total = r["count"]
    if chan:
        print(f"There are {utils.format_number(total)} documents in the {chan} channel.")
    else:
        print(f"There are {utils.format_number(total)} documents in the index.")


if __name__ == "__main__":
    main()
