from datetime import datetime
import sys

import click

import utils


ABBREV_MAP = {
    "p": "profox",
    "l": "prolinux",
    "y": "propython",
    "d": "dabo-dev",
    "u": "dabo-users",
    "c": "codebook",
}
LIST_MAP = {val: key for key, val in ABBREV_MAP.items()}


def get_latest(num, list_name):
    es = utils.get_elastic_client()
    if list_name:
        list_abbrev = LIST_MAP.get(list_name, list_name[0])
        body = {"query": {"match": {"list_name": list_abbrev}}}
        r = es.search(index="email", body=body, size=num, sort="posted:desc")
    else:
        r = es.search(index="email", size=num, sort="posted:desc")

    records = utils.extract_records(r)
    utils.massage_date_records(records, "posted")
    return records


@click.command()
@click.option("--list_name", "-l", default="", help="Only return records for the specified list")
@click.option("--number", "-n", default=10, help="How many records to return. Default=10")
def main(list_name, number):
    recs = get_latest(number, list_name)
    utils.print_message_list(recs)


if __name__ == "__main__":
    main()
