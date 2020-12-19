import click

import utils


ABBREV_MAP = {"p": "profox", "l": "prolinux", "y": "propython", "d": "dabo-dev", "u": "dabo-users", "c": "codebook"}
HOST = "dodata"
es = utils.get_elastic_client()


def print_rec(rec):
    print(f"Message #: {rec['msg_num']}")
    print(f"     List: {ABBREV_MAP[rec['list_name']]}")
    print(f"     From: {rec['from']}")
    print(f"   Posted: {rec['posted']}")
    print(f"  Subject: {rec['subject']}")
    print()
    print(rec["body"])


@click.command()
@click.argument("msg_num")
@click.option(
    "--delete",
    "-d",
    "delete",
    default=False,
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
        print_rec(recs[0])


if __name__ == "__main__":
    main()
