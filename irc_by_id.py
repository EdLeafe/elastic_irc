import click
from elasticsearch import Elasticsearch

HOST = "dodata"
es = Elasticsearch(host=HOST)


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


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
        recs = extract_records(r)
        print(recs)


if __name__ == "__main__":
    main()
