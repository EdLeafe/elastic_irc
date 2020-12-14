import sys

import click

import utils


@click.command()
@click.argument("field")
@click.argument("val")
@click.option("--size", "-s", default=10, help="Maximum number of records to return")
def main(field, val, size):
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
        kwargs = {"body": {"query": {"match": {field: val}}}}
    else:
        kwargs = {"body": {"query": {"match_phrase": {field: val}}}}
    kwargs["size"] = size
    kwargs["sort"] = ["posted:desc"]

    r = es.search(index="irclog", **kwargs)
    total = r["hits"]["total"]["value"]
    relation = r["hits"]["total"]["relation"]
    # print(r)
    recs = utils.extract_records(r)
    if relation == "eq":
        print("\nThere were {} records found".format(total))
    elif relation == "gte":
        print("\nThere were more than {} records found".format(total))
    if recs:
        print("Here are the {} most recent:".format(size))
        for rec in recs:
            print(rec)


if __name__ == "__main__":
    main()
