import sys

import click

import utils


es = utils.get_elastic_client()

field_map = {
    "msg_num": "keyword",
    "list_name": "keyword",
    "subject": "text",
    "from": "keyword",
    "posted": "keyword",
    "message_id": "keyword",
    "replyto_id": "keyword",
    "body": "text",
    "id": "id",
}


@click.command()
@click.argument("field", type=click.Choice(field_map.keys()), nargs=-1)
@click.argument("value", nargs=1)
@click.option("--num", "-n", default=10, help="Maximum number of records to return")
def main(field, value, num):
    field = field or "body"
    if isinstance(field, (tuple, list)):
        field = field[0]
    if field_map[field] == "keyword":
        kwargs = {"body": {"query": {"match": {field: value}}}}
    else:
        field = "fulltext_subject" if field == "subject" else field
        kwargs = {"body": {"query": {"match_phrase": {field: value}}}}
    kwargs["body"]["sort"] = {"msg_num": "desc"}

    r = es.search(index="email", size=num, **kwargs)
    recs = utils.extract_records(r)
    count = len(recs)
    if count == num:
        # See how many total matches there are
        count_recs = es.search(index="email", size=0, **kwargs)
        count = count_recs["hits"]["total"]["value"]
    print(
        "Your query",
        "matched" if count < 10000 else "matched more than",
        count,
        "records",
    )
    if recs:
        print(f"Here are the {min(num, len(recs))} most recent:")
        utils.print_message_list(recs)
    else:
        print(f"No matches for '{value}' in field '{field}'")


if __name__ == "__main__":
    main()
