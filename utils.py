import copy
from datetime import datetime
from functools import wraps, update_wrapper
from hashlib import blake2b
import logging
from math import log
import os
from subprocess import Popen, PIPE
import uuid

from dateutil import parser
import elasticsearch
import pymysql
from rich import box
from rich.console import Console
from rich.table import Table


main_cursor = None
HOST = "dodata"
conn = None
CURDIR = os.getcwd()

LOG = logging.getLogger(__name__)
ABBREV_MAP = {
    "p": "profox",
    "l": "prolinux",
    "y": "propython",
    "d": "dabo-dev",
    "u": "dabo-users",
    "c": "codebook",
}
NAME_COLOR = "bright_red"

IntegrityError = pymysql.err.IntegrityError


def runproc(cmd):
    proc = Popen([cmd], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    stdout_text, stderr_text = proc.communicate()
    return stdout_text, stderr_text


def _parse_creds():
    fpath = os.path.expanduser("~/.dbcreds")
    with open(fpath) as ff:
        lines = ff.read().splitlines()
    ret = {}
    for ln in lines:
        key, val = ln.split("=")
        ret[key] = val
    return ret


def connect():
    cls = pymysql.cursors.DictCursor
    creds = _parse_creds()
    db = creds.get("DB_NAME") or "webdata"
    ret = pymysql.connect(
        host=HOST,
        user=creds["DB_USERNAME"],
        passwd=creds["DB_PWD"],
        db=db,
        charset="utf8",
        cursorclass=cls,
    )
    return ret


def gen_uuid():
    return str(uuid.uuid4())


def get_cursor():
    global conn, main_cursor
    if not (conn and conn.open):
        LOG.debug("No DB connection")
        main_cursor = None
        conn = connect()
    if not main_cursor:
        LOG.debug("No cursor")
        main_cursor = conn.cursor(pymysql.cursors.DictCursor)
    return main_cursor


def commit():
    conn.commit()


def logit(*args):
    argtxt = [str(arg) for arg in args]
    msg = "  ".join(argtxt) + "\n"
    with open("LOGOUT", "a") as ff:
        ff.write(msg)


def debugout(*args):
    with open("/tmp/debugout", "a") as ff:
        ff.write("YO!")
    argtxt = [str(arg) for arg in args]
    msg = "  ".join(argtxt) + "\n"
    with open("/tmp/debugout", "a") as ff:
        ff.write(msg)


def nocache(view):
    @wraps(view)
    def no_cache(*args, **kwargs):
        response = make_response(view(*args, **kwargs))
        response.headers["Last-Modified"] = datetime.now()
        response.headers["Cache-Control"] = (
            "no-store, no-cache, " "must-revalidate, post-check=0, pre-check=0, max-age=0"
        )
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "-1"
        return response

    return update_wrapper(no_cache, view)


def human_fmt(num):
    """Human friendly file size"""
    # Make sure that we get a valid input. If an invalid value is passed, we
    # want the exception to be raised.
    num = int(num)
    units = list(zip(["bytes", "K", "MB", "GB", "TB", "PB"], [0, 0, 1, 2, 2, 2]))
    if num > 1:
        exponent = min(int(log(num, 1024)), len(units) - 1)
        quotient = float(num) / 1024 ** exponent
        unit, num_decimals = units[exponent]
        format_string = "{:.%sf} {}" % (num_decimals)
        return format_string.format(quotient, unit)
    if num == 0:
        return "0 bytes"
    if num == 1:
        return "1 byte"


def format_number(num):
    """Return a number representation with comma separators."""
    snum = str(num)
    parts = []
    while snum:
        snum, part = snum[:-3], snum[-3:]
        parts.append(part)
    parts.reverse()
    return ",".join(parts)


def get_elastic_client():
    return elasticsearch.Elasticsearch(host=HOST)


def _get_mapping():
    es_client = get_elastic_client()
    return es_client.indices.get_mapping()


def get_indices():
    return list(_get_mapping().keys())


def get_mapping(index):
    """Returns the field definitions for the specified index"""
    props = _get_mapping().get(index, {}).get("mappings", {}).get("properties", {})
    return props


def get_fields(index):
    """Returns just the field names for the specified index"""
    return get_mapping(index).keys()


def gen_key(orig_rec, digest_size=8):
    """Generates a hash value by concatenating the values in the dictionary."""
    # Don't modify the original dict
    rec = copy.deepcopy(orig_rec)
    # Remove the 'id' field, if present
    rec.pop("id", None)
    m = blake2b(digest_size=digest_size)
    txt_vals = ["%s" % val for val in rec.values()]
    txt_vals.sort()
    txt = "".join(txt_vals)
    m.update(txt.encode("utf-8"))
    return m.hexdigest()


def extract_records(resp):
    return [r["_source"] for r in resp["hits"]["hits"]]


def massage_date(val):
    dt = parser.parse(val)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def massage_date_records(records, field_name):
    for rec in records:
        rec[field_name] = massage_date(rec[field_name])


def print_messages(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold blue_violet")
    table.add_column("MSG #", justify="right")
    table.add_column("List")
    table.add_column("Posted", justify="right")
    table.add_column("From")
    table.add_column("Subject")
    for rec in recs:
        table.add_row(
            str(rec["msg_num"]),
            ABBREV_MAP.get(rec["list_name"]),
            massage_date(rec["posted"]),
            rec["from"],
            rec["subject"],
        )
    console.print(table)


def print_message_list(recs):
    console = Console()
    table = Table(show_header=True, header_style="bold cyan", box=box.HEAVY)
    #    table.add_column("ID", style="dim", width=13)
    table.add_column("MSG #")
    table.add_column("List")
    table.add_column("Posted")
    table.add_column("From")
    table.add_column("Subject")
    for rec in recs:
        sender_parts = rec["from"].split("<")
        name = sender_parts[0]
        addr = f"<{sender_parts[1]}" if len(sender_parts) > 1 else ""
        sender = f"[bold {NAME_COLOR}]{name}[/bold {NAME_COLOR}]{addr}"
        subj = rec["subject"]
        low_subj = subj.lower()
        if low_subj.startswith("re:") or low_subj.startswith("aw:"):
            subj = f"[green]{subj[:3]}[/green]{subj[3:]}"
        table.add_row(
            str(rec["msg_num"]),
            ABBREV_MAP.get(rec["list_name"]),
            rec["posted"],
            sender,
            subj,
        )
    console.print(table)
