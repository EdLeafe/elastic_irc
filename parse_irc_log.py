import argparse
import datetime as dt
import os
import re
import time
from urllib.parse import quote
import warnings

from elasticsearch.helpers import bulk
import requests
import utils
from utils import logit


es_client = utils.get_elastic_client()


with open("CHANNELS") as ff:
    CHANNELS = [chan.strip() for chan in ff.readlines()]
DEFAULT_START_DATE = dt.date(2017, 1, 1)
# URI_PAT = "http://eavesdrop.openstack.org/irclogs/%(esc_chan)s/%(esc_chan)s.%(year)s-%(month)s-%(day)s.log"
URI_PAT = (
    "https://meetings.opendev.org/irclogs/%(esc_chan)s/%(esc_chan)s.%(year)s-%(month)s-%(day)s.log"
)
NICK_PAT = re.compile(r"<([^>]+)> (.*)")
ONEDAY = dt.timedelta(days=1)
ctl_chars = dict.fromkeys(range(32))
del ctl_chars[10], ctl_chars[13]
CTRL_CHAR_MAP = ctl_chars

last_channel = None

SPAM_FILE = os.path.join(os.getcwd(), "SPAM_PHRASES")
ignored_nicks = set()
with open(SPAM_FILE, "r") as ff:
    spam_phrases = [ln for ln in ff.read().splitlines() if ln]


def from_time(val):
    return dt.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")


def parse_line(ln):
    return (from_time(ln[:19]), ln[21:])


def nextdate(day, end_day=None):
    end_day = end_day or dt.date.today()
    while day < end_day:
        yield day
        day += ONEDAY


def ignore_spam(nick, remark):
    global ignored_nicks
    if any([phrase in remark for phrase in spam_phrases]):
        if nick not in ignored_nicks:
            print("ADDING %s TO IGNORED NICKS" % nick)
            ignored_nicks.add(nick)
    return nick in ignored_nicks


def get_data(start_day, end_day=None, chan=None, exclusive=False):
    global last_channel
    idx = 0
    if chan:
        idx = CHANNELS.index(chan)
    chans = [CHANNELS[idx]] if exclusive else CHANNELS[idx:]
    good2go = False
    for channel in chans:
        good2go = True
        # Use this for when an import fails to avoid re-doing the completed
        # channels. I added this when the import crashed while importing the
        # #openstack-infra channel
        #        if channel == "#openstack-infra":
        #            good2go = True
        if not good2go:
            print("Skipping", channel)
            continue
        # In case this crashes, we know where to re-start
        last_channel = channel
        esc_chan = quote(channel)
        for day in nextdate(start_day, end_day):
            print("Starting %s-%s-%s for %s" % (day.year, day.month, day.day, channel))
            vals = {
                "esc_chan": esc_chan,
                "year": day.year,
                "month": str(day.month).zfill(2),
                "day": str(day.day).zfill(2),
            }
            uri = URI_PAT % vals
            retries = 3
            while retries:
                try:
                    resp = requests.get(uri, timeout=10)
                    break
                except requests.exceptions.ConnectionError:
                    print("Failed {}; retrying...")
                    time.sleep((5 - retries) ** 2)
                    retries -= 1
            if retries == 0 or resp.status_code > 299:
                # Error; skip it
                continue
            text = resp.text.translate(CTRL_CHAR_MAP)
            for ln in text.splitlines():
                if not ln:
                    continue
                try:
                    tm, tx = parse_line(ln)
                except ValueError as e:
                    print("Error encountered: %s" % e)
                    continue
                mtch = NICK_PAT.match(tx)
                if not mtch:
                    # JOINS, QUITS, etc.
                    continue
                nick, remark = mtch.groups()
                if ignore_spam(nick, remark):
                    continue

                doc = {
                    "channel": channel,
                    "posted": tm.strftime("%Y-%m-%dT%H:%M:%S"),
                    "nick": nick,
                    "remark": remark,
                }
                doc["id"] = utils.gen_key(doc)
                yield {
                    "_index": "irclog",
                    "_op_type": "index",
                    "_id": doc["id"],
                    "_source": doc,
                }


def import_irc(start_day, end_day=None, chan=None, exclusive=False):
    global last_channel
    last_channel = chan or last_channel
    start_time = dt.datetime.now()
    while True:
        try:
            success, failures = bulk(
                es_client,
                get_data(start_day, end_day, chan=last_channel, exclusive=exclusive),
                max_retries=5,
            )
            break
        except Exception:
            pass
    print("SUCCESS", success)
    print("FAILS", failures)
    print("Elapsed:", dt.datetime.now() - start_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IRC Log Parsing")
    parser.add_argument("--start", "-s", action="append", help="Start date for log parsing.")
    parser.add_argument(
        "--end",
        "-e",
        action="append",
        help="End date for log parsing. Optional; defaults to today.",
    )
    parser.add_argument("--chan", "-c", help="Channel to start parsing (alphabetically)")
    parser.add_argument(
        "--exclusive",
        "-x",
        action="store_true",
        help="Only parse the specified channel (requires --chan)",
    )
    args = parser.parse_args()
    if args.start:
        start_day = dt.datetime.strptime(args.start[0], "%Y-%m-%d").date()
    else:
        start_day = DEFAULT_START_DATE
    if args.end:
        end_day = dt.datetime.strptime(args.end[0], "%Y-%m-%d").date()
    else:
        end_day = dt.date.today()
    import_irc(start_day, end_day, chan=args.chan, exclusive=args.exclusive)
