import aiohttp
import argparse
import asyncio
import datetime as dt
import json
import os
import re
import time
from urllib.parse import quote
import warnings

import click
from elasticsearch.helpers import bulk

import utils
from utils import logit


es_client = utils.get_elastic_client()

with open("CHANNELS") as ff:
    CHANNELS = [chan.strip() for chan in ff.readlines()]
DEFAULT_START_DATE = dt.date(2017, 1, 1)
NUM_CONCURRENT = 10

URI_PAT = "http://eavesdrop.openstack.org/irclogs/%(esc_chan)s/%(esc_chan)s.%(year)s-%(month)s-%(day)s.log"
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


async def nextdate(day, end_day=None):
    end_day = end_day or dt.date.today()
    while day < end_day:
        yield day
        day += ONEDAY
        await asyncio.sleep(0.01)


def ignore_spam(nick, remark):
    global ignored_nicks
    if any([phrase in remark for phrase in spam_phrases]):
        if nick not in ignored_nicks:
            print(f"ADDING {nick} TO IGNORED NICKS")
            ignored_nicks.add(nick)
    return nick in ignored_nicks


async def get_data(name, start_day, end_day, queue):
    session = aiohttp.ClientSession()
    updates = []
    while True:
        channel = await queue.get()
        print(f"{name}: working on **{channel}**")
        esc_chan = quote(channel)
        async for day in nextdate(start_day, end_day):
            if day.day == 1:
                print(
                    f"{name}: Starting {day.year}-{day.month}-{day.day} for {channel}"
                )
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
                    async with session.get(uri) as resp:
                        status_code = resp.status
                        try:
                            resp_text = await resp.text()
                        except UnicodeDecodeError:
                            resp_text = ""
                    break
                except aiohttp.ClientConnectionError as e:
                    print(f"Failed '{e}'; retrying...")
                    await asyncio.sleep((5 - retries) ** 2)
                    retries -= 1
            if retries == 0 or status_code > 299:
                # Error; skip it
                continue
            text = resp_text.translate(CTRL_CHAR_MAP)
            for ln in text.splitlines():
                if not ln:
                    continue
                try:
                    tm, tx = parse_line(ln)
                except ValueError as e:
                    print(f"Error encountered: {e}")
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

                updates.append(
                    json.dumps({"index": {"_id": doc["id"], "_index": "irclog"}})
                )
                updates.append(json.dumps(doc))
                if len(updates) >= 1000:
                    await post_updates(name, updates, session)
                    updates = []
                await asyncio.sleep(0.01)
        await post_updates(name, updates, session)
        print(f"{name}: calling task_done")
        queue.task_done()
    await session.close()


async def post_updates(name, updates, session):
    data = "\n".join(updates) + "\n"
    async with session.post(
        "http://dodata:9200/irclog/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=data,
        timeout=30,
    ) as resp:
        if resp.status != 200:
            cnt = await resp.content.read()
            print(f"BAD POST: {resp.status} -- {cnt}")


# @click.command()
# @click.option("--start", "-s", help="Start day for parsing. Default=2017-01-01")
# @click.option("--end", "-e", help="End day for parsing. Default=today")
# @click.option("--chan", "-c", help="Only parse records for the specified channel")
async def main(start=None, end=None, chan=None):
    start_day = dt.datetime.strptime(start, "%Y-%m-%d") if start else DEFAULT_START_DATE
    end_day = dt.datetime.strptime(end, "%Y-%m-%d") if end else dt.date.today()
    channels = [chan] if chan else CHANNELS

    queue = asyncio.Queue()
    for channel in channels:
        queue.put_nowait(channel)

    # Create worker tasks to process the queue concurrently.
    tasks = []
    for i in range(NUM_CONCURRENT):
        name = f"Task-{i}"
        print(f">> Creating {name}")
        task = asyncio.create_task(get_data(name, start_day, end_day, queue))
        print(f">> Created {task}")
        tasks.append(task)

    time_started = time.monotonic()
    print(">> Starting await queue.join()")
    await queue.join()
    print(">> Finished await queue.join()")
    elapsed = time.monotonic() - time_started

    for task in tasks:
        print(f">> Canceling {task}")
        task.cancel()
    print(">> Starting gather()")
    await asyncio.gather(*tasks, return_exceptions=True)
    print(">> Finished gather()")

    print(f">> ELAPSED: {elapsed}")


asyncio.run(main())
