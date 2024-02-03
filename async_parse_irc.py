import asyncio
import datetime as dt
import json
import os
import re
import time
from urllib.parse import quote

import click
import httpx

import utils


es_client = utils.get_elastic_client()
print_output = True


def output(*msgs):
    if print_output:
        print(" ".join(msgs))


DEFAULT_START_DATE = dt.datetime(2020, 1, 1)
NUM_CONCURRENT = 20
TIMEOUT = 30

URI_PAT = (
    "https://meetings.opendev.org/irclogs/%(esc_chan)s/%(esc_chan)s.%(year)s-%(month)s-%(day)s.log"
)
NICK_PAT = re.compile(r"<([^>]+)> (.*)")
CHANNEL_URI = "https://meetings.opendev.org/irclogs/"
CHANNEL_PAT = re.compile(r'<a href="(%23[a-z0-9_-]+)/">')
ONEDAY = dt.timedelta(days=1)
ctl_chars = dict.fromkeys(range(32))
del ctl_chars[10], ctl_chars[13]
CTRL_CHAR_MAP = ctl_chars

last_channel = None

SPAM_FILE = os.path.join(os.getcwd(), "SPAM_PHRASES")
ignored_nicks = set()
with open(SPAM_FILE, "r") as ff:
    spam_phrases = [ln for ln in ff.read().splitlines() if ln]


def _get_channels():
    channels = []
    resp = httpx.get(CHANNEL_URI)
    for ln in resp.text.splitlines():
        mtch = CHANNEL_PAT.search(ln)
        if mtch:
            chan = mtch.groups()[0].replace("%23", "#")
            channels.append(chan)
    return channels


def from_time(val):
    return dt.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")


def parse_line(ln):
    return (from_time(ln[:19]), ln[21:])


async def nextdate(day, end_day=None):
    end_day = end_day or dt.datetime.now()
    while day < end_day:
        yield day
        day += ONEDAY
        await asyncio.sleep(0.01)


def ignore_spam(nick, remark):
    global ignored_nicks
    if any([phrase in remark for phrase in spam_phrases]):
        if nick not in ignored_nicks:
            output(f"ADDING {nick} TO IGNORED NICKS")
            ignored_nicks.add(nick)
    return nick in ignored_nicks


async def get_data(name, start_day, end_day, queue):
    async with httpx.AsyncClient(timeout=TIMEOUT) as session:
        updates = []
        while True:
            channel = await queue.get()
            output(f"{name}: working on **{channel}**")
            esc_chan = quote(channel)
            async for day in nextdate(start_day, end_day):
                if day.day == 1:
                    output(f"{name}: Starting {day.year}-{day.month}-{day.day} for {channel}")
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
                        resp = await session.get(uri)
                        status_code = resp.status_code
                        try:
                            resp_text = resp.text
                        except UnicodeDecodeError:
                            resp_text = ""
                        break
                    except (httpx.ConnectError, httpx.RemoteProtocolError) as e:
                        output(f"Failed '{e}'; retrying...")
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
                        output(f"Error encountered: {e}")
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

                    updates.append(json.dumps({"index": {"_id": doc["id"], "_index": "irclog"}}))
                    updates.append(json.dumps(doc))
                    if len(updates) >= 1000:
                        await post_updates(name, updates, session)
                        updates = []
                    await asyncio.sleep(0.01)
            await post_updates(name, updates, session)
            output(f"{name}: calling task_done")
            queue.task_done()
    await session.close()


async def post_updates(name, updates, session):
    data = "\n".join(updates) + "\n"
    output("POSTING UPDATE", data)
    resp = await session.post(
        "http://dodata:9200/irclog/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=data,
        timeout=30,
    )
    if resp.status_code != 200:
        output(f"BAD POST: {resp.status_code} -- {resp.text}")


async def main_runner(start=None, end=None, chan=None, quiet=False):
    start_day = dt.datetime.strptime(start, "%Y-%m-%d") if start else DEFAULT_START_DATE
    end_day = dt.datetime.strptime(end, "%Y-%m-%d") if end else dt.datetime.now()
    channels = [chan] if chan else _get_channels()

    global print_output
    print_output = not quiet

    queue = asyncio.Queue()
    for channel in channels:
        queue.put_nowait(channel)

    # Create worker tasks to process the queue concurrently.
    tasks = []
    for i in range(NUM_CONCURRENT):
        name = f"Task-{i}"
        output(f">> Creating {name}")
        task = asyncio.create_task(get_data(name, start_day, end_day, queue))
        output(f">> Created {task}")
        tasks.append(task)

    time_started = time.monotonic()
    output(">> Starting await queue.join()")
    await queue.join()
    output(">> Finished await queue.join()")
    elapsed = time.monotonic() - time_started

    for task in tasks:
        output(f">> Canceling {task}")
        task.cancel()
    output(">> Starting gather()")
    await asyncio.gather(*tasks, return_exceptions=True)
    output(">> Finished gather()")

    output(f">> ELAPSED: {elapsed}")


def parse(start=None, end=None, chan=None, quiet=False):
    asyncio.run(main_runner(start=start, end=end, chan=chan, quiet=quiet))


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--start", "-s", help="Start day for parsing. Default=2017-01-01")
@click.option("--end", "-e", help="End day for parsing. Default=today")
@click.option("--chan", "-c", help="Only parse records for the specified channel")
@click.option("--quiet", "-q", help="Suppress all print output")
def main(start=None, end=None, chan=None, quiet=False):
    return parse(start=start, end=end, chan=chan, quiet=quiet)


if __name__ == "__main__":
    main()
