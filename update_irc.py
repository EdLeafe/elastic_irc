import datetime
import irc_latest
import parselog
import sys

use_latest = True
if len(sys.argv) > 1:
    start_date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    use_latest = False

for chan in parselog.CHANNELS:
    if use_latest:
        latest = irc_latest.get_latest(1, chan)
        if latest:
            latest_rec = latest[0]
            last_post_str = latest_rec["posted"].split("T")[0]
            last_post_date = datetime.datetime.strptime(last_post_str, "%Y-%m-%d").date()
        else:
            last_post_date = parselog.DEFAULT_START_DATE
        start_date = last_post_date + datetime.timedelta(days=1)
    parselog.import_irc(start_date, end_day=None, chan=chan)
