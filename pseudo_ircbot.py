import datetime

import async_parse_irc

yesterday = datetime.date.today() - datetime.timedelta(days=1)
start = yesterday.strftime("%Y-%m-%d")
async_parse_irc.parse(start=start, quiet=True)
