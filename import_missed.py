import pudb

import utils

DELIM = "|*|"
PTH = "/Users/ed/dls/MISSED"

clt = utils.get_elastic_client()

with open(PTH, "r") as ff:
    for ln in ff:
        id_, body_str = ln.strip().split(DELIM)
        body = eval(body_str)
        resp = clt.index(index="email", id=id_, body=body)
        if resp["result"] != "created":
            pudb.set_trace()
