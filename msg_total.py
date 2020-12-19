from datetime import datetime

import utils

es = utils.get_elastic_client()

r = es.count(index="email")
total = r["count"]
print("There are %s documents in the index." % total)
