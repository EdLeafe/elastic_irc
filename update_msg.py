from elasticsearch.helpers import bulk

from msg_archive import es_client, get_data
import utils


crs = utils.get_cursor()
sql = "select max(imsg) as highmsg from webdata.archive"
crs.execute(sql)
rec = crs.fetchone()
curr = rec["highmsg"]

with open(".highmessage") as ff:
    last = int(ff.read().strip())
if curr > last:
    success, failures = bulk(es_client, get_data(currmsg=last, verbose=True))
#    num = 0
#    data_gen = msg_archive.get_data(currmsg=last)
#    for data in data_gen:
#        num += 1
#        vals = data["_source"]
#        res = msg_archive.es_client.index(index="email", doc_type="mail",
#                    id=vals["id"], body=vals)
#        if num % 100 == 0:
#            print("Imported msg#", num)
#    with open(".highmessage", "w") as ff:
#        ff.write("%s" % curr)
