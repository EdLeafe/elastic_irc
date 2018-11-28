import msg_archive
import utils

crs = utils.get_cursor()
sql = "select max(imsg) as highmsg from webdata.archive"
crs.execute(sql)
rec = crs.fetchone()
curr = rec["highmsg"]

with open(".highmessage") as ff:
    last = int(ff.read().strip())
if curr > last:
    data_gen = msg_archive.get_data(currmsg=last)
    for data in data_gen:
        vals = data["_source"]
        res = msg_archive.es_client.index(index="email", doc_type="mail",
                    id=vals["id"], body=vals)
        print(res)
    with open(".highmessage", "w") as ff:
        ff.write("%s" % curr)
