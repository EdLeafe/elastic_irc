import requests

HQ = ".elastichq"
url = "http://dodata:9200/_snapshot/elasticback/_all"
response = requests.request("GET", url)
snaps = response.json()["snapshots"]
print(f"Total Snapshots: {len(snaps)}")

for snap in snaps[:-10:-1]:
    print(f"Name: {snap['snapshot']}   {snap['end_time']}")
    idxs = sorted([idx for idx in snap["indices"] if not idx.startswith(".")])
    print(f"  Indexes: {', '.join(idxs)}")
