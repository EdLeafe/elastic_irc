import requests

HQ = ".elastichq"
url = "http://dodata:9200/_cat/indices"
response = requests.request("GET", url)

indexes = [ln.split()[2] for ln in response.text.splitlines()]
# Remove the ElasticHQ index, if present
if HQ in indexes:
    indexes.remove(HQ)
print(indexes)
