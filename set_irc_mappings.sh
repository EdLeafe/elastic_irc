curl -X DELETE datahost:9200/irclog
echo Deleted

curl -X PUT datahost:9200/irclog -H 'Content-Type: application/json' -d '{
"mappings": {
    "properties": {
        "channel": {
            "type": "keyword"},
        "nick": {
            "type": "keyword"},
        "posted": {
            "type": "keyword"},
        "remark": {
            "type": "text",
            "analyzer": "standard"}
        }
    }
}
'

