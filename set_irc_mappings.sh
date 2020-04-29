curl -X DELETE dodata:9200/irclog

curl -X PUT dodata:9200/irclog -H 'Content-Type: application/json' -d '{"mappings": {
    "irc": {
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
}
'

