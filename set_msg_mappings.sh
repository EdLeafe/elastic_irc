curl -X DELETE datahost:9200/email

curl -X PUT datahost:9200/email -H 'Content-Type: application/json' -d '{"mappings": {
    "properties": {
        "msg_num": {
            "type": "integer"},
        "list_name": {
            "type": "keyword"},
        "subject": {
            "type": "keyword"},
        "fulltext_subject": {
            "type": "text",
            "analyzer": "standard"},
        "from": {
            "type": "keyword"},
        "posted": {
            "type": "date"},
        "message_id": {
            "type": "keyword"},
        "replyto_id": {
            "type": "keyword"},
        "body": {
            "type": "text",
            "analyzer": "standard"}
        }
    }
}
'
