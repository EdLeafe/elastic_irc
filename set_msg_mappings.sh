curl -X PUT dodb:9200/email -H 'Content-Type: application/json' -d '{"mappings": {
    "mail": {
        "properties": {
            "msg_num": {
                "type": "keyword"},
            "list_name": {
                "type": "keyword"},
            "subject": {
                "type": "text",
                "analyzer": "standard"},
            "from": {
                "type": "keyword"},
            "posted": {
                "type": "keyword"},
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
}
'
