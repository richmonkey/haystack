#!/bin/sh
host=localhost:8000

curl "http://$host/request_writable_node"
echo ""
curl "http://$host/request_readable_node?fileid=18014398509481985"
echo ""