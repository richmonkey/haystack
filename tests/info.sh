#!/bin/sh
host=localhost:8000
echo "track info"
curl "http://$host/info"
host=localhost:8080
echo "\nstorage1 info"
curl "http://$host/info"
host=localhost:8081
echo "\nstorage2 info"
curl "http://$host/info"
echo ""