#!/bin/sh
host=localhost:8080
ab -k -c 100 -n 100000 -p ./send.data -T 'application/octet-stream'   http://$host/upload