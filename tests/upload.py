#!/usr/local/bin/python
# coding: utf-8
import sys
import urllib3
import logging
import json

def upload():
    upload_url = "http://127.0.0.1:8080/upload"
    url = urllib3.util.parse_url(upload_url)
    cb_url = url.request_uri
     
    if url.port is not None:
        server = "%s:%d"%(url.host, url.port)            
    else:
        server = url.host
     
    conn = urllib3.connection_from_url(server)
    headers = urllib3.make_headers(keep_alive=True)
    content = "hello world"
    response = conn.urlopen("POST", cb_url, body=content, headers=headers)
    if response.status != 200:
        print "eeeeeeeeeeee"
        sys.exit(1)
    else:
        print response.getheaders()
        print response.read()
        print response.data
        fileid = json.loads(response.data)["fileid"]
     
    path = "/download?fileid=%d"%fileid
    print "download path:", path
    response = conn.urlopen("GET", path, headers=headers)
    if response.status != 200:
        print "download fail"
        sys.exit(1)
    else:
        print response.data

def main():
    for _ in range(1):
        upload()

main()
