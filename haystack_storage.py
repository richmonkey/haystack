import sys
import gevent
import gevent.socket as socket
import gevent.select as select
from geventsendfile import gevent_sendfile as sendfile
import json
import logging
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import haystack
import haystack_logging

groupid = 0
haystack_path = ""
haystack_index_path = ""

def make_fileid(groupid, fileno):
    return (groupid<<54)|fileno

def parse_fileid(fileid):
    fileno = fileid & ((1<<54)-1)
    groupid = fileid>>54
    return (groupid, fileno)

def handle_client(sock):
    while True:
        rds, _, _ = select.select([sock], [], [], 60*5)
        if not rds:
            break
        keepalived = handle_request(sock)
        if not keepalived:
            break
    logging.debug("close client")
    sock.close()

def handle_upload(sock, parser):
    while not parser.is_message_complete():
        data = sock.recv(64*1024)
        if not data:
            logging.warn("client sock closed")
            return False
        recved = len(data)
        nparsed = p.execute(data, recved)
        assert(nparsed == recved)

    body = parser.recv_body()
    if not body:
        return False

    haystack.haystack_last_fileno += 1
    needle = haystack.Needle()
    needle.data = body
    needle.key = haystack.haystack_last_fileno
    offset = needle.write()
    haystack.haystack_files[needle.key] = (offset, len(body))
    fileid = make_fileid(groupid, haystack.haystack_last_fileno)
    logging.debug("fileid:%d fileno:%d", fileid, haystack.haystack_last_fileno)

    resp = json.dumps({"fileid":fileid})
    sock.send("HTTP/1.1 200 OK\r\n")
    sock.send("Content-Length: %d\r\n"%len(resp))
    sock.send("Content-Type: application/octet-stream\r\n")
    keepalived = parser.should_keep_alive()
    if keepalived:
        sock.send("Connection: keep-alive\r\n")
    else:
        sock.send("Connection: close\r\n")
    sock.send("\r\n")
    sock.send(resp)
    return keepalived

def handle_download(sock, parser):
    while not parser.is_message_complete():
        data = sock.recv(1024)
        if not data:
            logging.warn("client sock closed")
            return False
        recved = len(data)
        nparsed = p.execute(data, recved)
        assert(nparsed == recved)
    
    keepalived = parser.should_keep_alive()
    args = urlparse.parse_qs(parser.get_query_string())
    logging.debug("args:%r", args)
    fileid = int(args["fileid"][0]) if args.has_key("fileid") else 0
    _, fileid = parse_fileid(fileid)
    if not haystack.haystack_files.has_key(fileid):
        logging.debug("can't find file:%d", fileid)
        sock.send("HTTP/1.1 404 Not Found\r\n")
        sock.send("Content-Length: 0\r\n")
        if keepalived:
            sock.send("Connection: keep-alive\r\n")
        else:
            sock.send("Connection: close\r\n")
        sock.send("\r\n")
    else:
        sock.send("HTTP/1.1 200 OK\r\n")
        sock.send("Content-Type: application/octet-stream\r\n")
        offset, size = haystack.haystack_files[fileid]
        sock.send("Content-Length: %d\r\n"%size)
        if keepalived:
            sock.send("Connection: keep-alive\r\n")
        else:
            sock.send("Connection: close\r\n")
        sock.send("\r\n")
        logging.debug("offset:%d, size:%d\n", offset, size)
        sendfile(sock.fileno(), haystack.haystack_file.fileno(), offset+haystack.Needle.HEADER_SIZE, size)

    return keepalived

def handle_index(sock, parser):
    logging.debug("handle index")
    while not parser.is_message_complete():
        logging.debug("recv body")
        data = sock.recv(1024)
        if not data:
            logging.warn("client sock closed")
            return False
        recved = len(data)
        nparsed = p.execute(data, recved)
        assert(nparsed == recved)
    
    resp = "Hello World"
    keepalived = parser.should_keep_alive()
    print "keep alived:", keepalived
    args = urlparse.parse_qs(parser.get_query_string())
    print "args:", args
    args = urlparse.parse_qs(parser.get_query_string())
    sock.send("HTTP/1.1 200 OK\r\n")
    sock.send("Content-Type: application/octet-stream\r\n")

    sock.send("Content-Length: %d\r\n"%len(resp))
    if keepalived:
        sock.send("Connection: keep-alive\r\n")
    else:
        sock.send("Connection: close\r\n")
    sock.send("\r\n")
    sock.send(resp)
    return keepalived
    
def handle_request(sock):
    p = HttpParser()
    while True:
        logging.debug("recv........")
        data = sock.recv(1024)
        if not data:
            logging.warn("client sock closed")
            return False
        recved = len(data)
        nparsed = p.execute(data, recved)
        assert(nparsed == recved)
        if p.is_headers_complete():
            break
    if p.get_path() == "/upload":
        return handle_upload(sock, p)
    elif p.get_path() == "/download":
        return handle_download(sock, p)
    elif p.get_path() == "/index.html":
        return handle_index(sock, p)
    logging.debug("unknown request path:%s", p.get_path())
    return False

def file_exists(path):
    try:
        f = open(path, "rb")
        f.close()
        return True
    except IOError:
        return False

def main():
    global haystack_path, haystack_index_path, groupid
    haystack_logging.init_logger("storage", logging.DEBUG)

    config = {}
    execfile("haystack.conf", config)
    haystack_path = config["dbfilename"]
    haystack_index_path = config["dbindexfilename"]
    groupid = config["groupid"]

    if not file_exists(haystack_path) and not haystack.create_store(haystack_path):
        logging.error("create store file fail")
        sys.exit(1)
    if not file_exists(haystack_index_path) and not haystack.create_index(haystack_index_path):
        logging.error("create index file fail")
        sys.exit(1)
    if not haystack.recover(haystack_path, haystack_index_path):
        logging.error("recover haystack store fail")
        sys.exit(1)
    if not haystack.load(haystack_path, haystack_index_path):
        logging.error("load haystack file fail")
        sys.exit(1)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = ('127.0.0.1', 8080)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(address)
    s.listen(5)

    while True:
        client_sock, address = s.accept()
        gevent.spawn(handle_client, client_sock)

main()

