import sys
import gevent
import gevent.socket as socket
import gevent.select as select
import json
import logging
import random
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import haystack_logging

groupnodes = {}
masternodes = {}

class HaystackNode:
    def __init__(self):
        self.disk_available_size = 0
        self.last_fileno = 0
        self.master = False
        self.groupid = 0
        self.listenip = ""
        self.listenport = 0
        self.read_priority = 0
        self.write_priority = 0

        
def parse_fileid(fileid):
    fileno = fileid & ((1<<54)-1)
    groupid = fileid>>54
    return (groupid, fileno)

def handle_report(sock, parser):
    logging.debug("handle report")

    body = parser.recv_body()
    if not body:
        return False

    obj = json.loads(body)

    if hasattr(sock, "node"):
        node = sock.node
    else:
        node = HaystackNode()
        sock.node = node
    if obj.has_key("last_fileno"):
        node.last_fileno = obj["last_fileno"]
    if obj.has_key("listenip"):
        node.listenip = obj["listenip"]
    if obj.has_key("listenport"):
        node.listenport = obj["listenport"]
    if obj.has_key("groupid"):
        assert(node.groupid == 0 or node.groupid == obj["groupid"])
        node.groupid = obj["groupid"]
    if obj.has_key("master"):
        node.master = obj["master"]
    if obj.has_key("disk_available_size"):
        node.disk_available_size = obj["disk_available_size"]

    if not node.groupid:
        return False

    if node.master:
        if masternodes.has_key(node.groupid) and node != masternodes[node.groupid]:
            logging.error("group:%d already has master")
        else:
            masternodes[node.groupid] = node

    if not groupnodes.has_key(node.groupid):
        groupnodes[node.groupid] = []

    if node not in groupnodes[node.groupid]:
        groupnodes[node.groupid].append(node)

    keepalived = parser.should_keep_alive()
    assert(keepalived)
    sock.send("HTTP/1.1 200 OK\r\n")
    sock.send("Content-Length: 0\r\n")
    sock.send("Connection: keep-alive\r\n")
    sock.send("\r\n")
    return bool(keepalived)

def handle_request_readable_node(sock, parser):
    logging.debug("handle_request_readable_node")
    args = urlparse.parse_qs(parser.get_query_string())
    if not args.has_key("fileid"):
        return False
    fildid = args["fileid"][0]
    groupid, fileno = parse_fileid(fileid)
    if not groupnodes.has_key(groupid):
        return False
    
    rnodes = []
    for node in groupnodes[groupid]:
        if node.last_fileno >= fileno:
            rnodes.append(node)
    if not rnodes and masternodes.has_key(groupid):
        rnodes.append(masternodes[groupid])
    if not rnodes:
        return False
    i = random.randint(len(rnodes)-1)
    node = rnodes[i]
    obj = {"ip":node.listenip, "port":node.listenport}
    return obj

def handle_request_writable_node(sock, parser):
    wnodes = []
    for groupid in masternodes:
        node = masterndoes[groupid]
        GB = 1024*1024*1024
        if node.disk_available_size > GB:
            wnodes.append(node)

    if not wnodes:
        return False
    i = random.randint(len(wnodes)-1)
    node = wnodes[i]
    obj = {"ip":node.listenip, "port":node.listenport}
    return obj

def handle_ping(sock, parser):
    logging.debug("handle ping")
    return "pong"
    
def handle_info(sock, parser):
    logging.debug("handle info")
    obj = []
    for groupid in groupnodes:
        group_nodes = groupnodes[groupid]
        for node in group_nodes:
            o = {}
            o["last_fileno"] = node.last_fileno
            o["role"] = "master" if node.master else "slave"
            o["groupid"] = node.groupid
            o["listenip"] = node.listenip
            o["listenport"] = node.listenport
            obj.append(o)

    return obj

def handle_request(sock):
    parser = HttpParser()
    while True:
        data = sock.recv(1024)
        if not data:
            logging.warn("client sock closed")
            return False
        recved = len(data)
        nparsed = parser.execute(data, recved)
        assert(nparsed == recved)
        if parser.is_message_complete():
            break

    obj = None
    if parser.get_path() == "/request_readable_node":
        obj = handle_request_readable_node(sock, parser)
    elif parser.get_path() == "/request_writable_node":
        obj = handle_request_writable_node(sock, parser)
    elif parser.get_path() == "/ping":
        obj = handle_ping(sock, parser)
    elif parser.get_path() == "/report":
        obj = handle_report(sock, parser)
    elif parser.get_path() == "/info":
        obj = handle_info(sock, parser)
    else:
        logging.debug("unknown request path:%s", parser.get_path())

    keepalived = parser.should_keep_alive()    
    if obj is None:
        sock.send("HTTP/1.1 404 Not Found\r\n")
        sock.send("Content-Length: 0\r\n")
        if keepalived:
            sock.send("Connection: keep-alive\r\n")
        else:
            sock.send("Connection: close\r\n")
        sock.send("\r\n")
        return False

    if not isinstance(obj, bool):
        resp = json.dumps(obj)
        sock.send("HTTP/1.1 200 OK\r\n")
        sock.send("Content-Type: application/json\r\n")
        sock.send("Content-Length: %d\r\n"%len(resp))
        if keepalived:
            sock.send("Connection: keep-alive\r\n")
        else:
            sock.send("Connection: close\r\n")
        sock.send("\r\n")
        sock.send(resp)
        return bool(keepalived)
    else:
        return obj

def handle_client(sock):
    while True:
        rds, _, _ = select.select([sock], [], [], 60*5)
        if not rds:
            break
        try:
            keepalived = handle_request(sock)
            if not keepalived:
                break
        except socket.error:
            break
    logging.debug("close client")
    if hasattr(sock, "node"):
        node = sock.node
        groupnodes[node.groupid].remove(node)
        if masternodes.has_key(node.groupid) and \
                masternodes[node.groupid] == node:
            masternodes.pop(node.groupid)
        sock.node = None
    sock.close()


def main():
    haystack_logging.init_logger("track", logging.DEBUG)

    config = {}
    if len(sys.argv) == 1:
        logging.error("needs config file")
        return
    config_file = sys.argv[1]
    execfile(config_file, config)
    listenip = config["listenip"]
    listenport = config["listenport"]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = (listenip, listenport)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(address)
    s.listen(5)

    while True:
        client_sock, address = s.accept()
        gevent.spawn(handle_client, client_sock)

if __name__ == "__main__":
    main()

