import sys
import gevent
import gevent.socket as socket
import gevent.select as select
import gevent.queue as queue
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
listenip = ""
listenport = 0
master = False

slaves = []
track = None

masterip = ""
masterport = 0

TRACK_ONLINE = 1
TRACK_OFFLINE = 2

class HaystackTrack:
    def __init__(self):
        self.ip = ""
        self.port = 0
        self.channel = queue.Channel()
        self.state = TRACK_OFFLINE
        self.waiting = False

class HaystackSlave:
    def __init__(self):
        self.channel = queue.Channel()
        self.last_fileno = 0
        self.waiting = False

def make_fileid(groupid, fileno):
    return (groupid<<54)|fileno

def parse_fileid(fileid):
    fileno = fileid & ((1<<54)-1)
    groupid = fileid>>54
    return (groupid, fileno)

def handle_sync_upload(sock, parser):
    logging.debug("handle sync upload")
    body = parser.recv_body()
    if not body:
        return False
    headers = parser.get_headers()
    fileno = int(headers["FileNO"]) if headers.has_key("FileNO") else 0
    if fileno <= haystack.haystack_last_fileno:
        logging.error("fileno:%d less than %d", \
                          fileno, haystack.haystack_last_fileno)
        return False

    if fileno - haystack.haystack_last_fileno != 1:
        logging.error("fileno is't continuous fileno:%d, last_fileno:%d", \
                          fileno, haystack.haystack_last_fileno)

    needle = haystack.Needle()
    needle.data = body
    needle.key = fileno
    offset = needle.write()
    haystack.haystack_files[needle.key] = (offset, len(body))
    haystack.haystack_last_fileno = fileno
    logging.debug("fileno:%d", fileno)

    for slave in slaves:
        if slave.waiting:
            slave.channel.put(haystack.haystack_last_fileno)
    if track.waiting:
        track.channel.put(haystack.haystack_last_fileno)

    return True

def handle_upload(sock, parser):
    body = parser.recv_body()
    if not body:
        return False

    needle = haystack.Needle()
    needle.data = body
    needle.key = haystack.haystack_last_fileno+1
    offset = needle.write()
    haystack.haystack_last_fileno += 1
    haystack.haystack_files[needle.key] = (offset, len(body))
    fileid = make_fileid(groupid, haystack.haystack_last_fileno)
    logging.debug("fileid:%d fileno:%d", fileid, haystack.haystack_last_fileno)

    for slave in slaves:
        if slave.waiting:
            slave.channel.put(haystack.haystack_last_fileno)
    if track.waiting:
        track.channel.put(haystack.haystack_last_fileno)

    return {"fileid":fileid}

def handle_download(sock, parser):
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

    return bool(keepalived)
    
def heartbeat(sock):
    ip, port = sock.getpeername()
    parser = HttpParser()
    sock.send("GET /ping HTTP/1.1\r\nHost: %s:%d\r\n\r\n"%(ip, port))

    while True:
      data = sock.recv(1024)
      if not data:
          return False

      recved = len(data)
      nparsed = parser.execute(data, recved)
      assert(nparsed == recved)
      if parser.is_message_complete():
          break

    return parser.get_status_code() == 200

def post_file(sock, fileno):
    ip, port = sock.getpeername()
    if not haystack.haystack_files.has_key(fileno):
        logging.error("can't find file:%d", fileno)
        return False

    offset, size = haystack.haystack_files[fileno]
    logging.debug("post file:%d size:%d", fileno, size)

    sock.send("POST /sync_upload HTTP/1.1\r\n")
    sock.send("Host: %s:%d\r\n"%(ip, port))
    sock.send("Content-Length: %d\r\n"%size)
    sock.send("Content-Type: application/octet-stream\r\n")
    sock.send("Connection: keep-alive\r\n")
    sock.send("FileNO: %d\r\n"%fileno)
    sock.send("\r\n")
    sendfile(sock.fileno(), haystack.haystack_file.fileno(), \
                 offset+haystack.Needle.HEADER_SIZE, size)
    return True

def handle_sync(sock, parser):
    logging.debug("handle sync")
    body = parser.recv_body()
    if not body:
        return False
    obj = json.loads(body)
    last_fileno = obj["last_fileno"]
    keepalived = parser.should_keep_alive()
    assert(keepalived)
    sock.send("HTTP/1.1 200 OK\r\n")
    sock.send("Content-Length: 0\r\n")
    sock.send("Connection: keep-alive\r\n")
    sock.send("\r\n")

    slave = HaystackSlave()
    slave.last_fileno = last_fileno
    slaves.append(slave)

    try:
        while True:
            while slave.last_fileno < haystack.haystack_last_fileno:
                if not post_file(sock, slave.last_fileno+1):
                    return False
                slave.last_fileno += 1
            try:
                slave.waiting = True
                slave.channel.get(timeout=5)
            except queue.Empty:
                if not heartbeat(sock):
                    return False
            finally:
                slave.waiting = False
    finally:
        slaves.remove(slave)

def handle_info(sock, parser):
    logging.debug("handle info")
    keepalived = parser.should_keep_alive()
    obj = {}
    obj["trackip"] = track.ip
    obj["trackport"] = track.port
    if track.state == TRACK_ONLINE:
        obj["trackstate"] = "online"
    elif track.state == TRACK_OFFLINE:
        obj["trackstate"] = "offline"
        
    if master:
        obj["role"] = "master"
        obj["nslaves"] = len(slaves)
    else:
        obj["role"] = "slave"
        obj["masterip"] = masterip
        obj["masterport"] = masterport
    obj["last_fileno"] = haystack.haystack_last_fileno
    return obj

def handle_ping(sock, parser):
    logging.debug("handle ping")
    return "pong"
    
def handle_request(sock, parser, preread):
    logging.debug("handle request")
    if parser:
        assert(parser.is_headers_complete())
        headers = parser.get_headers()
        content_length = int(headers["Content-Length"]) if headers.has_key("Content-Length") else 0
        assert(content_length >= len(preread))
        if content_length:
            if preread:
                nparsed = parser.execute(preread, len(preread))
                assert(nparsed == len(preread))
                content_length -= len(preread)
            while content_length:
                data = sock.recv(content_length)
                if not data:
                    logging.warn("client sock closed")
                    return False
                recved = len(data)
                content_length -= recved
                nparsed = parser.execute(data, recved)
                assert(nparsed == recved)
                if parser.is_message_complete():
                    break
    else:
        parser = HttpParser()
        while True:
            logging.debug("recv........")
            data = sock.recv(64*1024)
            if not data:
                logging.warn("client sock closed")
                return False
            recved = len(data)
            nparsed = parser.execute(data, recved)
            assert(nparsed == recved)
            if parser.is_message_complete():
                break

    obj = None
    if parser.get_path() == "/upload":
        obj = handle_upload(sock, parser)
    elif parser.get_path() == "/sync_upload":
        obj = handle_sync_upload(sock, parser)
    elif parser.get_path() == "/download":
        obj = handle_download(sock, parser)
    elif parser.get_path() == "/sync":
        obj = handle_sync(sock, parser)
    elif parser.get_path() == "/ping":
        obj = handle_ping(sock, parser)
    elif parser.get_path() == "/info":
        obj = handle_info(sock, parser)
    else:
        logging.debug("unknown request path:%s", parser.get_path())
        
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
        keepalived = parser.should_keep_alive()
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
    try:
        while True:
            rds, _, _ = select.select([sock], [], [], 60*5)
            if not rds:
                break

            keepalived = handle_request(sock, None, None)
            if not keepalived:
                break
    except socket.error, e:
        logging.debug("socket error:%r", e)
    finally:
        logging.debug("close client")
        sock.close()
    
def handle_batch_client(sock):
    recvbuf = ""
    while True:
        rds, _, _ = select.select([sock], [], [], 60*5)
        if not rds:
            break

        data = sock.recv(1024)
        if not data:
            break
        recvbuf += data
        
        pos = recvbuf.find("\r\n\r\n")
        if pos == -1:
            continue
        parser = HttpParser()
        nparsed = parser.execute(recvbuf, pos+4)
        if nparsed != pos+4:
            logging.debug("pos:%d, nparsed:%d, recvbuf:%r", pos, nparsed, recvbuf)
        assert(nparsed == pos+4)
        assert(parser.is_headers_complete())
        headers = parser.get_headers()
        content_length = int(headers["Content-Length"]) if headers.has_key("Content-Length") else 0
        logging.debug("content length:%d", content_length)
        recvbuf = recvbuf[pos+4:]
        preread = recvbuf[:content_length]
        recvbuf = recvbuf[content_length:]
        keepalived = handle_request(sock, parser, preread)
        if not keepalived:
            break

    logging.debug("close client")
    sock.close()

def post_sync(sock, masterip, masterport):
    obj = {"last_fileno":haystack.haystack_last_fileno}
    body = json.dumps(obj)
    sock.send("POST /sync HTTP/1.1\r\n")
    sock.send("Host: %s:%d\r\n"%(masterip, masterport))
    sock.send("Content-Length: %d\r\n"%len(body))
    sock.send("Content-Type: application/json\r\n")
    sock.send("Connection: keep-alive\r\n")
    sock.send("\r\n")
    sock.send(body)

    parser = HttpParser()
    while True:
        #!!!ugly prevent recveive next http request
        data = sock.recv(1)
        if not data:
            return False

        recved = len(data)
        nparsed = parser.execute(data, recved)
        assert(nparsed == recved)
        if parser.is_message_complete():
            break

    return parser.get_status_code() == 200

def _sync(masterip, masterport):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((masterip, masterport))
    if not post_sync(sock, masterip, masterport):
        return
    logging.debug("slave sync begin recv...")
    handle_batch_client(sock)
  
def sync_with_master(masterip, masterport):
    while True:
        try:
            logging.debug("sync........")
            _sync(masterip, masterport)
        except socket.error, e:
            logging.debug("disconnect with master, exception:%r", e)
            gevent.sleep(5)
        except Exception, e:
            logging.debug("sync exception:%r", e)
            gevent.sleep(5)

def post_report(sock):
    obj = {}
    obj["listenip"] = listenip
    obj["listenport"] = listenport
    GB = 1024*1024*1024
    obj["disk_available_size"] = GB+1
    obj["master"] = master
    obj["groupid"] = groupid
    obj["last_fileno"] = haystack.haystack_last_fileno
    body = json.dumps(obj)
    sock.send("POST /report HTTP/1.1\r\n")
    sock.send("Host: %s:%d\r\n"%(track.ip, track.port))
    sock.send("Content-Length: %d\r\n"%len(body))
    sock.send("Content-Type: application/json\r\n")
    sock.send("Connection: keep-alive\r\n")
    sock.send("\r\n")
    sock.send(body)
    
    parser = HttpParser()
    while True:
      data = sock.recv(1024)
      if not data:
          return False

      recved = len(data)
      nparsed = parser.execute(data, recved)
      assert(nparsed == recved)
      if parser.is_message_complete():
          break

    return parser.get_status_code() == 200

#reconnect when 0,1,2,4,8,16.
def track_report():
    nseconds = 0
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((track.ip, track.port))
            while True:
                last_fileno = haystack.haystack_last_fileno
                post_report(sock)
                track.state = TRACK_ONLINE
                nseconds = 0
                if last_fileno != haystack.haystack_last_fileno:
                    continue
                try:    
                    track.waiting = True
                    track.channel.get(timeout=5)
                except queue.Empty:
                    continue
                finally:
                    track.waiting = False
        except socket.error, e:
            logging.debug("socket error:%r", e)
        except Exception, e:
            logging.debug("exception:%r", e)
        finally:
            track.state = TRACK_OFFLINE
            sock.close()
            if nseconds == 0:
                nseconds= 1
            else:
                gevent.sleep(nseconds)
                if nseconds < 10:
                    nseconds *= 2
            logging.debug("disconnect with track")          

def file_exists(path):
    try:
        f = open(path, "rb")
        f.close()
        return True
    except IOError:
        return False

def main():
    global listenip, listenport
    global track, masterip, masterport
    global groupid, master
    haystack_logging.init_logger("storage", logging.DEBUG)

    config = {}
    if len(sys.argv) == 1:
        logging.error("needs config file")
        return
    config_file = sys.argv[1]
    execfile(config_file, config)
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
    

    masterip = config["masterip"] if config.has_key("masterip") else ""
    masterport = config["masterport"] if config.has_key("masterport") else 0
    listenip = config["listenip"]
    listenport = config["listenport"]

    trackip = config["trackip"]
    trackport = config["trackport"]
    track = HaystackTrack()
    track.ip = trackip
    track.port = trackport
    assert(track.ip and track.port)
    gevent.spawn(track_report)
    if masterip and masterport:
        gevent.spawn(sync_with_master, masterip, masterport)
        master = False
    else:
        master = True

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

