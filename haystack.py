import struct
import logging
haystack_files = {}

haystack_file = None
haystack_index_file = None
haystack_last_fileno = 0

haystack_magic = "mofs"
haystack_version = 1<<16 #1.0

HEADER_SIZE = 8
NEEDLE_INDEX_SIZE = 21


class Needle:
    HEADER_SIZE = 4+8+1+4
    FOOTER_SIZE = 8
    def __init__(self):
        self.key = 0
        self.deleted = 0
        self.data = ""

    @property
    def padding_size(self):
          l = (4+8+1+4+self.data_size+4+4)
          if l%8:
              padding = 8-l%8
          else:
              padding = 0
          return padding

    def unpack_header(self, data):
        self.hmagic, self.key, self.deleted, self.data_size = struct.unpack("!IQBI", data)
        
    def write(self):
        assert(self.data)
        haystack_file.seek(0, 2)
        offset = haystack_file.tell()
        pk = struct.pack("!4sQBI", haystack_magic, self.key, self.deleted, len(self.data))
        haystack_file.write(pk)
        haystack_file.write(self.data)

        crc = 0
        l = len(pk) + len(self.data) + 8
        if l%8:
            padding = 8-l%8
            pk = struct.pack("!4sI%ds"%padding, haystack_magic, crc, "")
        else:
            pk = struct.pack("!4sI", haystack_magic, crc)
            
        haystack_file.write(pk)
        haystack_file.flush()
        index = NeedleIndex()
        index.key = self.key
        index.deleted = self.deleted
        index.offset = offset
        index.size = len(self.data)
        pk = index.pack()
        haystack_index_file.write(pk)
        haystack_index_file.flush()
        return offset

class NeedleIndex:
    def __init__(self):
        self.key = 0
        self.deleted = 0
        self.offset = 0
        self.size = 0

    def unpack(self, data):
        self.key, self.deleted, self.offset, self.size = struct.unpack("!QBQI", data)

    def pack(self):
        return struct.pack("!QBQI", self.key, self.deleted, self.offset, self.size)
    
def load(path, ipath):
    global haystack_last_fileno, haystack_file, haystack_index_file
    try:
        store_file = open(path, "rb")
        index_file = open(ipath, "rb")
        store_header = store_file.read(HEADER_SIZE)
        index_header = index_file.read(HEADER_SIZE)

        if len(store_header) != HEADER_SIZE:
            return False
        m, v = struct.unpack("!4sI", store_header)
        if m != haystack_magic or v != haystack_version:
            return False

        if len(index_header) != HEADER_SIZE:
            return False
        m, v = struct.unpack("!4sI", index_header)
        if m != haystack_magic or v != haystack_version:
            return False

        haystack_files.clear()
        nis = NEEDLE_INDEX_SIZE
        while True:
            data = index_file.read(nis*1024)
            if not data:
                break
            for i in range(0, len(data), nis):
                d = data[i:i+nis]
                if len(d) != nis:
                    logging.warn("index file corrupted")
                    return False
                index = NeedleIndex()
                index.unpack(d)
                haystack_last_fileno = index.key
                if not index.deleted:
                    haystack_files[index.key] = (index.offset, index.size)
                elif haystack_files.has_key(index.key):
                    haystack_files.pop(index.key)
                else:
                    logging.warn("delete nonthing:%d", index.key)
        store_file.close()
        index_file.close()
        store_file = open(path, "ab+")
        index_file = open(ipath, "ab+")
        haystack_file = store_file
        haystack_index_file = index_file
        logging.debug("last fileno:%d", haystack_last_fileno)
        return True
    except IOError:
        return False

def recover(path, ipath):
    try:
        store_file = open(path, "rb+")
        index_file = open(ipath, "rb+")
        store_header = store_file.read(HEADER_SIZE)
        index_header = index_file.read(HEADER_SIZE)

        if len(store_header) != HEADER_SIZE:
            logging.debug("111")
            return False
        m, v = struct.unpack("!4sI", store_header)
        if m != haystack_magic or v != haystack_version:
            logging.debug("222")
            return False

        if len(index_header) != HEADER_SIZE:
            return False
        m, v = struct.unpack("!4sI", index_header)
        if m != haystack_magic or v != haystack_version:
            return False
        
        index_file.seek(0, 2)
        fsize = index_file.tell()
        logging.debug("index file size:%d", fsize)
        index_size = fsize-HEADER_SIZE
        nis = NEEDLE_INDEX_SIZE
        if index_size%nis:
            logging.error("truncate index file:%d", index_size%nis)
            fsize -= index_size%nis
            index_size = fsize - HEADER_SIZE
            index_file.truncate(fsize)

        offset = 0
        if index_size:
            index_file.seek(fsize-nis)
            data = index_file.read(nis)
            assert(len(data) == nis)
            index = NeedleIndex()
            index.unpack(data)
            offset = index.offset
        if offset == 0:
            offset = HEADER_SIZE

        index_file.seek(0, 2)
        store_file.seek(0, 2)
        fsize = store_file.tell()
        store_file.seek(offset)
        while True:
            begin = store_file.tell()
            data = store_file.read(Needle.HEADER_SIZE)
            if len(data) != Needle.HEADER_SIZE:
                if data:
                    logging.error("truncate store file:%d", fsize - begin)
                    store_file.truncate(begin)
                store_file.close()
                index_file.close()
                return True
            needle = Needle()
            needle.unpack_header(data)

            index = NeedleIndex()
            index.key = needle.key
            index.deleted = needle.deleted
            index.offset = begin
            index.size = needle.data_size
            pk = index.pack()
            index_file.write(pk)
            store_file.seek(needle.data_size + needle.FOOTER_SIZE + needle.padding_size, 1)

        assert(False)
    except IOError:
        return False    

def _create(path):
    try:
        f = open(path, "ab")
        assert(f.tell() == 0)
        header = struct.pack("!4sI", haystack_magic, haystack_version)
        f.write(header)
        f.close()
        return True
    except IOError:
        return False    

def create_index(ipath):
    return _create(ipath)

def create_store(path):
    return _create(path)

