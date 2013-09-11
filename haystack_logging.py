import logging
import logging.handlers
import sys
import fcntl
import os

class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1

def lock(file, flags):
    try:
        fcntl.flock(file.fileno(), flags)
    except IOError, exc_value:
        #  IOError: [Errno 11] Resource temporarily unavailable
        if exc_value[0] == 11:
            raise LockException(LockException.LOCK_FAILED, exc_value[1])
        else:
            raise

def unlock(file):
    fcntl.flock(file.fileno(), fcntl.LOCK_UN)

#multi process lock, clone from logging.handlers.RotatingFileHandler
class RotatingFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0, lock_file=None):
        if maxBytes > 0:
            mode = 'a'
        super(RotatingFileHandler, self).__init__(filename, mode, encoding, delay)
        self.lock_file = lock_file
        self.maxBytes = maxBytes
        self.backupCount = backupCount

    def doRollover(self):
        lock(self.lock_file, fcntl.LOCK_EX)
        try:
            if self.stream:
                self.stream.close()
                self.stream = None
            #check wetcher rollover by other process
            if not self.shouldRollover(self.record):
                return
            self._doRollover()
        finally:
            unlock(self.lock_file)

    def _doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d" % (self.baseFilename, i)
                dfn = "%s.%d" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    #print "%s -> %s" % (sfn, dfn)
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.baseFilename + ".1"
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(self.baseFilename, dfn)
            #print "%s -> %s" % (self.baseFilename, dfn)
        self.stream = self._open()

    def emit(self, record):
        """
        Emit a record.

        Output the record to the file, catering for rollover as described
        in doRollover().
        """
        try:
            self.record = record
            if self.shouldRollover(record):
                self.doRollover()
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        Basically, see if the supplied record would cause the file to exceed
        the size limit we have.
        """
        if self.stream is None:                 # delay was set...
            self.stream = self._open()
        if self.maxBytes > 0:                   # are we rolling over?
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)  #due to non-posix-compliant Windows feature
            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1
        return 0


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

def init_logger1():
    h = NullHandler()
    logging.getLogger("pika").addHandler(h)
    logging.getLogger("pika").setLevel(logging.ERROR)
    logging.getLogger("amqp").addHandler(h)
    logging.getLogger("amqp").setLevel(logging.ERROR)
    return

def init_logger(appname, level, lock=False):
    init_logger1()
    root_logger = logging.getLogger('')

    strm_out = logging.StreamHandler(sys.__stdout__)
    strm_out.setFormatter(logging.Formatter('%(name)s %(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(strm_out)
    
    if lock:
        lock_file = open(appname + ".lock", "ab")

    if lock:
        handler = RotatingFileHandler(appname + "err.log", "ab", 50*1024*1024, 5, lock_file=lock_file)
    else:
        handler = logging.handlers.RotatingFileHandler(appname + "err.log", "ab", 50*1024*1024, 5)

    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(handler)

    if lock:
        handler = RotatingFileHandler(appname + ".log", "ab", 50*1024*1024, 5, lock_file=lock_file)
    else:
        handler = logging.handlers.RotatingFileHandler(appname + ".log", "ab", 50*1024*1024, 5)

    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(handler)

    root_logger.setLevel(level)

