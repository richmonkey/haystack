import logging
import logging.handlers
import sys
import fcntl
import os

def init_logger(appname, level):
    root_logger = logging.getLogger('')

    strm_out = logging.StreamHandler(sys.__stdout__)
    strm_out.setFormatter(logging.Formatter('%(name)s %(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(strm_out)
    
    handler = logging.handlers.RotatingFileHandler(appname + "err.log", "ab", 50*1024*1024, 5)

    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(handler)

    handler = logging.handlers.RotatingFileHandler(appname + ".log", "ab", 50*1024*1024, 5)

    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s %(filename)s %(lineno)s %(levelname)s:%(message)s'))
    root_logger.addHandler(handler)

    root_logger.setLevel(level)

