import logging
import_stmts = (
    'from pysnappy2 import snappy',
    'from pysnappy2_24 import snappy'
)

for status, stmt in enumerate(import_stmts):
    try:
        exec stmt
        break
    except ImportError:
        status = None
if status is None:
    logging.warn("could not import snappy module. Compress/uncompress function will be skipped.")

def uncompress(data, decoding=None):
    try:
        return snappy.uncompress(data, decoding)
    except Exception, err:
        return data

def compress(data, encoding='utf-8'):
    try:
        return snappy.compress(data, encoding)
    except Exception, err:
        return data
