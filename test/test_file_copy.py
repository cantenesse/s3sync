#!/usr/bin/env python
import os
import sys
import io

def write_chunk(from_fp, to_fp, offset, bytes):
    from_fp.seek(offset)
    to_fp.seek(offset)
    print "Writing %d bytes at offset %d" % (bytes, to_fp.tell())
    to_fp.write(from_fp.read(bytes))

def write_backwards(_from, to, parts):
    file_size = os.stat(_from).st_size
    chunksize = file_size / parts
    print "Filesize: ", file_size
    print "Chunksize: ", chunksize
    from_fp = io.open(_from, mode="rb")
    to_fp = io.open(to + "backwards", mode="wb")
    for offset in reversed(xrange(0, file_size, file_size / parts)):
        print "Calling write_chunk with offset: ", offset
        write_chunk(from_fp, to_fp, offset, chunksize)
    from_fp.close()
    to_fp.close()

def write_forwards(_from, to, parts):
    file_size = os.stat(_from).st_size
    chunksize = file_size / parts
    print "Filesize: ", file_size
    print "Chunksize: ", chunksize
    from_fp = io.open(_from, mode="rb")
    to_fp = io.open(to + "forwards", mode="wb")
    offset = 0
    while (file_size - offset) >= 0:
        write_chunk(from_fp, to_fp, offset, chunksize)
        offset += chunksize
    from_fp.close()
    to_fp.close()


if __name__ == "__main__":
    _from = sys.argv[1]
    to = sys.argv[2]
    parts = 10

    write_forwards(_from, to, parts)
    write_backwards(_from, to, parts)
