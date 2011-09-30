#!/usr/bin/env python
import argparse
import logging
from math import ceil
from multiprocessing import Pool
import os
import time
import urlparse

import boto

parser = argparse.ArgumentParser(description="Download a file from S3 in parallel",
        prog="s3-mp-download")
parser.add_argument("src", help="The S3 key to download")
parser.add_argument("dest", help="The destination file")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-f", "--force", help="Overwrite an existing file",
        action="store_true")

log = logging.getLogger("s3-mp-download")

def do_part_download(args):
    """
    Download a part of an S3 object using Range header

    We utilize the existing S3 GET request implemented by Boto and tack on the
    Range header. We then read in 1Mb chunks of the file and write out to the
    correct position in the target file

    :type args: tuple of (string, string, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 Bucket name, S3 key, local file name,
                 chunk size, and part number
    """
    bucket_name, key_name, fname, min_byte, max_byte = args
    conn = boto.connect_s3()

    # Make the S3 request
    resp = conn.make_request("GET", bucket=bucket_name,
            key=key_name, headers={'Range':"bytes=%d-%d" % (min_byte, max_byte)})

    # Open the target file, seek to byte offset
    fd = os.open(fname, os.O_WRONLY)
    log.info("Opening file descriptor %d, seeking to %d" % (fd, min_byte))
    os.lseek(fd, min_byte, os.SEEK_SET)

    chunk_size = min((max_byte-min_byte), 32*1024*1024)
    log.info("Reading HTTP stream in %dM chunks" % (chunk_size/1024./1024))
    t1 = time.time()
    s = 0
    while True:
        data = resp.read(chunk_size)
        if data == "":
            break
        os.write(fd, data)
        s += len(data)
    t2 = time.time() - t1
    os.close(fd)
    s = s / 1024 / 1024.
    log.info("Downloaded %0.2fM in %0.2fs at %0.2fMbps" % (s, t2, s/t2))

def gen_byte_ranges(size, num_parts):
    part_size = int(ceil(1. * size / num_parts))
    for i in range(num_parts):
        yield (part_size*i, min(part_size*(i+1)-1, size-1))

def main():
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    log.debug("Got args: %s" % args)


    # Check that src is a valid S3 url
    split_rs = urlparse.urlsplit(args.src)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % args.src)

    # Check that dest does not exist
    if os.path.exists(args.dest):
        if args.force:
            os.remove(args.dest)
        else:
            raise ValueError("Destination file '%s' exists, specify -f to"
                             " overwrite" % args.dest)

    # Touch the file
    fd = os.open(args.dest, os.O_CREAT)
    os.close(fd)

    # Split out the bucket and the key
    s3 = boto.connect_s3()
    bucket = s3.lookup(split_rs.netloc)
    key = bucket.get_key(split_rs.path)

    # Determine the total size and calculate byte ranges
    conn = boto.connect_s3()
    resp = conn.make_request("HEAD", bucket=bucket, key=key)
    size = int(resp.getheader("content-length"))
    logging.info("Got headers: %s" % resp.getheaders())

    num_parts = args.num_processes

    def arg_iterator(num_parts):
        for min_byte, max_byte in gen_byte_ranges(size, num_parts):
            yield (bucket.name, key.name, args.dest, min_byte, max_byte)

    s = size / 1024 / 1024.
    try:
        t1 = time.time()
        pool = Pool(processes=args.num_processes)
        pool.map_async(do_part_download, arg_iterator(num_parts)).get(9999999)
        t2 = time.time() - t1
        log.info("Finished downloading %0.2fM in %0.2fs (%0.2fMbps)" %
                (s, t2, s/t2))
    except KeyboardInterrupt:
        log.info("User terminated")
    except Exception, err:
        log.error(err)

if __name__ == "__main__":
    main()
