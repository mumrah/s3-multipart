#!/usr/bin/env python
import argparse
import logging
from math import ceil
from multiprocessing import Pool
import os
import time
import urlparse

import boto
from boto.s3.connection import OrdinaryCallingFormat

parser = argparse.ArgumentParser(description="Download a file from S3 in parallel",
        prog="s3-mp-download")
parser.add_argument("src", help="The S3 key to download")
parser.add_argument("dest", help="The destination file")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int, default=32)
parser.add_argument("-f", "--force", help="Overwrite an existing file",
        action="store_true")
parser.add_argument("--insecure", dest='secure', help="Use HTTP for connection",
        default=True, action="store_false")
parser.add_argument("-t", "--max-tries", help="Max allowed retries for http timeout", type=int, default=5)
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")
parser.add_argument("-q", "--quiet", help="Be less verbose (for use in cron jobs)", 
        default=False, action="store_true")

logger = logging.getLogger("s3-mp-download")

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
    bucket_name, key_name, fname, min_byte, max_byte, split, secure, max_tries, current_tries = args
    conn = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    conn.is_secure = secure

    # Make the S3 request
    resp = conn.make_request("GET", bucket=bucket_name,
            key=key_name, headers={'Range':"bytes=%d-%d" % (min_byte, max_byte)})

    # Open the target file, seek to byte offset
    fd = os.open(fname, os.O_WRONLY)
    logger.debug("Opening file descriptor %d, seeking to %d" % (fd, min_byte))
    os.lseek(fd, min_byte, os.SEEK_SET)

    chunk_size = min((max_byte-min_byte), split*1024*1024)
    logger.debug("Reading HTTP stream in %dM chunks" % (chunk_size/1024./1024))
    t1 = time.time()
    s = 0
    try:
        while True:
            data = resp.read(chunk_size)
            if data == "":
                break
            os.write(fd, data)
            s += len(data)
        t2 = time.time() - t1
        os.close(fd)
        s = s / 1024 / 1024.
        logger.debug("Downloaded %0.2fM in %0.2fs at %0.2fMBps" % (s, t2, s/t2))
    except Exception, err:
        logger.debug("Retry request %d of max %d times" % (current_tries, max_tries))
        if (current_tries > max_tries):
            logger.error(err)
        else:
            time.sleep(3)
            current_tries += 1
            do_part_download(bucket_name, key_name, fname, min_byte, max_byte, split, secure, max_tries, current_tries)

def gen_byte_ranges(size, num_parts):
    part_size = int(ceil(1. * size / num_parts))
    for i in range(num_parts):
        yield (part_size*i, min(part_size*(i+1)-1, size-1))

def main(src, dest, num_processes=2, split=32, force=False, verbose=False, quiet=False, secure=True, max_tries=5):

    # Check that src is a valid S3 url
    split_rs = urlparse.urlsplit(src)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % src)

    # Check that dest does not exist
    if os.path.isdir(dest):
        filename = split_rs.path.split('/')[-1]
        dest = os.path.join(dest, filename)

    if os.path.exists(dest):
        if force:
            os.remove(dest)
        else:
            raise ValueError("Destination file '%s' exists, specify -f to"
                             " overwrite" % dest)

    # Split out the bucket and the key
    s3 = boto.connect_s3()
    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    s3.is_secure = secure
    logger.debug("split_rs: %s" % str(split_rs))
    bucket = s3.lookup(split_rs.netloc)
    if bucket == None:
        raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)
    key = bucket.get_key(split_rs.path)
    if key is None:
      raise ValueError("'%s' does not exist." % split_rs.path)

    # Determine the total size and calculate byte ranges
    resp = s3.make_request("HEAD", bucket=bucket, key=key)
    if resp is None:
      raise ValueError("response is invalid.")
      
    size = int(resp.getheader("content-length"))
    logger.debug("Got headers: %s" % resp.getheaders())

    # Skipping multipart if file is less than 1mb
    if size < 1024 * 1024:
        t1 = time.time()
        key.get_contents_to_filename(dest)
        t2 = time.time() - t1
        size_mb = size / 1024 / 1024
        logger.info("Finished single-part download of %0.2fM in %0.2fs (%0.2fMBps)" %
                (size_mb, t2, size_mb/t2))
    else:
        # Touch the file
        fd = os.open(dest, os.O_CREAT)
        os.close(fd)
    
        size_mb = size / 1024 / 1024
        num_parts = (size_mb+(-size_mb%split))//split

        def arg_iterator(num_parts):
            for min_byte, max_byte in gen_byte_ranges(size, num_parts):
                yield (bucket.name, key.name, dest, min_byte, max_byte, split, secure, max_tries, 0)

        s = size / 1024 / 1024.
        try:
            t1 = time.time()
            pool = Pool(processes=num_processes)
            pool.map_async(do_part_download, arg_iterator(num_parts)).get(9999999)
            t2 = time.time() - t1
            logger.info("Finished downloading %0.2fM in %0.2fs (%0.2fMBps)" %
                    (s, t2, s/t2))
        except KeyboardInterrupt:
            logger.warning("User terminated")
        except Exception, err:
            logger.error(err)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    arg_dict = vars(args)
    if arg_dict['quiet'] == True:
        logger.setLevel(logging.WARNING)
    if arg_dict['verbose'] == True:
        logger.setLevel(logging.DEBUG)
    logger.debug("CLI args: %s" % args)
    main(**arg_dict)
