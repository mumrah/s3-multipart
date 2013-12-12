#!/usr/bin/env python
import argparse
from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Pool
import sys
import time
import urlparse

import boto
from boto.s3.connection import OrdinaryCallingFormat


parser = argparse.ArgumentParser(description="Copy large files within S3",
        prog="s3-mp-copy")
parser.add_argument("src", help="The S3 source object")
parser.add_argument("dest", help="The S3 destination object")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-f", "--force", help="Overwrite an existing S3 key",
        action="store_true")
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int, default=50)
parser.add_argument("-rrs", "--reduced-redundancy", help="Use reduced redundancy storage. Default is standard.", 
        default=False,  action="store_true")
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")

logger = logging.getLogger("s3-mp-copy")

def do_part_copy(args):
    """
    Copy a part of a MultiPartUpload

    Copy a single chunk between S3 objects. Since we can't pickle
    S3Connection or MultiPartUpload objects, we have to reconnect and lookup
    the MPU object with each part upload.

    :type args: tuple of (string, string, string, int, int, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 src bucket name, S3 key name, S3 dest
                 bucket_name, MultiPartUpload id, the part number, 
                 part start position, part stop position
    """
    # Multiprocessing args lameness
    src_bucket_name, src_key_name, dest_bucket_name, mpu_id, part_num, start_pos, end_pos = args
    logger.debug("do_part_copy got args: %s" % (args,))

    # Connect to S3, get the MultiPartUpload
    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    dest_bucket = s3.lookup(dest_bucket_name)
    mpu = None
    for mp in dest_bucket.list_multipart_uploads():
        if mp.id == mpu_id:
            mpu = mp
            break
    if mpu is None:
        raise Exception("Could not find MultiPartUpload %s" % mpu_id)

    # make sure we have a valid key
    src_bucket = s3.lookup( src_bucket_name )
    src_key    = src_bucket.get_key( src_key_name )
    # Do the copy
    t1 = time.time()
    mpu.copy_part_from_key(src_bucket_name, src_key_name, part_num, start_pos, end_pos)

    # Print some timings
    t2 = time.time() - t1
    s = (end_pos - start_pos)/1024./1024.
    logger.info("Copied part %s (%0.2fM) in %0.2fs at %0.2fMbps" % (part_num, s, t2, s/t2))

def validate_url( url ):
    split = urlparse.urlsplit( url )
    if split.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % url)
    return split.netloc, split.path[1:]

def main(src, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, verbose=False):
    dest_bucket_name, dest_key_name = validate_url( dest )
    src_bucket_name, src_key_name   = validate_url( src )

    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    dest_bucket = s3.lookup( dest_bucket_name )
    dest_key    = dest_bucket.get_key( dest_key_name )
    
    # See if we're overwriting an existing key
    if dest_key is not None:
        if not force:
            raise ValueError("'%s' already exists. Specify -f to overwrite it" % dest)

    # Determine the total size and calculate byte ranges
    src_bucket = s3.lookup( src_bucket_name )
    src_key    = src_bucket.get_key( src_key_name )
    size       = src_key.size

    # If file is less than 5G, copy it directly
    if size < 5*1024*1024*1024:
        logging.info("Source object is %0.2fM copying it directly" % ( size/1024./1024. ))
        t1 = time.time()
        src_key.copy( dest_bucket_name, dest_key_name, reduced_redundancy=reduced_redundancy )
        t2 = time.time() - t1
        s = size/1024./1024.
        logger.info("Finished copying %0.2fM in %0.2fs (%0.2fMbps)" % (s, t2, s/t2))
        return

    part_size   = max(5*1024*1024, 1024*1024*split)
    num_parts   = int(ceil(size / float(part_size)))
    logging.info("Source object is %0.2fM splitting into %d parts of size %0.2fM" % (size/1024./1024., num_parts, part_size/1024./1024.) )

    # Create the multi-part upload object
    mpu = dest_bucket.initiate_multipart_upload( dest_key_name, reduced_redundancy=reduced_redundancy)
    logger.info("Initialized copy: %s" % mpu.id)

    # Generate arguments for invocations of do_part_copy 
    def gen_args(num_parts):
        cur_pos = 0
        for i in range(num_parts):
            part_start = cur_pos
            cur_pos    = cur_pos + part_size
            part_end   = min(cur_pos - 1, size - 1)
            part_num   = i + 1
            yield (src_bucket_name, src_key_name, dest_bucket_name, mpu.id, part_num, part_start, part_end)

    # Do the thing
    try:
        # Create a pool of workers
        pool = Pool(processes=num_processes)
        t1 = time.time()
        pool.map_async(do_part_copy, gen_args(num_parts)).get(9999999)
        # Print out some timings
        t2 = time.time() - t1
        s = size/1024./1024.
        # Finalize
        mpu.complete_upload()
        logger.info("Finished copying %0.2fM in %0.2fs (%0.2fMbps)" % (s, t2, s/t2))
    except KeyboardInterrupt:
        logger.warn("Received KeyboardInterrupt, canceling copy")
        pool.terminate()
        mpu.cancel_upload()
    except Exception, err:
        logger.error("Encountered an error, canceling copy")
        logger.error(err)
        mpu.cancel_upload()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    arg_dict = vars(args)
    if arg_dict['verbose'] == True:
        logger.setLevel(logging.DEBUG)
    logger.debug("CLI args: %s" % args)
    main(**arg_dict)
