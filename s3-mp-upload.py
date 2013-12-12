#!/usr/bin/env python
import argparse
from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Pool
import time
import urlparse

import boto
from boto.s3.connection import OrdinaryCallingFormat

parser = argparse.ArgumentParser(description="Transfer large files to S3",
        prog="s3-mp-upload")
parser.add_argument("src", type=file, help="The file to transfer")
parser.add_argument("dest", help="The S3 destination object")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-f", "--force", help="Overwrite an existing S3 key",
        action="store_true")
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int, default=50)
parser.add_argument("-rrs", "--reduced-redundancy", help="Use reduced redundancy storage. Default is standard.", default=False,  action="store_true")
parser.add_argument("--insecure", dest='secure', help="Use HTTP for connection",
        default=True, action="store_false")
parser.add_argument("-t", "--max-tries", help="Max allowed retries for http timeout", type=int, default=5)
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")
parser.add_argument("-q", "--quiet", help="Be less verbose (for use in cron jobs)", default=False, action="store_true")

logger = logging.getLogger("s3-mp-upload")

def do_part_upload(args):
    """
    Upload a part of a MultiPartUpload

    Open the target file and read in a chunk. Since we can't pickle
    S3Connection or MultiPartUpload objects, we have to reconnect and lookup
    the MPU object with each part upload.

    :type args: tuple of (string, string, string, int, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 Bucket name, MultiPartUpload id, file
                 name, the part number, part offset, part size
    """
    # Multiprocessing args lameness
    bucket_name, mpu_id, fname, i, start, size, secure, max_tries, current_tries = args
    logger.debug("do_part_upload got args: %s" % (args,))

    # Connect to S3, get the MultiPartUpload
    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    s3.is_secure = secure
    bucket = s3.lookup(bucket_name)
    mpu = None
    for mp in bucket.list_multipart_uploads():
        if mp.id == mpu_id:
            mpu = mp
            break
    if mpu is None:
        raise Exception("Could not find MultiPartUpload %s" % mpu_id)

    # Read the chunk from the file
    fp = open(fname, 'rb')
    fp.seek(start)
    data = fp.read(size)
    fp.close()
    if not data:
        raise Exception("Unexpectedly tried to read an empty chunk")

    def progress(x,y):
        logger.debug("Part %d: %0.2f%%" % (i+1, 100.*x/y))

    try:
        # Do the upload
        t1 = time.time()
        mpu.upload_part_from_file(StringIO(data), i+1, cb=progress)

        # Print some timings
        t2 = time.time() - t1
        s = len(data)/1024./1024.
        logger.info("Uploaded part %s (%0.2fM) in %0.2fs at %0.2fMBps" % (i+1, s, t2, s/t2))
    except Exception, err:
        logger.debug("Retry request %d of max %d times" % (current_tries, max_tries))
        if (current_tries > max_tries):
            logger.error(err)
        else:
            time.sleep(3)
            current_tries += 1
            do_part_download(bucket_name, mpu_id, fname, i, start, size, secure, max_tries, current_tries)

def main(src, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, verbose=False, quiet=False, secure=True, max_tries=5):
    # Check that dest is a valid S3 url
    split_rs = urlparse.urlsplit(dest)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % dest)

    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    s3.is_secure = secure
    bucket = s3.lookup(split_rs.netloc)
    if bucket == None:
        raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)
    key = bucket.get_key(split_rs.path)
    # See if we're overwriting an existing key
    if key is not None:
        if not force:
            raise ValueError("'%s' already exists. Specify -f to overwrite it" % dest)

    # Determine the splits
    part_size = max(5*1024*1024, 1024*1024*split)
    src.seek(0,2)
    size = src.tell()
    num_parts = int(ceil(size / part_size))

    # If file is less than 5M, just upload it directly
    if size < 5*1024*1024:
        src.seek(0)
        t1 = time.time()
        k = boto.s3.key.Key(bucket,split_rs.path)
        k.set_contents_from_file(src)
        t2 = time.time() - t1
        s = size/1024./1024.
        logger.info("Finished uploading %0.2fM in %0.2fs (%0.2fMBps)" % (s, t2, s/t2))
        return

    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(split_rs.path, reduced_redundancy=reduced_redundancy)
    logger.info("Initialized upload: %s" % mpu.id)

    # Generate arguments for invocations of do_part_upload
    def gen_args(num_parts, fold_last):
        for i in range(num_parts+1):
            part_start = part_size*i
            if i == (num_parts-1) and fold_last is True:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size*2, secure, max_tries, 0)
                break
            else:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size, secure, max_tries, 0)


    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < 5*1024*1024)

    # Do the thing
    try:
        # Create a pool of workers
        pool = Pool(processes=num_processes)
        t1 = time.time()
        pool.map_async(do_part_upload, gen_args(num_parts, fold_last)).get(9999999)
        # Print out some timings
        t2 = time.time() - t1
        s = size/1024./1024.
        # Finalize
        src.close()
        mpu.complete_upload()
        logger.info("Finished uploading %0.2fM in %0.2fs (%0.2fMBps)" % (s, t2, s/t2))
    except KeyboardInterrupt:
        logger.warn("Received KeyboardInterrupt, canceling upload")
        pool.terminate()
        mpu.cancel_upload()
    except Exception, err:
        logger.error("Encountered an error, canceling upload")
        logger.error(err)
        mpu.cancel_upload()

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
