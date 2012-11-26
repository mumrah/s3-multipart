#!/usr/bin/env python
import argparse
from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Pool
import time
import urlparse

import boto

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
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")

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
    bucket_name, mpu_id, fname, i, start, size = args
    logger.debug("do_part_upload got args: %s" % (args,))

    # Connect to S3, get the MultiPartUpload
    s3 = boto.connect_s3()
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

    # Do the upload
    t1 = time.time()
    mpu.upload_part_from_file(StringIO(data), i+1)

    # Print some timings
    t2 = time.time() - t1
    s = len(data)/1024./1024.
    logger.info("Uploaded part %s (%0.2fM) in %0.2fs at %0.2fMbps" % (i+1, s, t2, s/t2))

def main():
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.verbose == True:
        logger.setLevel(logging.DEBUG)
    logger.debug("Got CLI args: %s" % args)

    # Check that dest is a valid S3 url
    split_rs = urlparse.urlsplit(args.dest)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % args.dest)

    s3 = boto.connect_s3()
    bucket = s3.lookup(split_rs.netloc)
    key = bucket.get_key(split_rs.path)
    # See if we're overwriting an existing key
    if key is not None:
        if not args.force:
            raise ValueError("'%s' already exists. Specify -f to overwrite it" % args.dest)

    # Determine the splits
    part_size = max(5*1024*1024, 1024*1024*args.split)
    args.src.seek(0,2)
    size = args.src.tell()
    num_parts = int(ceil(size / part_size))

    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(split_rs.path, reduced_redundancy=args.reduced_redundancy)
    logger.info("Initialized upload: %s" % mpu.id)


    # Generate arguments for invocations of do_part_upload
    def gen_args(num_parts, fold_last):
        for i in range(num_parts+1):
            part_start = part_size*i
            if i == (num_parts-1) and fold_last is True:
                yield (bucket.name, mpu.id, args.src.name, i, part_start, part_size*2)
                break
            else:
                yield (bucket.name, mpu.id, args.src.name, i, part_start, part_size)


    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < 5*1024*1024)

    # Do the thing
    try:
        # Create a pool of workers
        pool = Pool(processes=args.num_processes)
        t1 = time.time()
        pool.map(do_part_upload, gen_args(num_parts, fold_last))
        # Print out some timings
        t2 = time.time() - t1
        s = size/1024./1024.
        # Finalize
        args.src.close()
        mpu.complete_upload()
        logger.info("Finished uploading %0.2fM in %0.2fs (%0.2fMbps)" % (s, t2, s/t2))
    except Exception, err:
        logger.error("Encountered an error, canceling upload")
        logger.error(err)
        mpu.cancel_upload()

if __name__ == "__main__":
    main()
