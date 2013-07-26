#!/usr/bin/env python
import argparse
import urlparse
import boto
import sys

parser = argparse.ArgumentParser(description="View or remove incomplete S3 multipart uploads",
        prog="s3-mp-cleanup")
parser.add_argument("uri", type=str, help="The S3 URI to operate on")
parser.add_argument("-c", "--cancel", help="Upload ID to cancel", type=str, required=False)

def main(uri, cancel):
    # Check that dest is a valid S3 url
    split_rs = urlparse.urlsplit(uri)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % uri)

    s3 = boto.connect_s3()
    bucket = s3.lookup(split_rs.netloc)
    
    mpul = bucket.list_multipart_uploads()
    for mpu in mpul:
        if not cancel:
            print('s3-mp-cleanup.py s3://{}/{} -c {}  # {} {}'.format(mpu.bucket.name, mpu.key_name, mpu.id, mpu.initiator.display_name, mpu.initiated))
        elif cancel == mpu.id:
            bucket.cancel_multipart_upload(mpu.key_name, mpu.id)
            break
    else:
        if cancel:
            print("No multipart upload {} found for {}".format(cancel, uri))
            sys.exit(1)
        
    

if __name__ == "__main__":
    args = parser.parse_args()
    arg_dict = vars(args)
    main(**arg_dict)
