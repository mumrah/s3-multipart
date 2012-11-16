Utilities to do parallel upload/download with Amazon S3

# Install with Pip 

    pip install -r requirements.txt

# Install without Pip

    easy_install -U boto

# Parallel download (s3-mp-download)

Utilizes S3's support for the Range HTTP header, fetches multiple chunks of the
file in parallel. See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html

    $ ./s3-mp-download.py -h
    usage: s3-mp-download [-h] [-np NUM_PROCESSES] [-f] src dest

    Download a file from S3 in parallel

    positional arguments:
      src                   The S3 key to download
      dest                  The destination file

    optional arguments:
      -h, --help            show this help message and exit
      -np NUM_PROCESSES, --num-processes NUM_PROCESSES
			    Number of processors to use
      -f, --force           Overwrite an existing file

# Parallel upload (s3-mp-upload)

Utilizes the Multipart Upload feature of S3. Splits up the local file into
chunks and uploads them in parallel. See:
http://aws.typepad.com/aws/2010/11/amazon-s3-multipart-upload.html

    usage: s3-mp-upload [-h] [-n NUM_PROCESSES] [-f] [-s SPLIT] src dest

    Transfer large files to S3

    positional arguments:
      src                   The file to transfer
      dest                  The S3 destination object

    optional arguments:
      -h, --help            show this help message and exit
      -n NUM_PROCESSES, --num-processes NUM_PROCESSES
			    Number of processors to use
      -f, --force           Overwrite an existing S3 key
      -s SPLIT, --split SPLIT
			    Split size, in Mb

# Credits

As always, mad props to the Boto project and it's maintainer, Mitch
https://github.com/boto/boto

# License

Copyright 2012, David Arthur under Apache License, v2.0. See `LICENSE`
