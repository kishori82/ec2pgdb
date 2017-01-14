#!/usr/bin/python27

import boto, boto.sqs, sys, os, time, logging, re, gzip
from boto.s3.key import Key
from optparse import OptionParser
#sudo apt-get install python-pip
#pip install -U boto


REGION = "us-east-1"
ACCESS_KEY=""
SECRET_KEY=""
#QUEUE_NAME="crazyqueue1367"
QUEUE_NAME="crazyqueue1368"

#logging.basicConfig(filename='debug.log',level=logging.DEBUG)
logging.basicConfig(filename='info.log',level=logging.INFO)

script_name = sys.argv[0]
usage = script_name + "[ --input <file name>  --bucket <bucket name> --key <key file>"
# Parse command line
parser = OptionParser()
parser.add_option("--input", dest="input", default =None, help="input file")
parser.add_option("--bucket", dest="bucket", default =None, help="bucket")
parser.add_option("--key", dest="key", default =None, help="the AWS key")


def fprintf(file, fmt, *args):
   file.write(fmt % args)

def printf(fmt, *args):
   sys.stdout.write(fmt % args)
 
def eprintf(fmt, *args):
   sys.stderr.write(fmt % args)
   sys.stderr.flush()


def read_key(keyfile):
    global SECRET_KEY, ACCESS_KEY
    with open(keyfile, 'r') as infile:
       for _line in infile:
          line = _line.strip()
          AccessKeyId  = re.search(r'^AWSAccessKeyId=(.*)', line)
          if AccessKeyId: 
             ACCESS_KEY=AccessKeyId.group(1)

          SecretKey  = re.search(r'^AWSSecretKey=(.*)', line)
          if SecretKey: 
             SECRET_KEY=SecretKey.group(1)



def upload_to_s3_bucket(conn, BUCKET_NAME, filename):
    b = conn.get_bucket(BUCKET_NAME)
    k = Key(b)
    k.key= os.path.basename(filename)
    k.set_contents_from_filename(filename)


def upload_file_to_s3(options):
   bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                  aws_secret_access_key=SECRET_KEY)

   print "Access key :", ACCESS_KEY, "Secret Key :", SECRET_KEY, "Bucket :", options.bucket, "Input File :", options.input
   upload_to_s3_bucket(bucket_conn, options.bucket, options.input)
   logging.info("Uploaded file : %s\n", options.input)
   printf("Uploaded file : %s\n", options.input)


def main(argv):
  (options, args) = parser.parse_args()

  read_key(options.key)

  upload_file_to_s3(options)


if __name__ == "__main__":
   main( sys.argv[1:])

