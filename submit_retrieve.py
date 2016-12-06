#!/usr/bin/python27

import boto, boto.sqs, sys, os, time, logging, re, gzip
from boto.s3.key import Key
from boto.sqs.message import Message
from optparse import OptionParser


REGION = "us-east-1"
ACCESS_KEY=""
SECRET_KEY=""
BUCKET_NAME='ptools1431'
#QUEUE_NAME="crazyqueue1367"
QUEUE_NAME="crazyqueue1368"

#logging.basicConfig(filename='debug.log',level=logging.DEBUG)
logging.basicConfig(filename='info.log',level=logging.INFO)

script_name = sys.argv[0]
usage = script_name + "--sample <name> --input <input> "
# Parse command line
parser = OptionParser()
parser.add_option("--sample", dest="sample", default =None, help="sample name")
parser.add_option("--input", dest="input", default =None, help="input file")
parser.add_option("--type", dest="type", default =None, choices=["producer", "consumer"], 
                help="whether producer or consumer")
parser.add_option("--key", dest="key", default =None, help="the AWS key")
#parser.add_option("--output", dest="output", default =None, help="output folder")


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



def upload_to_s3_bucket(conn, BUCKET_NAME, path, filename):
    b = conn.get_bucket(BUCKET_NAME)
    k = Key(b)
    k.key= 'data/input/'+ os.path.basename(filename)
    k.set_contents_from_filename(filename)


def submit_a_job(options):
   #first create a file in the bucket
   upload_sample_to_s3(options)

   submit_sample_name_to_SQS(options)


def upload_sample_to_s3(options):
   bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                  aws_secret_access_key=SECRET_KEY)

   print ACCESS_KEY, SECRET_KEY
   upload_to_s3_bucket(bucket_conn, BUCKET_NAME, 'input', options.input)
   logging.info("Uploaded sample:%s filename:%s\n", options.sample, options.input)
   printf("Uploaded sample:%s filename:%s\n", options.sample, options.input)

def submit_sample_name_to_SQS(options):
   # Send to SQS
   conn = boto.sqs.connect_to_region(REGION, 
                                    aws_access_key_id = ACCESS_KEY, 
                                    aws_secret_access_key=SECRET_KEY)
   logging.info("Connection to region:%s", REGION)
   printf("Connection to region:%s\n", REGION)
   q = conn.get_queue(QUEUE_NAME)
   if q==None:
      q = conn.create_queue(QUEUE_NAME)

   m = Message()
   m.set_body( "SAMPLE\t%s\nFILENAME\t%s\n" %(options.sample, options.input))
   q.write(m)
   logging.info("Uploaded sample:%s filename:%s\n", options.sample, options.input)
   printf("Uploaded sample:%s filename:%s\n", options.sample, options.input)
   printf("SAMPLE\t%s\nFILENAME\t%s\n" %(options.sample, os.path.basename(options.input)))
   
def do_some_work():
     print "Doing some work"
     time.sleep(5)



def download_file( mybucket,  inputfile):
   printf("Downloading inputfile %s\n",  inputfile)

#   for key in mybucket.list():
#      print 'key', key
   
   k = mybucket.get_key( inputfile)

   k.get_contents_to_filename('data/output/' + os.path.basename(inputfile))
   return 'data/output/',  os.path.basename(inputfile)


def gunzip_file(outputdir,  tar_gz_file):
    import tarfile
    targz  = tarfile.open(outputdir + tar_gz_file)
    targz.extractall(path=outputdir)
    targz.close()

def download_input_for_work(sample, inputfile) :
    printf("Downloading sample: %s inputfile:  %s\n", sample, inputfile)
    try:
       bucket_conn= boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                        aws_secret_access_key=SECRET_KEY)
       mybucket = bucket_conn.get_bucket(BUCKET_NAME) # Substitute in your bucket name
       
       outputdir,tar_gz_file = download_file(mybucket, inputfile)
       gunzip_file(outputdir, tar_gz_file)

    except:
       return False
    return True 

def parse_message(msg):
   mesPATT = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\n')
   res = mesPATT.search(msg)
   if res:
      sample = res.group(1)
      filename  = res.group(2)
   else:
      sampel = None
      filename = None
   return sample, filename

def retrieve_a_job():
     conn = boto.sqs.connect_to_region(REGION, 
                                       aws_access_key_id = ACCESS_KEY, 
                                       aws_secret_access_key=SECRET_KEY)
     q = conn.get_queue(QUEUE_NAME)
     if q==None:
        print "ERROR: SQS my-queue does not exist"
        sys.exit(0)

     count = q.count()
     logging.info("Number of jobs in queue:%s", count)
     printf("Number of jobs in queue:%s\n", count)

     if count == 0:
       return False, False

     m = q.read()
     print 'm', m
     msg = str(m.get_body())
     sample, filename = parse_message(msg)

     if sample==None:
        return False, False
     printf("Received:SAMPLE\t%s   FILENAME\t%s\n" %(sample, filename))

     q.delete_message(m)

     return sample, filename


def consume_a_job(options):
    logging.info("Trying to consume a job")

    while True:
       print " "
       sample, inputname = retrieve_a_job()
       if sample:
         logging.info("Successfully found a job to work on")

         download_status= download_input_for_work( sample, inputname) 
         if download_status==False:
            logging.warning("Failed to download data: %s for sample %s\n", sample, inputname)
            printf("Failed to download data: %s for sample %s\n", sample, inputname)
            break;
         else:
            logging.info("Preparing to work on job")
            printf("Preparing to work on job")
            #do_some_work() 
       else:
         logging.info("Failed to retrieve a job sleeping of 4 secs")
         print "Failed to retrieve a job sleeping of 4 secs"
         time.sleep(4)
    
def main(argv):
  (options, args) = parser.parse_args()

  read_key(options.key)

  if options.type =="producer":
      submit_a_job(options) 
  else:
      consume_a_job(options)


if __name__ == "__main__":
   main( sys.argv[1:])

