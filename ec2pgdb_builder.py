#!/usr/bin/python27

import boto, boto.sqs, sys, os, time, logging, re, gzip, random, shutil, socket, datetime
from boto.s3.key import Key
from boto.sqs.message import Message
from optparse import OptionParser

#sudo apt-get install python-pip
#pip install -U boto



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
parser.add_option("--process-type", dest="processtype", default =None, choices=["worker", "submiter", "monitor","command"], 
                help="[worker, submitter, monitor, command]")

parser.add_option("--sample", dest="samples", action='append', default =[], help="sample name")
parser.add_option("--inputbucket", dest="inputbucket", default ="pgdbinput", help="input bucket [ def: pgdbinput ]")
parser.add_option("--outputbucket", dest="outputbucket", default ="pgdboutput", help="output bucket [ def : pgdboutput ] ")

parser.add_option("--readyqueue", dest="readyqueue", default ='ready', help="ready queue [ def: None ]")

parser.add_option("--submittedqueue", dest="submittedqueue", default='submitted', help="ready queue [ def: submitted ]")
parser.add_option("--runningqueue", dest="runningqueue", default ='running', help="running queue [ def: running ]")
parser.add_option("--completequeue", dest="completequeue", default ='complete', help="complete queue [ def: complete] ")

parser.add_option("--submiter_dir", dest="submit_dir", default ="submit_dir", help="submit dir [ def : /home/ubuntu/submit_dir ]")
parser.add_option("--worker_dir", dest="worker_dir", default ="/home/ubuntu/worker_dir", help="worker dir [ def : /home/ubuntu/worker_dir] ")


parser.add_option("--clearqueue", dest="clearqueues", action='append', default = [], help="queues to clear [ def: [] ]")
parser.add_option("--clearbucket", dest="clearbuckets", action='append',  default = [], help="queues to clear [ def: [] ]")

parser.add_option("--status", dest="status", default =None, help="status by sample or jobid")
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





def do_some_work():
   try:
     print "\tBuilding PGDB"
     time.sleep(5)
   except:
     return False

   return True



def download_file(options, filename):

   printf("Downloading filename %s\n",  filename)
   bucket_conn= boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                        aws_secret_access_key=SECRET_KEY)
   mybucket = bucket_conn.get_bucket(options.inputbucket) # Substitute in your bucket name

   k = mybucket.get_key(filename)
   k.get_contents_to_filename('/tmp/' + os.path.basename(filename))



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
       
    #  outputdir,tar_gz_file = download_file(mybucket, inputfile)
     #  gunzip_file(outputdir, tar_gz_file)

    except:
       return False
    return True 

def parse_message(msg, fields=3):
   mesPATT3= re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)')
   mesPATT4 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)')
   mesPATT5 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)\nTIME\t(.*)')
   mesPATT6 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)\nTIME\t(.*)\nSIZE\t(.*)')

   res = None
   if fields==3:
      res = mesPATT3.search(msg)
   if fields==4:
      res = mesPATT4.search(msg)

   if fields==5:
      res = mesPATT5.search(msg)

   if fields==6:
      res = mesPATT6.search(msg)

   hostname = None
   time_stamp = None
   size = None
   if res:
      sample = res.group(1)
      filename  = res.group(2)
      jobid  = res.group(3)

      if fields==4:
          hostname = res.group(4)

      if fields==5:
          hostname = res.group(4)
          time_stamp = res.group(5)

      if fields==6:
          hostname = res.group(4)
          time_stamp = res.group(5)
          size = res.group(6)
   else:
      sample = None
      filename = None
      jobid = None
      hostname = None
      time_stamp = None
   return sample, filename, jobid, hostname, time_stamp, size

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


def count_orfs(folder):
     pf = folder + "/ptools/" + "0.pf"
     namePATT = re.compile(r'NAME\t')
     count = 0
     with open(pf, 'r') as f:
        lines = f.readlines()
        for _line in lines:   
           line = _line.strip()
           if namePATT.search(line):
              count += 1

     return count

def create_tarzip_file(foldername, jobid):
    import tarfile

    pf = foldername + "/ptools/" + "0.pf"
    gen_elem= foldername + "/ptools/genetic-elements.dat"
    org_params = foldername + "/ptools/organism-params.dat"

    samplename = os.path.basename(foldername)
    with tarfile.open("/tmp/"+ "jobid-" + jobid + "-" + samplename + ".tar.gz", "w:gz") as tar:
       for name in [pf,  gen_elem, org_params] :
           print "\t", "adding :", name
           tar.add(foldername, arcname=os.path.basename(foldername))


def upload_to_s3_bucket(conn, BUCKET_NAME, filepath):

    b = conn.get_bucket(BUCKET_NAME)
    k = Key(b)
    k.key= os.path.basename(filepath)
    k.set_contents_from_filename(filepath)


def upload_file_to_s3(options, samplename,  jobid):
   bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

#   print ACCESS_KEY, SECRET_KEY
   upload_to_s3_bucket(bucket_conn, options.inputbucket, "/tmp/" + "jobid-" + jobid + "-"+ samplename + ".tar.gz")
   logging.info("Uploaded sample:%s\n", samplename)
   printf("\tuploaded : %s Bucket %s\n", samplename, options.inputbucket)
   os.remove("/tmp/" + "jobid-" +  jobid + "-" + samplename + ".tar.gz")

def submit_sample_name_to_SQS(queuename, samplename, filename, jobid, hostname,  event_time, size, time_min):
   # Send to SQS
   conn = boto.sqs.connect_to_region(REGION, 
                                    aws_access_key_id = ACCESS_KEY, 
                                    aws_secret_access_key=SECRET_KEY)
   logging.info("Connection to region:%s", REGION)
   printf("\tConnecting to SQS : %s   REGION:%s\n", queuename, REGION)

   q = conn.get_queue(queuename)
   if q==None:
      q = conn.create_queue(queuename)

   m = Message()

   m.set_body("SAMPLE\t%s\nFILENAME\t%s\nJOBID\t%s\nHOSTNAME\t%s\nTIME\t%s\nSIZE\t%sDURATION\t%s\n" %(samplename, filename, jobid, hostname, event_time, size, time_min))
   q.write(m)
   printf("\tINFO : SAMPLE\t%s; FILENAME\t%s\tJOBID\t%s; HOSTNAME\t%s; TIME\t%s; SIZE\t%s; DURATION\t%s\n" %(samplename, filename, jobid, hostname, event_time, size, time_min))

 
def sanity_check(foldername) :
     pf = foldername + "/ptools/" + "0.pf"
     gen_elem= foldername + "/ptools/genetic-elements.dat"
     org_params = foldername + "/ptools/organism-params.dat"

     if not os.path.exists(pf):
        return False

     if not os.path.exists(gen_elem):
        return False

     if not os.path.exists(org_params):
        return False

     return True 
    
def submiter(options):
   for sample in options.samples:
      print "SUBMITTING : ", sample
      submiter_daemon(options, sample)


def submiter_daemon(options, samplename):
    if options.submit_dir!=None:
       if sanity_check(options.submit_dir + "/" + samplename):
         print "\t", "#ORFS : ", count_orfs(options.submit_dir + "/" + samplename)
         num = count_orfs(options.submit_dir + "/" + samplename)
          
         jobid = str(random.randrange(0,1000000000)) 
         if os.path.exists("/tmp/" + "jobid-" + jobid + "-" + samplename + ".tar.gz"):
             os.remove("/tmp/" +  "jobid-" + jobid + "-" +  samplename + ".tar.gz")

         create_tarzip_file(options.submit_dir + "/" + samplename, jobid)

         if options.inputbucket != None:
            upload_file_to_s3(options, samplename, jobid)

            if options.readyqueue != None:
               filename = "jobid-" + jobid + "-" + samplename + ".tar.gz"
               suffix = "_large"
               size = "large"
               if num < 5000:
                  suffix = "_small"
                  size = "small"
               elif num > 5000 and num < 15000:
                  suffix = "_medium"
                  size = "medium"
               else:
                  suffix = "_large"
                  size = "large"

               submit_time =  str(datetime.datetime.now())
               submit_min =  str(time.time()%60)
               hostname = socket.gethostname().strip()
 
               submit_sample_name_to_SQS(options.readyqueue + suffix, samplename, filename, jobid, hostname, submit_time, size, submit_min)
               submit_sample_name_to_SQS(options.submittedqueue, samplename, filename, jobid, hostname, submit_time, size, submit_min)


def read_a_message(options):
     conn = boto.sqs.connect_to_region(REGION, 
                                       aws_access_key_id = ACCESS_KEY, 
                                       aws_secret_access_key=SECRET_KEY)
     q = conn.get_queue(options.readyqueue)
     if q==None:
        print "ERROR: SQS my-queue does not exist"
        sys.exit(0)

     count = q.count()
     logging.info("Number of jobs in queue:%s", count)
     printf("\t# Jobs in SQS %s : %s\n",options.readyqueue, count)


     if count == 0:
       return None, None, None

     m = q.read()
     #print 'm', m
     msg = str(m.get_body())
     sample, filename, jobid,_, size= parse_message(msg)

     if sample==None:
       return None, None, None
     printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s\n" %(sample, filename, jobid))

     q.delete_message(m)

     return sample, filename, jobid, size

def download_file(options, filename):

   printf("\tDownloading : %s\n",  filename)
   bucket_conn= boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                        aws_secret_access_key=SECRET_KEY)
   mybucket = bucket_conn.get_bucket(options.inputbucket) # Substitute in your bucket name

   k = mybucket.get_key(filename)
   result = k.get_contents_to_filename('/tmp/' + os.path.basename(filename))
   return result


def gunzip_file(outputdir,  tar_gz_file):
    import tarfile
    targz  = tarfile.open(tar_gz_file)
    targz.extractall(path=outputdir)
    targz.close()



def worker(options):
    if not os.path.exists(options.worker_dir):
       os.mkdir(options.worker_dir)

    if options.readyqueue==None:
       print "ERROR: Must satisfy readyqueue name"
       sys.exit(0)

    while True:
      worker_daemon(options)
      time.sleep(5)
      

def worker_daemon(options):
    
    if options.readyqueue!=None:
       print "READING : ", options.readyqueue
       sample, filename, jobid, size  = read_a_message(options)
       print "\tSAMPLE %s; FILENAME : %s; JOBID : %s\n" %(sample, filename,jobid)

       if filename!=None:
          result = download_file(options, filename)
          gunzip_file(options.worker_dir,  '/tmp/' + filename)
          os.remove('/tmp/' + filename)

          if options.runningqueue!=None:
             print "\tUpdate Running SQS : %s\n" %(options.runningqueue)
             start_time =  str(datetime.datetime.now()) 
             start_min =  str(time.time()%60) 
             hostname = socket.gethostname().strip()
             submit_sample_name_to_SQS(options.runningqueue, sample, filename, jobid, hostname, start_time, size, start_min)

          do_some_work()
          #shutil.rmtree( options.worker_dir + '/' + sample)

          end_time =  str(datetime.datetime.now())
          if options.completequeue!=None:
             print "\tUpdate Complete SQS : %s\n" %(options.completequeue)
             submit_sample_name_to_SQS(options.completequeue, sample, filename, jobid, hostname,  start_time, size)



def remove_job_from_SQS(queuename, samplename, filename, jobid):
   # Send to SQS
   conn = boto.sqs.connect_to_region(REGION, 
                                    aws_access_key_id = ACCESS_KEY, 
                                    aws_secret_access_key=SECRET_KEY)
   logging.info("Connection to region:%s", REGION)
   printf("\tConnecting to SQS : %s   REGION:%s\n", queuename, REGION)

   q = conn.get_queue(queuename)
   if q==None:
      print "ERROR: SQS my-queue does not exist"
      sys.exit(0)


   all_messages=[]
   rs=q.get_messages(1)
   while len(rs)>0:
       all_messages.extend(rs)
       m = rs[0]
       rs=q.get_messages(1)
       _samplename, _filename, _jobid = parse_message(m.get_body())

       if samplename == _samplename and filename == _filename and _jobid == jobid:
           q.delete_message(m)



def read_status(options, queuename, fields =3):
     conn = boto.sqs.connect_to_region(REGION, 
                                       aws_access_key_id = ACCESS_KEY, 
                                       aws_secret_access_key=SECRET_KEY)
     q = conn.get_queue(queuename)
     if q==None:
        print "ERROR: SQS my-queue does not exist"
        sys.exit(0)

     count = q.count()
     logging.info("Number of jobs in queue:%s", count)
     printf("\t# Jobs in SQS %s : %s\n",queuename, count)

     all_messages=[]
     rs=q.get_messages(1)
     while len(rs)>0:
       all_messages.extend(rs)
       rs=q.get_messages(1)

     for m in all_messages:
        if fields==3:
           sample, filename, jobid, _, _, _,_ = parse_message(m.get_body(), fields)
           printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s\n" %(sample, filename,jobid))

        if fields==4:
           sample, filename, jobid, hostname, _, _,_ = parse_message(m.get_body(), fields)
           printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s\n" %(sample, filename,jobid, hostname))

        if fields==5:
           sample, filename, jobid, hostname, time_stamp, _, _ = parse_message(m.get_body(), fields)
           printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s\n" %(sample, filename,jobid, hostname,time_stamp))
        if fields==6:
           sample, filename, jobid, hostname, time_stamp,  size, _ = parse_message(m.get_body(), fields)
           printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s; SIZE\t%s\n" %(sample, filename,jobid, hostname,time_stamp, size))
        if fields==7:
           sample, filename, jobid, hostname, time_stamp, size, duration  = parse_message(m.get_body(), fields)
           printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s; SIZE\t%s; DURATION\t%s\n" %(sample, filename,jobid, hostname,time_stamp, size, time_mins))







        times =datetime.datetime(time_stamp)
        print times


     #rs = q.get_messages(visibility_timeout=0)

#     sample, filename, jobid = parse_message(msg)

#     if sample==None:
#       return None, None, None
#     printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s\n" %(sample, filename,jobid))

#     q.delete_message(m)

#     return sample, filename, jobid


def monitor(options):

    read_status(options, options.completequeue, fields=6)
    read_status(options, options.runningqueue, fields=6)
    read_status(options, options.submittedqueue, fields=6)



def clear_queue(options, queuename):
     conn = boto.sqs.connect_to_region(REGION, 
                                       aws_access_key_id = ACCESS_KEY, 
                                       aws_secret_access_key=SECRET_KEY)
     q = conn.get_queue(queuename)
     if q==None:
        print "ERROR: SQS %s does not exist" %(queuename)
        sys.exit(0)

     printf("\tClearing queue : %s\n",queuename)

     rs=q.get_messages(1)
     while len(rs)>0:
         q.delete_message(rs[0])
         rs=q.get_messages(1)


def clear_bucket(options, bucketname):
     bucket_conn= boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
     mybucket = bucket_conn.get_bucket(bucketname) # Substitute in your bucket name

     printf("\tClearing bucket : %s\n",bucketname)
     for key in mybucket.list():
       print "Deleting : ", key.name
       key.delete()




def command(options):
   #clear the queues
   for queuename in options.clearqueues:
      clear_queue(options, queuename)

   #clear the buckets
   for bucketname in options.clearbuckets:
      clear_bucket(options, bucketname)


def main(argv):
  (options, args) = parser.parse_args()

  read_key(options.key)

  if options.processtype =="submiter":
      submiter(options) 

  if options.processtype =="worker":
      worker(options) 

  if options.processtype =="monitor":
      monitor(options) 

  if options.processtype =="command":
      command(options) 



if __name__ == "__main__":
   main( sys.argv[1:])
