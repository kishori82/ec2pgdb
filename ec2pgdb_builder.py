#!/usr/bin/python27

import boto, boto.sqs, boto.ec2, sys, os, time, logging, re, gzip, tarfile, random, shutil, socket, datetime

from boto.s3.key import Key
from boto.sqs.message import Message
from optparse import OptionParser
import optparse
#sudo apt-get install python-pip
#pip install -U boto


#python ec2pgdb_builder.py --key ~/.ssh/kishori.konwar.csv --role-type worker    --process extract --readyqueue ready_extract --worker_dir ~/ec2pgdb/ --runningqueue running_extract --completequeue complete_extract

#for f in `cat t`; do echo $f; echo python ec2pgdb_builder.py  --key ~/.ssh/kishori.konwar.csv --submitter_dir ~/GLOBAL-STUDY-3.0/output/ --sample $f  --role-type submitter; done

REGION = "us-east-1"
ACCESS_KEY=""
SECRET_KEY=""
BUCKET_NAME='pgdbinput1'
#QUEUE_NAME="crazyqueue1367"
QUEUE_NAME="crazyqueue1368"

SMALL=100
LARGE=1000

#logging.basicConfig(filename='debug.log',level=logging.DEBUG)
logging.basicConfig(filename='/tmp/info.log',level=logging.INFO)

script_name = sys.argv[0]
usage = script_name + """--sample <name> --input <input> 
     e.g.      # Parse command line
          python ec2pgdb_builder.py --key ~/.ssh/kishori.konwar.csv --role-type worker    --process extract --readyqueue ready_extract --worker_dir ~/ec2pgdb/ --runningqueue running_extract --completequeue complete_extract

          for f in `cat t`; do echo $f; echo python ec2pgdb_builder.py  --key ~/.ssh/kishori.konwar.csv --submitter_dir ~/GLOBAL-STUDY-3.0/output/ --sample $f  --role-type submitter; done"""

parser = OptionParser(usage=usage)


parser.add_option("--key", dest="key", default ='/home/ubuntu/.ssh/rootkey1.txt', help="the AWS key")
parser.add_option("--role-type", dest="roletype", default =None, choices=["worker", "submitter", "uploader", "monitor","command"], 
                help="[worker, submitter, uploader,  monitor, command]")

parser.add_option("--sample", dest="samples", action='append', default =[], help="sample name")


aws_group = optparse.OptionGroup(parser, 'S3 and SQS options-inputs and queues')
aws_group.add_option("--inputbucket", dest="inputbucket", default ="pgdbinput1", help="input bucket [ def: pgdbinput1 ]")
aws_group.add_option("--outputbucket", dest="outputbucket", default ="pgdboutput1", help="output bucket [ def : pgdboutput1 ] ")

aws_group.add_option("--readyqueue", dest="readyqueue", default ="ready",  help="ready queue [ def: None ]")

aws_group.add_option("--submittedqueue", dest="submittedqueue", default='submitted', help="ready queue [ def: submitted ]")
aws_group.add_option("--runningqueue", dest="runningqueue", default ='running', help="running queue [ def: running ]")
aws_group.add_option("--completequeue", dest="completequeue", default ='complete', help="complete queue [ def: complete] ")

parser.add_option_group(aws_group)

worker_group = optparse.OptionGroup(parser, 'worker group options')
worker_group.add_option("--worker_dir", dest="worker_dir", default ="./", help="dir  where the ptools input is dumped for processing [default:  ./ ] ")
parser.add_option_group(worker_group)

submitter_group = optparse.OptionGroup(parser, 'submitter group options')
submitter_group.add_option("--submitter_dir", dest="submit_dir", default ="./", help="dir where the inputs are [ default:  ./ ]")
parser.add_option_group(submitter_group)

monitor_group = optparse.OptionGroup(parser, 'monitor options')
monitor_group.add_option("--stats", dest="stats", action='store_true', default = False, help="reports the number of small, medium and large size samples [ def: False]")
monitor_group.add_option("--status", dest="status", default =None, help="status by sample or jobid")
monitor_group.add_option("--status-jobs", dest="status_jobs", action='store_true', default=False, help="status by sample or jobid")
monitor_group.add_option("--status-servers", dest="status_servers", action='store_true', default=False, help="status by servers by latest completion")
monitor_group.add_option("--status-SQS", dest="status_SQS", action='store_true', default=False, help="status by printing the SQSs")
monitor_group.add_option("--verbose", dest="verbose", action='store_true', default=False, help="print the detailed SQS queues")


parser.add_option_group(monitor_group)

uploader_group = optparse.OptionGroup(parser, 'process option')
#uploader_group.add_option("--sample", dest="samples", action='append', default =[], help="sample name")
uploader_group.add_option("--process", dest="process", default ="pathologic", choices= ['pathologic', 'extract'],  help="process pathologic/extract [ def: pathologic]")
parser.add_option_group(uploader_group)


command_group = optparse.OptionGroup(parser, 'command options')
command_group.add_option("--download", dest="download", action='append', default = [], help="download sample [ def: [] ]")
command_group.add_option("--delete", dest="delete", action='store_true', default = False, help="removes the output file after downloading [ def: False]")
command_group.add_option("--clearqueue", dest="clearqueues", action='append', default = [], help="queues to clear [ def: [] ]")
command_group.add_option("--clearbucket", dest="clearbuckets", action='append',  default = [], help="queues to clear [ def: [] ]")

parser.add_option_group(command_group)

def fprintf(file, fmt, *args):
   file.write(fmt % args)

def printf(fmt, *args):
   sys.stdout.write(fmt % args)
 
def eprintf(fmt, *args):
   sys.stderr.write(fmt % args)
   sys.stderr.flush()

def getstatusoutput(cmd):
    """Return (status, output) of executing cmd in a shell."""
    pipe = os.popen(cmd + ' 2>&1', 'r')
    text = pipe.read()
    sts = pipe.close()
    if sts is None: sts = 0
    if text[-1:] == '\n': text = text[:-1]
    return sts, text



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




def do_some_work(samplefolder):

   cmd = ['/home/ubuntu/pathway-tools/pathway-tools',  '-patho',  samplefolder + '/', '-no-web-cel-overview',  '-no-taxonomic-pruning']

   result = getstatusoutput(' '.join(cmd))

   if result[0]==0:
       return True
   return False

def do_some_work_extract(sample, samplecyc):
   HOME =  os.environ['HOME']
   cmd = [ 'python',  HOME + '/ec2pgdb/pwy_extract/libs/python_scripts/MetaPathways_run_pathologic.py',  '--reactions',
             HOME + '/ec2pgdb/pwy_extract/output/'+ sample + '.metacyc.orf.annots.txt', 
             '--ptoolsExec',  HOME + '/pathway-tools/pathway-tools', 
             '-i', HOME +'/ec2pgdb/pwy_extract/' + sample + '/ptools/', 
             '-p', HOME + '/ptools-local/pgdbs/user/' + samplecyc, 
             '-s', sample, '--wtd', '--annotation-table',  
             HOME + '/ec2pgdb/pwy_extract/' + sample + '/' + sample + '.functional_and_taxonomic_table.txt', 
             '--ncbi-tree',  HOME + '/ec2pgdb/pwy_extract/resouces_files/ncbi_taxonomy_tree.txt', 
             '--ncbi-megan-map', HOME + '/ec2pgdb/pwy_extract/resouces_files/ncbi.map',
             '--output-pwy-table', HOME + '/ec2pgdb/pwy_extract/output/' + samplecyc + '.pwy.txt' 
           ]
      
   #print ' '.join(cmd)
   result = getstatusoutput(' '.join(cmd))

   if result[0]==0:
       return True
   return False




def gunzip_file(outputdir,  tar_gz_file):
    targz  = tarfile.open(outputdir + tar_gz_file)
    targz.extractall(path=outputdir)
    targz.close()

def parse_message(msg, fields=3):
   mesPATT3= re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)')
   mesPATT4 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)')
   mesPATT5 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)\nTIME\t(.*)')
   mesPATT6 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)\nTIME\t(.*)\nSIZE\t(.*)')
   mesPATT7 = re.compile(r'SAMPLE\t(.*)\nFILENAME\t(.*)\nJOBID\t(.*)\nHOSTNAME\t(.*)\nTIME\t(.*)\nSIZE\t(.*)\nDURATION\t(.*)')

   res = None
   if fields==3:
      res = mesPATT3.search(msg)
   if fields==4:
      res = mesPATT4.search(msg)

   if fields==5:
      res = mesPATT5.search(msg)

   if fields==6:
      res = mesPATT6.search(msg)

   if fields==7:
      res = mesPATT7.search(msg)


   hostname = None
   time_stamp = None
   size = None
   duration = None
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

      if fields==7:
          hostname = res.group(4)
          time_stamp = res.group(5)
          size = res.group(6)
          duration = res.group(7)

   else:
      sample = None
      filename = None
      jobid = None
      hostname = None
      time_stamp = None

   return sample, filename, jobid, hostname, time_stamp, size, duration

def retrieve_a_job():
     conn = boto.sqs.connect_to_region(REGION, 
                                       aws_access_key_id = ACCESS_KEY, 
                                       aws_secret_access_key=SECRET_KEY)
     q = conn.get_queue(QUEUE_NAME)
     if q==None:
        print "ERROR: SQS queue %s does not exist"  %(QUEUE_NAME)
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
           tar.add(name, arcname=os.path.basename(name))


def upload_to_s3_bucket(conn, BUCKET_NAME, filepath):

    b = conn.get_bucket(BUCKET_NAME)
    k = Key(b)
    k.key= os.path.basename(filepath)
    k.set_contents_from_filename(filepath)


def upload_output_to_s3(options, samplename,  jobid):
   bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

#   print ACCESS_KEY, SECRET_KEY
   upload_to_s3_bucket(bucket_conn, options.inputbucket, "/tmp/" + "jobid-" + jobid + "-"+ samplename + ".tar.gz")
   logging.info("Uploaded sample:%s\n", samplename)
   printf("\tuploaded : %s Bucket %s\n", samplename, options.inputbucket)
   os.remove("/tmp/" + "jobid-" +  jobid + "-" + samplename + ".tar.gz")


def upload_file_to_s3(options, samplename,  jobid):
   bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

#   print ACCESS_KEY, SECRET_KEY
   if options.process=='extract':
     filepath =  "/tmp/" + "jobeid-" + jobid + "-"+ samplename + ".tar.gz"
   if options.process=='pathologic':
     filepath =  "/tmp/" + "jobid-" + jobid + "-"+ samplename + ".tar.gz"

   upload_to_s3_bucket(bucket_conn, options.inputbucket, filepath)

   logging.info("Uploaded sample:%s\n", samplename)
   printf("\tuploaded : %s Bucket %s\n", samplename, options.inputbucket)

   os.remove(filepath)

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

   m.set_body("SAMPLE\t%s\nFILENAME\t%s\nJOBID\t%s\nHOSTNAME\t%s\nTIME\t%s\nSIZE\t%s\nDURATION\t%s\n" %(samplename, filename, jobid, hostname, event_time, size, time_min))
   q.write(m)
   printf("\tINFO:\n\t\tSAMPLE\t%s\n\t\tFILENAME\t%s\n\t\tJOBID\t%s\n\t\tHOSTNAME\t%s\n\t\tTIME\t%s\n\t\tSIZE\t%s\n\t\tDURATION\t%s\n" %(samplename, filename, jobid, hostname, event_time, size, time_min))

 
def sanity_check(foldername) :
     pf = foldername + "/ptools/" + "0.pf"
     gen_elem= foldername + "/ptools" + "/genetic-elements.dat"
     org_params = foldername + "/ptools" + "/organism-params.dat"

     if not os.path.exists(pf):
        return False

     if not os.path.exists(gen_elem):
        return False

     if not os.path.exists(org_params):
        return False

     return True 
    
def submitter(options):

    for sample in options.samples:
       print "SUBMITTING : ", sample
       submitter_daemon(options, sample)


def submitter_daemon(options, samplename):
    if options.submit_dir!=None:
       if sanity_check(options.submit_dir + "/" + samplename):
         print "\t", "#ORFS : ", count_orfs(options.submit_dir + "/" + samplename )
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
               if num < SMALL:
                  suffix = "_small"
                  size = "small"
               elif num > SMALL and num < LARGE:
                  suffix = "_medium"
                  size = "medium"
               else:
                  suffix = "_large"
                  size = "large"

               submit_time =  str(datetime.datetime.now())
               submit_min =  str(time.time()/60)
               hostname = socket.gethostname().strip()
 
               submit_sample_name_to_SQS(options.readyqueue + suffix, samplename, filename, jobid, hostname, submit_time, size, submit_min)
               submit_sample_name_to_SQS(options.submittedqueue, samplename, filename, jobid, hostname, submit_time, size, submit_min)

def uploader(options):
    for sample in options.samples:
       print "UPLOADING : ", sample
       uploader_daemon(options, sample)



def create_upload_file_extract(foldername, jobid):
    import tarfile

    samplename = os.path.basename(foldername)
    pf = foldername + "/ptools/" + "0.pf"
    gen_elem= foldername + "/ptools/genetic-elements.dat"
    org_params = foldername + "/ptools/organism-params.dat"
    reduced = foldername + "/ptools/reduced.txt"
    table = foldername + "/results/annotation_table/" + samplename + ".functional_and_taxonomic_table.txt"


    with tarfile.open("/tmp/"+ "jobeid-" + jobid + "-" + samplename + ".tar.gz", "w:gz") as tar:
       t = tarfile.TarInfo(samplename)
       t.type = tarfile.DIRTYPE
       t.mode = 0755
       t.mtime = time.time()
       tar.addfile(t)

       t = tarfile.TarInfo(samplename + '/ptools')
       t.type = tarfile.DIRTYPE
       t.mode = 0755
       t.mtime = time.time()
       tar.addfile(t)

       print "\t", "adding :", table
       tar.add(table, arcname=samplename + '/' +  samplename + ".functional_and_taxonomic_table.txt")

       print "\t", "adding :", pf
       tar.add(pf, arcname=samplename + '/ptools/0.pf')
       print "\t", "adding :", org_params
       tar.add(pf, arcname=samplename + '/ptools/organism-params.dat')
       print "\t", "adding :", gen_elem
       tar.add(pf, arcname=samplename + '/ptools/genetic-elements.dat')
       if os.path.exists(reduced):
         print "\t", "adding :", reduced
         tar.add(pf, arcname=samplename + '/ptools/reduced.txt')


def submitter_extract(options):
    for sample in options.samples:
       print "SUBMITTING : ", sample
       submitter_extract_daemon(options, sample)

def submitter_extract_daemon(options, samplename):
    if options.submit_dir!=None:
       if sanity_check(options.submit_dir + "/" + samplename):
         print "\t", "#ORFS : ", count_orfs(options.submit_dir + "/" + samplename )
         num = count_orfs(options.submit_dir + "/" + samplename)
          
         jobid = str(random.randrange(0,1000000000)) 
         if os.path.exists("/tmp/" + "jobeid-" + jobid + "-" + samplename + ".tar.gz"):
             os.remove("/tmp/" +  "jobeid-" + jobid + "-" +  samplename + ".tar.gz")

         create_upload_file_extract(options.submit_dir + "/" + samplename, jobid)

         if options.inputbucket != None:
            upload_file_to_s3(options, samplename, jobid)

            if options.readyqueue != None:
               filename = "jobeid-" + jobid + "-" + samplename + ".tar.gz"
               suffix = "_extract"

               submit_time =  str(datetime.datetime.now())
               submit_min =  str(time.time()/60)
               hostname = socket.gethostname().strip()
 
               submit_sample_name_to_SQS(options.readyqueue + suffix, samplename, filename, jobid, hostname, submit_time, 'NA', submit_min)
               submit_sample_name_to_SQS(options.submittedqueue, samplename, filename, jobid, hostname, submit_time, 'NA', submit_min)


def create_tarzip_file(foldername, jobid):
    import tarfile

    pf = foldername + "/ptools/" + "0.pf"
    gen_elem= foldername + "/ptools/genetic-elements.dat"
    org_params = foldername + "/ptools/organism-params.dat"

    samplename = os.path.basename(foldername)
    with tarfile.open("/tmp/"+ "jobid-" + jobid + "-" + samplename + ".tar.gz", "w:gz") as tar:
       for name in [pf,  gen_elem, org_params] :
           print "\t", "adding :", name
           tar.add(name, arcname=os.path.basename(name))




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
       return None, None, None, None

     rs = q.get_messages()
     print rs[0].get_body()

     m = q.read()
     msg = str(m.get_body())

     sample, filename, jobid, hostname, time_stamp, size, duration= parse_message(msg, fields=7)

     if sample==None:
       return None, None, None, None

     printf("\tRETRIVED:\n\t\tSAMPLE\t%s\n\t\tFILENAME\t%s\n\t\tJOBID\t%s\n\t\tHOSTNAME\t%s\n\t\tTIME\t%s\n\t\tSIZE\t%s\n\t\tDURATION\t%s\n" %(sample, filename, jobid, hostname,  time_stamp, size, duration  ))

     q.delete_message(m)

     return sample, filename, jobid, size

def download_file(bucketname, folder, filename, delete=False):

   printf("\n\tDownloading : %s\n",  filename)
   bucket_conn= boto.connect_s3(aws_access_key_id = ACCESS_KEY, 
                                        aws_secret_access_key=SECRET_KEY)
   mybucket = bucket_conn.get_bucket(bucketname) # Substitute in your bucket name

   k = mybucket.get_key(filename)
   result = k.get_contents_to_filename(folder +  os.path.basename(filename))

   if delete:
      k.delete()


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
       #print "\tSAMPLE %s; FILENAME : %s; JOBID : %s\n" %(sample, filename,jobid)

       if filename!=None:
          result = download_file(options.inputbucket, "/tmp/", filename, delete=True)

          os.makedirs(options.worker_dir + '/' + sample)
          gunzip_file(options.worker_dir + '/' + sample,  '/tmp/' + filename)
          os.remove('/tmp/' + filename)

          if options.runningqueue!=None:
             print "\tUpdate Running SQS : %s\n" %(options.runningqueue)
             start_time =  str(datetime.datetime.now()) 
             start_min =  str(time.time()/60) 
             hostname = socket.gethostname().strip()
             submit_sample_name_to_SQS(options.runningqueue, sample, filename, jobid, hostname, start_time, size, start_min)

          success = do_some_work(options.worker_dir + '/' + sample)
          shutil.rmtree( options.worker_dir + '/' + sample)

          if success:
             pgdbfolder='/home/ubuntu/ptools-local/pgdbs/user/' + sample.lower() + 'cyc'
             if os.path.exists(pgdbfolder):
                tar = tarfile.open('/tmp/' + sample.lower() + "cyc.tar.gz", "w:gz")
                tar.add(pgdbfolder, arcname=sample.lower())
                tar.close()
                     
             shutil.rmtree(pgdbfolder)


             bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
             upload_to_s3_bucket(bucket_conn, options.outputbucket, "/tmp/" +  sample.lower() + "cyc.tar.gz")
             os.remove("/tmp/" +  sample.lower() + "cyc.tar.gz")

             end_min =  str(time.time()/60)
             if options.completequeue!=None:
                print "\tUpdate Complete SQS : %s\n" %(options.completequeue)
                submit_sample_name_to_SQS(options.completequeue, sample, filename, jobid, hostname,  start_time, size, end_min)





def worker_extract(options):
    if not os.path.exists(options.worker_dir):
       os.mkdir(options.worker_dir)

    while True:
      try:
        worker_daemon_extract(options)
      except:
        pass
      time.sleep(5)
      

def worker_daemon_extract(options):
    
    
    if options.readyqueue!=None:
       print "READING :", options.readyqueue
       sample, filename, jobid, size  = read_a_message(options)
       #print "\tSAMPLE %s; FILENAME : %s; JOBID : %s\n" %(sample, filename,jobid)

       if filename!=None:
          ''' Download the inputs containing  ORF annotations'''
          result = download_file(options.inputbucket, "/tmp/", filename, delete=True)
          if os.path.exists( options.worker_dir + '/' + sample):
             shutil.rmtree( options.worker_dir + '/' + sample)
          os.makedirs(options.worker_dir + '/' + sample)
          gunzip_file(options.worker_dir + '/',  '/tmp/' + filename)
          os.remove('/tmp/' + filename)

          '''Download the ePGDB'''
          filetoget = sample.lower() + "cyc.tar.gz"
          samplecyc = sample.lower() + "cyc"

          home = os.environ['HOME']
          if os.path.exists(home + "/ptools-local/pgdbs/user/" + samplecyc):
              shutil.rmtree(home + "/ptools-local/pgdbs/user/" + samplecyc)

          result = download_file(options.outputbucket, "/tmp/", filetoget, delete=False)
          gunzip_file(options.worker_dir + '/' + sample + '/',    '/tmp/' + filetoget)
          os.rename(options.worker_dir + '/' + sample + '/'+ sample, home + "/ptools-local/pgdbs/user/" + samplecyc)
          os.remove('/tmp/' + filetoget)

          if options.runningqueue!=None:
             '''Update the SQS running queue'''
             print "\tUpdate Running SQS : %s\n" %(options.runningqueue)
             start_time =  str(datetime.datetime.now()) 
             start_min =  str(time.time()/60) 
             hostname = socket.gethostname().strip()
             submit_sample_name_to_SQS(options.runningqueue, sample, filename, jobid, hostname, start_time, size, start_min)

          '''extract the pathways'''
          success = do_some_work_extract(sample, samplecyc)
          shutil.rmtree( options.worker_dir + '/' + sample)
          shutil.rmtree(home + "/ptools-local/pgdbs/user/" + samplecyc)

          if success:
             bucket_conn = boto.connect_s3(aws_access_key_id = ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

             #print options.outputbucket
             #print "Update Complete SQS : %s\n" %(options.completequeue)
             #print 'uploading',  home + '/ec2pgdb/pwy_extract/output/' + samplecyc + '.pwy.txt' 
             #print 'uploading',  home + '/ec2pgdb/pwy_extract/output/' + sample + '.metacyc.orf.annots.txt'
             upload_to_s3_bucket(bucket_conn, options.outputbucket, home + '/ec2pgdb/pwy_extract/output/' + samplecyc + '.pwy.txt' )
             upload_to_s3_bucket(bucket_conn, options.outputbucket, home + '/ec2pgdb/pwy_extract/output/' + sample + '.metacyc.orf.annots.txt')
             os.remove( home + '/ec2pgdb/pwy_extract/output/' + samplecyc + '.pwy.txt' )
             os.remove(home + '/ec2pgdb/pwy_extract/output/' + sample + '.metacyc.orf.annots.txt')

             end_min =  str(time.time()/60)
             if options.completequeue!=None:
                print "\tUpdate Complete SQS : %s\n" %(options.completequeue)
                submit_sample_name_to_SQS(options.completequeue, sample, filename, jobid, hostname,  start_time, size, end_min)




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

     if options.process=='extract':
       queuename += '_extract'

     q = conn.get_queue(queuename)
     if q==None:
        print "ERROR: SQS my-queue does not exist"
        sys.exit(0)

     count = q.count()
     logging.info("Number of jobs in queue:%s", count)
     printf("\t# Jobs in SQS %s : %s\n",queuename, count)

     timeout = count/10
     all_messages=[]
     rs=q.get_messages(10, visibility_timeout=timeout)
     while len(rs)>0:
       all_messages.extend(rs)
       rs=q.get_messages(10, visibility_timeout=timeout)
    # all_messages = q.get_messages(10)

     statistics = {}
     if options.status_SQS:
          printf("SQS QUEUE: %s\n" %(queuename))
     for m in all_messages:
        
        if fields==3:
           sample, filename, jobid, _, _, _,_ = parse_message(m.get_body(), fields)
           if options.status_SQS:
              printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s\n" %(sample, filename,jobid))

        if fields==4:
           sample, filename, jobid, hostname, _, _,_ = parse_message(m.get_body(), fields)
           if options.status_SQS:
              printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s\n" %(sample, filename,jobid, hostname))

        if fields==5:
           sample, filename, jobid, hostname, time_stamp, _, _ = parse_message(m.get_body(), fields)
           if options.status_SQS:
              if options.verbose:
                 printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s\n" %(sample, filename,jobid, hostname,time_stamp))


        if fields==6:
           sample, filename, jobid, hostname, time_stamp,  size, _ = parse_message(m.get_body(), fields)
           if options.status_SQS:
              if options.verbose:
                 printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s; SIZE\t%s\n" %(sample, filename,jobid, hostname,time_stamp, size))
        if fields==7:
           sample, filename, jobid, hostname, time_stamp, size, time_mins  = parse_message(m.get_body(), fields)
           if options.status_SQS:
              if options.verbose:
                 printf("\tReceived: SAMPLE\t%s; FILENAME\t%s; JOBID\t%s;  HOSTNAME\t%s; TIME\t%s; SIZE\t%s; DURATION\t%s\n" %(sample, filename,jobid, hostname,time_stamp, size, time_mins))
            
        statistics[jobid] = [sample, filename, jobid, hostname, time_stamp, size, time_mins ]

     return statistics 


def monitor(options):
    submitted = read_status(options, options.submittedqueue, fields=7)
    running =read_status(options, options.runningqueue, fields=7)
    complete = read_status(options, options.completequeue, fields=7)

    if options.status_jobs:
       print_status_jobs(submitted, running, complete)

    if options.status_servers:
       print_status_servers(running, complete)



def get_servers_times(states, instance_ips, servers, i):
     for k in sorted(states.keys()):
        a = states[k]
        if a[3] in instance_ips:
          if not a[3] in servers[i]: 
             servers[i][a[3]] = []
          servers[i][a[3]].append(float(a[6]))



def print_status_servers(running, complete): 
      instance_ips = get_ec2_instances()
      current_time =  time.time()
      servers=[ {}, {}  ]
   
      get_servers_times(running, instance_ips, servers, 0)
      get_servers_times(complete, instance_ips, servers, 1)

      printf("%4s\t%7s\t%7s\t%6s\t%5s\t%20s\t%15s\t%50s\n\n", 'NO', 'START_T', 'END_T', '#TASKS', 'STAT', 'SERVER', 'REGION', 'HOSTNAME')
     
      count = 1
      num_task =0

      for instance in instance_ips:
           printf("%4d\t", count)
           start_time  = 10000 
           if instance in servers[0]:
              start_time  = (current_time -max(servers[0][instance])*60)/60 
              printf("%7.2f\t", (current_time -max(servers[0][instance])*60)/60 ) 
           else:
              printf("%7.2f\t", start_time ) 

           end_time = 10000
           if instance in servers[1]:
              end_time=(current_time -max(servers[1][instance])*60)/60 
              printf("%7.2f\t%6d\t", end_time, len(servers[1][instance]) ) 
              num_task += len(servers[1][instance])
           else:
              printf("%7.2f\t%6s\t", end_time, '-' ) 

           if end_time > start_time:
                printf("%5s\t", 'BUSY' ) 
           else:
                printf("%5s\t", 'IDLE' ) 

           printf("%20s\t%15s\t%50s\n", instance, instance_ips[instance][0], instance_ips[instance][1])
           count += 1


      printf("%4s\t%7s\t%7s\t%7d\n", 'Total','','', num_task)


def print_status_jobs(submitted, running, complete): 
    current_time =  time.time()
    printf("%12s\t%40s\t%10s\t%10s\t%10s\t%10s\n",'JOBID', 'SAMPLE', 'RUNNING', 'COMPLETE', 'SUB_TIME', 'MINUTES')
    for jobid in submitted:
       #print submitted[jobid]  
       printf("%12s\t%40s", jobid, submitted[jobid][0]);
       if jobid in running:
           printf("\t%10s","RUNNING");
       else:
           printf("\t%10s","WAITING");

       if jobid in complete:
           printf("\t%10s","COMPLETE");
       else:
           printf("\t%10s","       ");


       if jobid in running:
          printf("\t%8.2f", (current_time-float(running[jobid][6])*60)/60)
       else:
           printf("\t    ")

       if jobid in running and jobid in complete:
            printf("\t%8.2f\n", float(complete[jobid][6])-float(running[jobid][6]))
       else:
            printf("\t   \n")



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


   for sample in options.download:
     try:
      result = download_file(options.outputbucket, "", sample.lower() + "cyc.tar.gz", delete=options.delete)
     except:
       print "ERROR: Failed to download %s\n" %(sample)



   if options.stats:
     size={'SMALL':0, 'MEDIUM':0, 'LARGE':0}
     for samplename in options.samples:
        num = count_orfs(options.submit_dir + "/" + samplename)
        if num < SMALL:
            size['SMALL'] += 1
        elif num > SMALL and num < LARGE:
            size['MEDIUM'] += 1
        else:
            size['LARGE'] += 1

     print 'SMALL :', size['SMALL'], 'MEDIUM :', size['MEDIUM'], 'LARGE :', size['LARGE'],


def get_ec2_instances():
     instance_ips = {}
     regions = ['us-east-1','us-east-2', 'us-west-1','us-west-2', 'eu-west-1', 'eu-west-2', 'eu-central-1', 'ca-central-1', 'eu-west-1','sa-east-1']
   #, 'ap-south-1', 'ap-southeast-1','ap-southeast-2','ap-northeast-1']
     for region in regions: 
       print region
       ips =get_ec2_instances_in_region(region)
       for key, value in ips.iteritems():
    
         ip = re.sub(r'[.]','-', key)
         instance_ips[ip] = value
    
     return instance_ips

def get_ec2_instances_in_region(region):
    ips = {}
    ec2_conn = boto.ec2.connect_to_region(region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    reservations = ec2_conn.get_all_reservations()
    for reservation in reservations:    
        for instance in reservation.instances:
           if instance.private_ip_address:
              ips["ip-" + str(instance.private_ip_address)] = [region,  str(instance.public_dns_name) ]

    return ips
       
    #for vol in ec2_conn.get_all_volumes():
    #    print region+':',vol.id


def main(argv):
  (options, args) = parser.parse_args()

  read_key(options.key)

  if options.roletype == None:
      print 'ERROR: Must specify a role-type'

  if options.roletype =="submitter":
     if options.process=='pathologic':
        submitter(options) 
     if options.process=='extract':
        submitter_extract(options) 

  if options.roletype =="worker":
     if options.process=='pathologic':
        worker(options) 
     if options.process=='extract':
        worker_extract(options) 

  if options.roletype =="monitor":
      monitor(options) 

  if options.roletype =="command":
      command(options) 


if __name__ == "__main__":
   main( sys.argv[1:])

