#!/usr/bin/python

"""This script run the pathologic """

try:
   import copy, optparse, sys, re, csv, traceback
   from os import path, _exit, rename
   import logging.handlers
   from glob import glob
   import multiprocessing

   from libs.python_modules.utils.sysutil import pathDelim
   from libs.python_modules.utils.metapathways_utils  import fprintf, printf, eprintf, exit_process, getReadFiles
   from libs.python_modules.utils.sysutil import getstatusoutput

   from libs.python_modules.utils.pathwaytoolsutils import *

except:
     print """ Could not load some user defined  module functions"""
     print """ Make sure your typed 'source MetaPathwaysrc'"""
     print """ """
     print traceback.print_exc(10)
     sys.exit(3)


PATHDELIM= pathDelim()

def fprintf(file, fmt, *args):
    file.write(fmt % args)

def printf(fmt, *args):
    sys.stdout.write(fmt % args)

def files_exist( files , errorlogger = None):
    status = True    
    for file in files:
       if not path.exists(file):
          if errorlogger:
             errorlogger.write( 'ERROR\tCould not find ptools input  file : ' +  file )
          status = False
    return not status


epilog = """\n""" + """
                This script computes the RPKM values for each ORF, from the BWA recruits.  The input reads 
                (in the form of fastq files)  for this step must be added to the subdirectory reads in the 
                input folder (where the input fasta  files are located). The read file sare identified by 
                the name format of the files: For examples, if the sample name is "abcd" then the following 
                read files in the "reads" folders associated with the samples abcd:
                
                1.   abcd.fastq : this means non-paired reads

                2.   abcd.b1.fastq  : means only unpaired read from batch b1

                3.   abcd_1.fastq  and abcd_2.fastq: this means paired reads for sample

                4.   abcd_1.fastq or abcd_2.fastq: this means only one end of a paired read

                5.   abcd_1.b2.fastq and  abcd_2.b2.fastq: this means paried reads from batch b2, note that batches are idenfied as bn, 
                     where n is a number

                6.   abcd_1.b3.fastq or abcd_2.b3.fastq: this means only one of a paried read from batch b1
             """


usage = sys.argv[0] + """ -c <contigs> -o <output> -r <reads>  -O <orfgff> --rpkmExec <rpkmexec> """ + epilog
parser = None
def createParser():
    global parser


    parser = optparse.OptionParser(usage=usage)

    # Input options


    parser.add_option('-c', '--contigs', dest='contigs', default=None,
                           help='the contigs file')

    parser.add_option('-o', '--output', dest='output', default=None,
                           help='orfwise read count file')

    parser.add_option('-m', '--microbecensusoutput', dest='microbecensusoutput', default=None,
                           help='output from the MicrobeCensus run')

    parser.add_option('--stats', dest='stats', default=None,
                           help='output stats for ORFs  into file')

    parser.add_option('-r', '--readsdir', dest='readsdir', default=None,
                           help='the directory that should have the read files')

    parser.add_option('-O', '--orfgff', dest='orfgff', default=None,
                           help='folder of the PGDB')

    parser.add_option('-s', '--sample_name', dest='sample_name', default=None,
                           help='name of the sample')

    parser.add_option('--rpkmExec', dest='rpkmExec', default=None,
                           help='RPKM Executable')

    parser.add_option('--bwaExec', dest='bwaExec', default=None,
                           help='BWA Executable')

    parser.add_option('--bwaFolder', dest='bwaFolder', default=None,
                           help='BWA Folder')


def getSamFiles(readdir, sample_name):
   '''This function finds the set of SAM files that has the BWA recruitment information'''

   samFiles = []
   _samFile = glob(readdir + PATHDELIM + sample_name + '.sam')

   if _samFile:
      samFiles += _samFile

   _samFiles = glob(readdir + PATHDELIM + sample_name + '_[0-9]*.sam')

   if _samFiles:
     samFiles += _samFiles

   return samFiles



def indexForBWA(bwaExec, contigs,  indexfile):
    cmd = "%s index -p %s %s"  %(bwaExec, indexfile, contigs, )

    result = getstatusoutput(cmd)

    if result[0]==0:
       return True

    return False


def runUsingBWA(bwaExec, sample_name, indexFile,  _readFiles, bwaFolder) :

    num_threads =  int(multiprocessing.cpu_count()*0.8)
    if num_threads < 1:
       num_threads = 1
    status = True
    count = 0;
    for readFiles in _readFiles:
       bwaOutput = bwaFolder + PATHDELIM + sample_name + "_" + str(count) + '.sam'
       bwaOutputTmp = bwaOutput + ".tmp"
       cmd ="command not prepared"
       if len(readFiles) == 2:
          cmd = "%s mem -t %d %s %s %s > %s"  %(bwaExec, num_threads, indexFile,  readFiles[0], readFiles[1], bwaOutputTmp )

       if len(readFiles) == 1:
          res0 = re.search(r'_[1-2].fastq',readFiles[0])
          res1 = re.search(r'_[1-2].b\d+.fastq',readFiles[0])
          if res0 or res1:
             cmd = "%s mem -t %d  %s %s > %s "%(bwaExec, num_threads, indexFile,  readFiles[0], bwaOutputTmp)
          else:
             cmd = "%s mem -t %d -p  %s %s > %s "%(bwaExec, num_threads,  indexFile,  readFiles[0], bwaOutputTmp)
       result = getstatusoutput(cmd)

       if result[0]==0:
          pass
          rename(bwaOutputTmp, bwaOutput)
       else:
          eprintf("ERROR:\t Error file processing read files %s\n", readFiles)
          status = False
       count += 1
          
    return status

def runMicrobeCensus(microbeCensusExec, microbeCensusOutput,  sample_name, readFiles, rpkmFolder) :

    num_threads =  int(multiprocessing.cpu_count()*0.8)
    if num_threads < 1:
       num_threads = 1
    status = True

    readfiles= [ ','.join(read) for read in readFiles ]

    
    if len(readFiles) == 2:
       command_frags = [microbeCensusExec, ','.join(readfiles), microbeCensusOutput + ".tmp"]

       result = getstatusoutput(' '.join(command_frags))
       print ' '.join(command_frags)

       if result[0]==0:
          pass
          rename(microbeCensusOutput+".tmp", microbeCensusOutput)
       else:
          eprintf("ERROR:\tError while running MicrobeCensus on read  files %s\n", readFiles)
          status = False
    else:
          eprintf("ERROR:\tThe number of read files for MicrobeCensus must be at most 3. Found %d:%s\n", len(readFiles), ','.join(readFiles))
          status = False
          
    return status


def read_genome_equivalent(microbecensusoutput):
    gen_equiv_patt = re.compile(r'genome_equivalents:\s+(.*)$')
    
    with open(microbecensusoutput, 'r') as inputfile: 
        lines = inputfile.readlines()
        for line in lines:
          result =  gen_equiv_patt.search(line)
          if result:
             genome_equivalent = result.group(1)
             try:
               return float(genome_equivalent)
             except:
               return 1

    return 1
        

def main(argv, errorlogger = None, runcommand = None, runstatslogger = None):
    global parser

    options, args = parser.parse_args(argv)
    if not (options.contigs!=None and  path.exists(options.contigs)):
       parser.error('ERROR\tThe contigs file is missing')
       return 255

    if not (options.rpkmExec !=None and path.exists(options.rpkmExec) ) :
       parser.error('ERROR\tThe RPKM executable is missing')
       return 255 

    if not (options.bwaExec !=None and path.exists(options.bwaExec) ) :
       parser.error('ERROR\tThe BWA executable is missing')
       return 255 

    if not (options.readsdir !=None and path.exists(options.readsdir) ):
       parser.error('ERROR\tThe RPKM directory is missing')
       return 255 

    if not (options.bwaFolder !=None and path.exists(options.bwaFolder) ):
       parser.error('ERROR\tThe BWA directory is missing')
       return 255 

    if  options.sample_name==None :
       parser.error('ERROR\tThe sample name is missing')
       return 255 


    # read the input sam and fastq  files
    samFiles = getSamFiles(options.readsdir, options.sample_name)
    readFiles = getReadFiles(options.readsdir, options.sample_name)
    if not  samFiles:
        samFiles = getSamFiles(options.bwaFolder, options.sample_name)

    genome_equivalent = 1
    if not samFiles and readFiles:
        if not readFiles:
           eprintf("ERROR\tCannot find the read files not found for sample %s!\n", options.sample_name)
           eprintf("ERROR\tMetaPathways need to have the sample names in the format %s.fastq or (%s_1.fastq and %s_2.fastq) !\n", options.sample_name, options.sample_name, options.sample_name)
           if errorlogger:
             errorlogger.eprintf("ERROR\tCannot find the read files not found for sample %s!\n", options.sample_name)
             errorlogger.eprintf("ERROR\tMetaPathways need to have the sample names in the format %s.fastq or (%s_1.fastq and %s_2.fastq) !\n", options.sample_name, options.sample_name, options.sample_name)
             return 255
    
        # index for BWA
        bwaIndexFile = options.bwaFolder + PATHDELIM + options.sample_name
        indexSuccess = indexForBWA(options.bwaExec, options.contigs, bwaIndexFile) 
        #indexSuccess=True

        if not indexSuccess:
           eprintf("ERROR\tCannot index the preprocessed file %s!\n", options.contigs)
           if errorlogger:
              errorlogger.eprintf("ERROR\tCannot index the preprocessed file %s!\n", options.contigs)
           return 255
           #exit_process("ERROR\tMissing read files!\n")
    
        # run the microbe Census  if not computed already
        if not path.exists(options.microbecensusoutput):
            microbeCensusStatus = runMicrobeCensus("run_microbe_census.py", options.microbecensusoutput, options.sample_name, readFiles, options.readsdir) 
            if microbeCensusStatus:
               print 'Successfully ran MicrobeCensus!'
            else:
               eprintf("ERROR\tCannot successfully run MicrobeCensus for file %s!\n", options.contigs)
               if errorlogger:
                  errorlogger.eprintf("ERROR\tCannot successfully run MicrobeCensus for file %s!\n", options.contigs)
               return 255



        genome_equivalent = read_genome_equivalent(options.microbecensusoutput)
        #bwaRunSuccess = True

       
        bwaRunSuccess = runUsingBWA(options.bwaExec, options.sample_name,  bwaIndexFile, readFiles, options.bwaFolder) 
        #bwaRunSuccess = True

        if bwaRunSuccess:
           print 'Successfully ran bwa!'
        else:
           eprintf("ERROR\tCannot successfully run BWA for file %s!\n", options.contigs)
           if errorlogger:
              errorlogger.eprintf("ERROR\tCannot successfully run BWA for file %s!\n", options.contigs)
           return 255
           #exit_process("ERROR\tFailed to run BWA!\n")
           # END of running BWA

           # make sure you get the latest set of sam file after the bwa
        samFiles = getSamFiles(options.bwaFolder, options.sample_name)

    if not samFiles:
       eprintf("ERROR\tSam files not created for RPMK run!\n")
       return 255

    print 'Running RPKM'
    if not path.exists(options.rpkmExec):
       eprintf("ERROR\tRPKM executable %s not found!\n", options.rpkmExec)
       if errorlogger:
          errorlogger.printf("ERROR\tRPKM executable %s not found!\n",  options.rpkmExec)
       return 255
       #exit_process("ERROR\tRPKM executable %s not found!\n" %(options.rpkmExec))

    # command to build the RPKM
  
    command =  [
                  "%s --contigs-file %s"  %(options.rpkmExec, options.contigs),
                  "--multireads" ,
                  "--read-counts", 
                  "--genome_equivalent %0.10f" %(genome_equivalent)
               ]

    if options.output:
       command.append("--ORF-RPKM %s" %(options.output))
       command.append("--stats %s" %(options.stats))

    if options.orfgff:
       command.append("--ORFS %s" %(options.orfgff))

    for samfile in samFiles:
        command.append("-r " + samfile)

    rpkmstatus =0

    try:
       command1 = copy.copy(command)

       command1.append("--type 1")
       rpkmstatus  = runRPKMCommand(runcommand = ' '.join(command1))
       rename(options.output, options.output + ".read_counts.txt")

       command2 = copy.copy(command)
       command2.append("--type 2")
       rpkmstatus  = runRPKMCommand(runcommand = ' '.join(command2)) 
       rename(options.output, options.output + ".rpkg.txt")
    except:
       rpkmstatus = 1
       pass

    if rpkmstatus:
        print 'Successfully ran RPKM!'

    if rpkmstatus!=0:
       eprintf("ERROR\tRPKM calculation was unsuccessful\n")
       return 255
       #exit_process("ERROR\tFailed to run RPKM" )

    return rpkmstatus

def runRPKMCommand(runcommand = None):
    if runcommand == None:
      return False

    result = getstatusoutput(runcommand)
    if result[1]:
       print result[1]
    return result[0]


def runBIOMCommand(infile, outfile, biomExec="biom"):
    commands =  [biomExec,  " convert", "-i", infile, "-o", outfile,  "--table-type=\"Table\"", "--to-hdf5"]
    result = getstatusoutput(' '.join(commands))
    return result[0]



# this is the portion of the code that fixes the name

def split_attributes(str, attributes):
     rawattributes = re.split(';', str)
     for attribStr in rawattributes:
        insert_attribute(attributes, attribStr)

     return attributes


# this is the function that fixes the name
def  fix_pgdb_input_files(pgdb_folder, pgdbs = []):
     pgdb_list = glob(pgdb_folder + '/*/input/organism.dat')     

     for pgdb_organism_file in pgdb_list:
        process_organism_file(pgdb_organism_file)


def fixLine(line, id):
     fields = line.split('\t')
     if len(fields)==2:
        return fields[0]+'\t' + id
     

def getID(line):
     fields = line.split('\t')
     if len(fields)==2:
        return fields[1]
     
def process_organism_file(filel):
     patternsToFix = [ re.compile(r'NAME\tunclassified sequences'), re.compile(r'ABBREV-NAME\tu. sequences') ]
     patternID =  re.compile(r'^ID\t.*')
     try:
         orgfile = open(filel,'r')
     except IOError:
         print "ERROR : Cannot open organism file" + str(filel)
         return 

     lines = orgfile.readlines()
     newlines = []
     needsFixing = False

     id = None
     for line in lines:
         line = line.strip()
         if len(line)==0:
            continue
         flag = False

         result = patternID.search(line)
         if result:   
             id = getID(line)
          
         for patternToFix in patternsToFix:
             result = patternToFix.search(line)
             if result and id:
                 newline = fixLine(line, id)
                 newlines.append(newline)
                 flag= True
                 needsFixing = True

         if flag==False:
            newlines.append(line)

     orgfile.close()
     if needsFixing:
       write_new_file(newlines, filel)


def write_new_file(lines, output_file):
    
    print "Fixing file " + output_file 
    try:
       outputfile = open(output_file,'w')
       pass
    except IOError:
         print "ERROR :Cannot open output file "  + output_file
   
    for line in lines:
       fprintf(outputfile, "%s\n", line)

    outputfile.close()


def MetaPathways_rpkm(argv, extra_command = None, errorlogger = None, runstatslogger =None): 
    if errorlogger != None:
       errorlogger.write("#STEP\tRPKM_CALCULATION\n")
    createParser()
    returncode = main(argv, errorlogger = errorlogger, runcommand= extra_command, runstatslogger = runstatslogger)
    return (returncode,'')

if __name__ == '__main__':
    createParser()
    main(sys.argv[1:])

