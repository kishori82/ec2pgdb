#!/usr/bin/python
# File created on 27 Jan 2012.
from __future__ import division

__author__ = "Kishori M Konwar"
__copyright__ = "Copyright 2013, MetaPathways"
__credits__ = ["r"]
__version__ = "1.0"
__maintainer__ = "Kishori M Konwar"
__status__ = "Release"

try:
     from os import makedirs, sys, remove, rename
     from sys import path
     import re, math, traceback
     from copy import copy
     from optparse import OptionParser, OptionGroup

     from libs.python_modules.utils.metapathways_utils  import parse_command_line_parameters, fprintf, printf, eprintf,  exit_process, ShortenORFId
     from libs.python_modules.utils.sysutil import getstatusoutput
except:
     print """ Could not load some user defined  module functions"""
     print """ Make sure your typed 'source MetaPathwaysrc'"""
     print """ """
     sys.exit(3)


usage= sys.argv[0] +" -i/--input <input> -o/--output  <output>  -t/--type <HMM/LAST/BLAST> -l/--list <listfile> [ -a OPTIONAL ] """


parser = None

def createParser():
    global parser

    epilog = """This script parses BLAST/LAST or HMMSCAN search results of the amino acid sequences against the reference protein databases, in a tabular format.""" 


    parser = OptionParser(usage, epilog= epilog)
    parser.add_option("-i", "--input", dest="input", default=None,
                      help='the input blastout files [at least 1 REQUIRED]')
    parser.add_option("-o", "--output", dest="output",  default=None,
                      help='the parsed  output file [REQUIRED]')
    parser.add_option("-a", "--append", dest="append",  action="store_true", default=False,
                      help='open the output file in the append mode [OPTIONAL , default = False]')
    parser.add_option("-t","--type", dest="input_type", choices = ['HMM', 'LAST1', 'LAST2'],  default=None,
                      help='the type of input : HMMSCAN or LAST/BLAST output [REQUIRED]')
    parser.add_option("-l", "--list", dest="gene_list",  default=None,
                      help='the list of genes to look for [REQUIRED]')
    



def check_arguments(opts, args):
    if opts.input == None:
         print "There sould be at least one input file"  
         return False

    if opts.output == None:
         print "There sould be at least one output file"  
         return False

    if opts.input_type == None:
         print "Input type not specified"  
         return False

    if opts.gene_list==None:
         print "Gene/Enzyme name list not specified"
         return False

    return True



def create_query_dictionary(blastoutputfile, query_dictionary, algorithm, errorlogger= None ):
       seq_beg_pattern = re.compile("^#")

       try:
          blastoutfh = open( blastoutputfile,'r')
       except:
          print "ERROR : cannot open B/LAST output file " + blastoutputfile + " to parse "
          return
  
       try:
          for line in blastoutfh:
             if not seq_beg_pattern.search(line):
                 words = line.rstrip().split('\t')
                 if len(words) != 12: 
                     continue
   
                 if algorithm =='BLAST': 
                   if not words[1] in query_dictionary:
                       query_dictionary[words[1]] = True
   
                 if algorithm =='LAST': 
                     if not words[1] in query_dictionary:
                        query_dictionary[words[1]] = True
          blastoutfh.close()
       except:
          eprintf("\nERROR : while reading  B/LAST output file " + blastoutputfile + " to parse " +\
                  "        : make sure B/LAST ing was done for the particular database")

          if errorlogger:
             errorlogger.write("\nERROR : while reading  B/LAST output file %s to parse\n" %(blastoutputfile))
             errorlogger.write("      : make sure B/LAST ing was done for the particular database\n")
          pass 

def create_dictionary(databasemapfile, annot_map, query_dictionary, errorlogger= None):
       if not query_dictionary:
          print "WARNING : empty query dictionary in parse B/LAST"

          if errorlogger:
            errologger.write("WARNING : empty query dictionary in parse B/LAST\n")
          return 

       seq_beg_pattern = re.compile(">")
       try:
            dbmapfile = open( databasemapfile,'r')
       except:
            if errorlogger:
               errologger.write("PARSE_BLAST\tERROR\tCannot open database map file %s\t Please check the file manuallyT\n" %(databasemapfile) )
            exit_process("ERROR: Cannot open database map file %s\n" %(databasemapfile))

       for line in dbmapfile:
          if seq_beg_pattern.search(line):
              words = line.rstrip().split()
              name = words[0].replace('>','',1)
              if not name in query_dictionary: 
                 continue
              words.pop(0)
              if len(words)==0:
                 annotation = 'hypothetical protein'
              else:
                 annotation = ' '.join(words)

              annot_map[name] = annotation
       dbmapfile.close()

       if len(annot_map)==0:
          if errorlogger:
             errorlogger.write( "PARSE_BLAST\tERROR\tFile "+databasemapfile+ " seems to be empty!\tCreate datbasemap file\n") 
             errorlogger.write( "Try re-running after deleting file : %s\n" %(databasemapfile)) 
          exit_process( "no anntations in file :" + databasemapfile)
        


class BlastOutputParser(object):
    commentPATTERN = re.compile(r'^#')
    commentLAST_VERSION_PATTERN = re.compile(r'^#.*LAST[\s]+version[\s]+\d+')

    def create_refBitScores(self):
       refscorefile = open(self.refscore_file,'r')
       for line in refscorefile:
           words =[ x.strip()  for x in  line.split('\t') ]
           if len(words) == 2:
              orfid = ShortenORFId(words[0])
              try:
                self.refBitScores[orfid]= int((self.Lambda*float(words[1]) -  self.lnk )/self.ln2)
              except:
                self.refBitScores[orfid]= int(1)
       refscorefile.close()


    def __init__(self, dbname,  blastoutput, database_mapfile, refscore_file, opts, errorlogger =None):
        self.Size = 10000
        self.dbname = dbname
        self.ln2 = 0.69314718055994530941
        self.lnk = math.log(opts.k)
        self.Lambda = opts.Lambda
        self.blastoutput = blastoutput
        self.database_mapfile =database_mapfile
        self.refscore_file = refscore_file
        self.annot_map = {} 
        self.i=0
        self.opts = opts
        self.hits_counts = {}
        self.data = {}
        self.refscores = {}
        self.refBitScores = {}
        self.needToPermute = False;

        self.MAX_READ_ERRORS_ALLOWED = 10
        self.ERROR_COUNT = 0
        self.STEP_NAME = 'PARSE_BLAST'
        self.error_and_warning_logger = errorlogger 


        #print "trying to open blastoutput file " + blastoutput
        query_dictionary = {}

        create_query_dictionary(self.blastoutput, query_dictionary, self.opts.algorithm, errorlogger =  errorlogger) 
        try:
            self.blastoutputfile = open(self.blastoutput,'r')
        except:
            eprintf("\nERROR : cannot open B/LAST output file " + blastoutput + " to parse "+\
                      "      : make sure \"B/LAST\"ing was done for the particular database" )

            if self.error_and_warning_logger:
               self.error_and_warning_logger.write("ERROR : cannot open B/LAST output file %s %s to parse \n" +\
                                             "      : make sure \"B/LAST\"ing was done for "+\
                                             "the particular database" %(blastoutput) )
            exit_process( "Cannot open B/LAST output file " + blastoutput )

        try:
            self.create_refBitScores()
        except:
            print traceback.print_exc(10)
            exit_process( "Error while reading from  B/LAST refscore file " + self.refscore_file )
        try:
           create_dictionary(database_mapfile, self.annot_map, query_dictionary)
           query_dictionary = {}
        except AttributeError:
           eprintf("Cannot read the map file for database : %s\n" % (dbname))
           if errorlogger!= None:
              errorlogger.write("PARSE_BLAST\tERROR\tCannot read the map file %s for database : %s\tDelete the formatted files for the database in the \"formatted\" folder\n" %(database_mapfile, dbname))

           exit_process("Cannot read the map file for database  " + dbname)


  
    def setMaxErrorsLimit(self, max):
       self.MAX_READ_ERRORS_ALLOWED = max

    def setErrorAndWarningLogger(self, logger):
       self.error_and_warning_logger = logger

    def setSTEP_NAME(self, step_name):
        self.STEP_NAME  = step_name

    def incErrorCount(self):
        self.ERROR_COUNT += 1

    def maxErrorsReached(self):
          return (self.ERROR_COUNT > self.MAX_READ_ERRORS_ALLOWED)

    def __iter__(self):
        return self
 
    def permuteForLAST(self, words):
        try :
           temp = copy(words)
           words[0] = temp[6] # query
           words[1] = temp[1] # target
           words[2] = 100.0 # percent id
           words[3] = temp[3]  #aln length
           words[6] = temp[2]
           words[7] = int(temp[2]) + int(temp[3]) - 1
           words[10] = 0.0   # evalue
           words[11] = temp[0]
        except:
           eprintf("ERROR : Invalid B/LAST output file %s \n" % (self.blastoutput))
           if self.error_and_warning_logger:   
               self.error_and_warning_logger.write("ERROR : Invalid B/LAST output file" %(self.blastoutput))
           exit_process( "ERROR : Invalid B/LAST output file %s " % (self.blastoutput))

    def refillBuffer(self):
        i = 0
        self.lines = []
        line = True # self.blastoutputfile.readline()
        while line and i < self.Size:
          line=self.blastoutputfile.readline()
          if self.commentPATTERN.match(line):
             if self.commentLAST_VERSION_PATTERN.match(line) ==False:
                self.needToPermute = True
             continue
          self.lines.append(line)
          if not line:
            break
          i += 1
        self.size = len(self.lines)
       
    def next(self):
        if self.i % self.Size ==0:
           self.refillBuffer()

        if  self.i % self.Size < self.size:
           words = [ x.strip()  for x in self.lines[self.i % self.Size].rstrip().split('\t')]

           if len(words) != 12:
               self.i = self.i + 1
               return None
           
           '''shorten the ORF id'''
           words[0] = ShortenORFId(words[0])
           #if  self.opts.algorithm =='LAST':
           if  self.needToPermute:
                self.permuteForLAST(words)

           if not words[0] in self.hits_counts:
              self.hits_counts[words[0]] = 0

           if self.hits_counts[words[0]] >= self.opts.limit:
              self.i = self.i + 1
              return None 

           if len(words) != 12 or not self.isWithinCutoffs(words, self.data, self.opts, self.annot_map, self.refBitScores):
             self.i = self.i + 1
             return None 

           self.hits_counts[words[0]] += 1
           self.i = self.i + 1
              
           try:
              return self.data
           except:
              return None
        else:
           self.blastoutputfile.close()
           raise StopIteration()
              
    def isWithinCutoffs(self, words, data, cutoffs, annot_map, refbitscores):

        try: 
           orfid  = ShortORFId(words[0])
        except:
           orfid  = words[0]

        data['query'] = orfid
    
        try:
           data['target'] = words[1]
        except:
           data['target'] = 0
    
        try:
           data['q_length'] = int(words[7]) - int(words[6]) + 1
        except:
           data['q_length'] = 0
    
        try:
           data['bitscore'] = float(words[11])
        except:
           data['bitscore'] = 0
    
        try:
           data['bsr'] = float(words[11])/refbitscores[orfid]
        except:
           #print "words 0 " + str(refscores[words[0]])
           #print "words 11 " + str( words[11])
           data['bsr'] = 0
    
        try:
           data['expect'] = float(words[10])
        except:
           data['expect'] = 0
    
        try:
           data['aln_length'] = float(words[3])
        except:
           data['aln_length'] = 0
    
        try:
           data['identity'] = float(words[2])
        except:
           data['identity'] = 0
    
        try:
           data['product'] = annot_map[words[1]]
        except:
           eprintf("Sequence with name \"" + words[1] + "\" is not present in map file\n")
           if self.error_and_warning_logger:   
              self.error_and_warning_logger.write("Sequence with name %s is not present in map file " %(words[1] ))
           self.incErrorCount()
           if self.maxErrorsReached():
               if self.error_and_warning_logger:   
                  self.error_and_warning_logger.write("Number of sequence absent in map file %s exceeds %d" %(self.blastoutput, self.ERROR_COUNT ))
               exit_process("Number of sequence absent in map file %s exceeds %d" %(self.blastoutput, self.ERROR_COUNT ))
           data['product'] = 'hypothetical protein'
    
        try:
           m = re.search(r'(\d+[.]\d+[.]\d+[.]\d+)', data['product'])
           if m != None:
             data['ec'] = m.group(0)
           else:
             data['ec'] = ''
        except:
            data['ec'] = ''
    
        if cutoffs.taxonomy:
           try:
              m = re.search(r'\[([^\[]+)\]', data['product'])
              if m != None:
                data['taxonomy'] = m.group(1)
              else:
                data['taxonomy'] = ''
           except:
                data['taxonomy'] = ''
    
        
        if cutoffs.remove_taxonomy:
           try:
              data['product'] = re.sub(r'\[([^\[]+)\]','', data['product'])
           except:
              data['product'] = ''
    
        if cutoffs.remove_ec:
           try:
              data['product'] = re.sub(r'\([Ee][Ce][:]\d+[.]\d+[.]\d+[.]\d+\)', '', data['product'])
              data['product'] = re.sub(r'\[[Ee][Ce][:]\d+[.]\d+[.]\d+[.]\d+\]', '', data['product'])
              data['product'] = re.sub(r'\[[Ee][Ce][:]\d+[.]\d+[.]\d+[.-]\]', '', data['product'])
              data['product'] = re.sub(r'\[[Ee][Ce][:]\d+[.]\d+[.-.-]\]', '', data['product'])
              data['product'] = re.sub(r'\[[Ee][Ce][:]\d+[.-.-.-]\]', '', data['product'])
           except:
              data['product'] = ''
    

        if data['q_length'] < cutoffs.min_length:
           return False
    
        if data['bitscore'] < cutoffs.min_score:
           return False
    
        if data['expect'] > cutoffs.max_evalue:
           return False
    
        if data['identity'] < cutoffs.min_identity:
           return False
    
        if data['bsr'] < cutoffs.min_bsr:
           return False
    
    #min_length'
    #'min_score'
    #'max_evalue'
    # 'min_identity'
    #'limit'
    #'max_length'
    #'min_query_coverage'
    #'max_gaps'
    #min_bsr'
    
        return True



def read_gene_list(gene_list):
    inputfile = open(gene_list, 'r') 

    list = {}
    for line in inputfile:
        list[line.strip()]  = True

    inputfile.close()
    return list.keys()

rePATT1 = re.compile(r'/')
rePATT2 = re.compile(r'|')
rePATT3 = re.compile(r' ')

# check if the string has one of the genes
def find_gene_name(string,  gene_list, gene_dict):

     fields = [ string.strip().lower()  ]
     _field_dict = {}

     if rePATT1.search(string):
        _field_dic = {}
        for field in fields:
           _fields = [ x.strip() for x in field.split('/') if len(x.strip()) ]
           for _field in _fields:
               _field_dict[_field] = True
        fields = _field_dict.keys()      


     if rePATT2.search(string):
        _field_dic = {}
        for field in fields:
           _fields = [  x.strip() for x in field.split('|') if len(x.strip())  ]
           for _field in _fields:
               _field_dict[_field] = True
        fields = _field_dict.keys()      


     if rePATT3.search(string):
        _field_dic = {}
        for field in fields:
           _fields = [  x.strip() for x in field.split(' ') if len(x.strip())  ]
           for _field in _fields:
               _field_dict[_field] = True
        fields = _field_dict.keys()      

     
     for word in fields:
         if word in gene_dict:
           return word

     return None
        

# check if the hmm hit is in the list
def find_hmm_name(string,  gene_list, gene_dict):

     if string in gene_dict:
         return string

     return None
        

# compute the refscores
def process_input(input, output, input_type , gene_list, append,  errorlogger = None):
    commentPATT = re.compile(r'^#')
    count = 0

    mode = 'w'
    if append:
       mode = 'a'

    gene_list = read_gene_list(gene_list)
    gene_dict = {}

    for gene in gene_list:
       gene_dict[gene.lower()] = gene # re.compile(r'[\/\s]' + gene + '[\/\s]') 

    if input_type=='LAST2':
      q = 0
      t = 9

    if input_type=='LAST1':
      q = 0
      t = 1

    if input_type=='HMM':
      q = 2
      t = 0

    try:
        inputfile = open(input, 'r') 
        outputfile = open(output, mode) 
    except:
        if errorlogger:
           errorlogger.write("PARSE_BLAST\tERROR\tCannot open temp file %s to sort\tfor reference db\n" %(soutput_blastoutput_parsed_tmp, dbname))
        exit_process("PARSE_BLAST\tERROR\tCannot open temp file %s to sort\tfor reference db\n" %(soutput_blastoutput_parsed_tmp, dbname))


    for line in inputfile:
        result = commentPATT.search(line)
        if result:
           continue

        fields = [ x.strip() for x in line.split('\t') ]
        if len(fields) < 3:
           continue

        orfid = fields[q]

        #if input_type=='LAST1' or input_type=='LAST2':
        target = find_gene_name(fields[t], gene_list, gene_dict)

        if target==None:
           continue

        fprintf(outputfile, "%s\t%s\n",orfid, gene_dict[target]);


    outputfile.close()
    inputfile.close()
#    rename(output_blastoutput_parsed_tmp, output_blastoutput_parsed)

    return count

# the main function
def main(argv, errorlogger = None, runstatslogger = None): 
    global parser
    (opts, args) = parser.parse_args(argv)
    if not check_arguments(opts, args):
       print usage
       sys.exit(0)
    
    if errorlogger:
      errorlogger.write("#STEP\tPARSE_BLAST\n")

    unique_count = process_input(opts.input, opts.output, opts.input_type , opts.gene_list, opts.append,  errorlogger = errorlogger)

    if runstatslogger:  
      runstatslogger.write("%s\tTotal Protein Annotations %s (%s)\t%s\n" %( str(priority), dbname, opts.algorithm, str(count)))  
      runstatslogger.write("%s\tNumber of ORFs with hits in %s (%s)\t%s\n" %( str(priority1), dbname, opts.algorithm, str(unique_count)))  

def MetaPathways_parse_blast(argv, errorlogger = None, runstatslogger = None):       
    createParser()
    main(argv, errorlogger = errorlogger, runstatslogger = runstatslogger)
    return (0,'')

# the main function of metapaths
if __name__ == "__main__":
    createParser()
    main(sys.argv[1:])

