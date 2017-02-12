#!/usr/bin/python
# File created on Nov 27 Jan 2012
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
   

    import os, re, traceback
    from optparse import OptionParser, OptionGroup
    from glob import glob
    from libs.python_modules.utils.metapathways_utils  import parse_command_line_parameters, fprintf, printf, eprintf
    from libs.python_modules.utils.metapathways_utils  import strip_taxonomy, ShortenORFId, ShortenContigId, ContigID, ShortenrRNAId
    from libs.python_modules.utils.sysutil import getstatusoutput
    from libs.python_modules.utils.utils  import doesFileExist, createDummyFile
    from libs.python_modules.utils.sysutil import pathDelim
    from libs.python_modules.parsers.blast  import  BlastOutputTsvParser, getParsedBlastFileNames, getrRNAStatFileNames
except:
    print """ Could not load some user defined  module functions"""
    print """ Make sure your typed 'source MetaPathwaysrc'"""
    print """ """
    sys.exit(3)


usage=  sys.argv[0] + """ dbname2 -b parsed_blastout_for_database2 -w weight_for_database2 ] [ --rRNA_16S  16SrRNA-stats-table ] [ --tRNA tRNA-stats-table ] [ --compact_output ]"""
parser = None

PATHDELIM =  str(pathDelim())
def createParser():
     global parser
     epilog = """Reads the parsed BLAST/LAST files to create files that provides gene count
               and then convert to biom format """

     epilog = re.sub(r'\s+',' ', epilog)
     parser = OptionParser(usage = usage, epilog = epilog)

     parser.add_option("-a", "--algorithm", dest="algorithm", default="BLAST", help="algorithm BLAST or LAST [default BLAST]" )


     parser.add_option("-B", "--blastdir", dest="blastdir",  default=None,
                       help='the blast dir where all the BLAST outputs are located')
     
     parser.add_option("-O", "--outputdir", dest="outputdir",  default=None,
                       help='the output for the biom files')
     
     parser.add_option("-s", "--samplename", dest="sample_name",  default=None,
                       help='the sample name')
     
     parser.add_option( "-R", "--rRNAdir", dest="rRNAdir",  default=None, 
                       help='the folder where the rRNA stats are [OPTIONAL]')
     
     parser.add_option( "--readcounts", dest="readcounts",  default=None, 
                       help='the file with the orfwise read counts')
     
     parser.add_option( "--readrpkgs", dest="readrpkgs",  default=None, 
                       help='the file with the orfwise rpkgreads')
     
     cutoffs_group =  OptionGroup(parser, 'Cuttoff Related Options')
     
     cutoffs_group.add_option("--min_score", dest="min_score", type='float', default=20,
                       help='the minimum bit score cutoff [default = 20 ] ')
     
     cutoffs_group.add_option("--max_evalue", dest="max_evalue", type='float', default=1e-6,
                       help='the maximum E-value cutoff [ default = 1e-6 ] ')

     cutoffs_group.add_option("--min_length", dest="min_length", type='float', default=30,
                       help='the minimum length of query cutoff [default = 30 ] ')

     cutoffs_group.add_option("--min_identity", dest="min_identity", type='float', default=20,
                       help='the minimum identity of query cutoff [default 30 ] ')

     cutoffs_group.add_option("--limit", dest="limit", type='float', default=1,
                       help='max number of hits per query cutoff [default = 5 ] ')
     
     parser.add_option_group(cutoffs_group)
     
     


def check_arguments(opts, args):

    if opts.blastdir == None:
         eprintf("The blast_results folder must be specified\n")  
         return False

    if opts.sample_name == None:
         eprintf("There should be at least one sample name\n")  
         return False


    return True

def  write_16S_tRNA_gene_info(rRNA_dictionary, outputgff_file, tag):
      fields = [  'source', 'feature', 'start', 'end', 'score', 'strand', 'frame' ]
      for rRNA in rRNA_dictionary:
          
          output_line= rRNA_dictionary[rRNA]['id']
          for field in fields:
             output_line += "\t"+ str(rRNA_dictionary[rRNA][field])

          attributes = "ID="+rRNA_dictionary[rRNA]['seqname'] + tag
          attributes += ";" + "locus_tag="+rRNA_dictionary[rRNA]['seqname'] + tag
          attributes += ";" + "orf_length=" + str(rRNA_dictionary[rRNA]['orf_length'])
          attributes += ";" + "contig_length=" + str(rRNA_dictionary[rRNA]['contig_length'])
          attributes += ";" + "ec="
          attributes += ";" + "product="+rRNA_dictionary[rRNA]['product']
          output_line += '\t' + attributes
          fprintf(outputgff_file, "%s\n", output_line);


def process_rRNA_16S_stats(dbname, rRNA_16S_file, orf_read_rpkgs, opts, shortenorfid=False):
     print "Processing rRNA database : ", dbname
     counter_rRNA={}
     if not doesFileExist(rRNA_16S_file):
         return
     try:
        taxonomy_file = open(rRNA_16S_file, 'r')
     except IOError:
        eprintf("Cannot read file %s!\n", rRNA_16S_file)
        exit_process()

     tax_lines = taxonomy_file.readlines()
     similarity_pattern = re.compile("similarity")
     evalue_pattern = re.compile("evalue")
     bitscore_pattern = re.compile("bitscore")
     taxonomy_pattern = re.compile("taxonomy")
     headerScanned = False

     seencounter = {}
     for line in tax_lines:
         if headerScanned == False:
            if similarity_pattern.search(line) and evalue_pattern.search(line) and bitscore_pattern.search(line) and  taxonomy_pattern.search(line):
                headerScanned = True
            continue
         fields = [ x.strip() for x in line.split('\t') ]
         if len(fields) >=6:
           if not fields[0] in seencounter:
              seencounter[fields[0]]=0
           else:
              seencounter[fields[0]] +=1

           _name = fields[0] + "_" + str(seencounter[fields[0]] ) + "_rRNA"


           if not fields[6] in counter_rRNA:
               counter_rRNA[fields[6]] = 0.0

           name = ShortenrRNAId(_name)
           if  name in orf_read_rpkgs:
               counter_rRNA[fields[6]] += orf_read_rpkgs[name]
           else:
               counter_rRNA[fields[6]] += 0

     taxonomy_file.close()
     with open(opts.outputdir + PATHDELIM + opts.sample_name +  "." + dbname + ".read_rpkgs.txt", 'w') as fout:
       fprintf(fout, "# Gene\tCounts\n");
       for name in counter_rRNA:
          fprintf(fout, "%s\t%0.2f\n", name, counter_rRNA[name])

     return len(counter_rRNA)
     

def get_sequence_number(line):
      seqnamePATT = re.compile(r'[\S]+_(\d+)$')
      result = seqnamePATT.search(line.strip())
      if result:
         return result.group(1)
      return  line


def getFunctionName(dbname, data):
     COG_NAME = re.compile(r'^cog', re.IGNORECASE)
     UNIPROT_NAME = re.compile(r'^uniprot.*sprot', re.IGNORECASE)

     COG_PATT = re.compile(r'(COG\d\d\d\d)')
     UNIPROT_PATT = re.compile("[ts][rp]\|([A-Z0-9]+)\|")

     if COG_NAME.search(dbname):
        res = COG_PATT.search(data['product']) 
        if res:
            return res.group(1)
        else:
            return data['target']

     if UNIPROT_NAME.search(dbname):
        res = UNIPROT_PATT.search(data['target']) 
        if res:
            return res.group(1)
        else:
            return data['target']

     return data['target']


def isWithinCutoffs(data, cutoffs):
    if data['q_length'] < cutoffs.min_length:
       return False

    if data['bitscore'] < cutoffs.min_score:
       return False

    if data['expect'] > cutoffs.max_evalue:
       return False

    if data['identity'] < cutoffs.min_identity:
       return False


    return True



#read counts
def  read_orf_read_counts(orf_read_counts, readcounts_file):
    if readcounts_file==None: 
       return

    comment_PATT = re.compile(r'^#')
    with open(readcounts_file, 'r') as finput:
      for line in finput:
         if not comment_PATT.search(line):
            fields = [ x.strip() for x in  line.split('\t') ]
             
            orf_read_counts[fields[0]] = float(fields[1])
            print  orf_read_counts[fields[0]], float(fields[1])

# compute the refscores
def process_parsed_blastoutput(dbname,  blastoutput, opts, orf_read_counts):
    blastparser =  BlastOutputTsvParser(dbname, blastoutput, shortenorfid=False)

    hit_counts={}
    for data in blastparser:
        #if count%10000==0:
        if isWithinCutoffs(data, opts) :

            target = getFunctionName(dbname, data) 

            if not target in hit_counts:
               hit_counts[target] = 0

            if data['query'] in orf_read_counts:
               hit_counts[target] += orf_read_counts[data['query']]
            else:
               #print 'query', data['query']
               hit_counts[target] += 1
            #print data
    #for name in hit_counts:
    #   print name, hit_counts[name]

    filename = opts.outputdir + PATHDELIM + opts.sample_name + "." + dbname 
    filename_txt  = filename + ".read_counts.txt"
    filename_biom  = filename + ".read_counts.biom"

    with open(filename_txt, 'w') as fout:
       fprintf(fout, "# Gene\tCounts\n");
       for name in hit_counts:
          fprintf(fout, "%s\t%d\n", name, hit_counts[name])

 
    runBIOMCommand(filename_txt, filename_biom, biomExec="biom")
    return len(hit_counts)


def runBIOMCommand(infile, outfile, biomExec="biom"):
    commands =  [biomExec,  " convert", "-i", infile, "-o", outfile,  "--table-type=\"Gene table\"", "--to-hdf5"]
    result = getstatusoutput(' '.join(commands))
    
    return result[0]


#def getBlastFileNames(opts) :

# the main function
def main(argv, errorlogger =None, runstatslogger = None): 
    global parser
    (opts, args) = parser.parse_args(argv)

    if not check_arguments(opts, args):
       print usage
       sys.exit(0)

    # read the ORFwise counts
    orf_read_counts={}
    if os.path.exists(opts.readcounts):
       read_orf_read_counts(orf_read_counts, opts.readcounts)

    # Functional databases
    database_names, input_blastouts = getParsedBlastFileNames(opts.blastdir, opts.sample_name, opts.algorithm) 
    priority = 6000
    count_annotations = {}
    for dbname, blastoutput, in zip(database_names, input_blastouts): 
        count = process_parsed_blastoutput(dbname, blastoutput, opts, orf_read_counts)
        if runstatslogger!=None:
           runstatslogger.write("%s\tProtein Annotations from %s\t%s\n" %( str(priority), dbname, str(count)))

    # rRNA databases
    orf_read_rpkgs={}
    if os.path.exists(opts.readrpkgs):
      read_orf_read_counts(orf_read_rpkgs, opts.readrpkgs)

    rRNA_16S_dictionary = {}
    database_names, input_blastouts = getrRNAStatFileNames(opts.rRNAdir, opts.sample_name, opts.algorithm) 
    for dbname, rRNAStatsFile in zip(database_names, input_blastouts): 
       process_rRNA_16S_stats(dbname, rRNAStatsFile, orf_read_rpkgs, opts,  shortenorfid=False)
        
    createDummyFile(opts.outputdir + PATHDELIM + opts.sample_name + ".dummy.txt")

    #create the annotations from he results

def MetaPathways_annotate_fast(argv, errorlogger = None, runstatslogger = None):       
    createParser()
    errorlogger.write("#STEP\tANNOTATE_ORFS\n")
    main(argv, errorlogger = errorlogger, runstatslogger = runstatslogger)
    return (0,'')

# the main function of metapaths
if __name__ == "__main__":
    createParser()
    main(sys.argv[1:])

