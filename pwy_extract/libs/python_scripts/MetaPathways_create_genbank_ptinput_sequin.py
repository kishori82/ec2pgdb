#!/usr/bin/python

__author__ = "Kishori M Konwar"
__copyright__ = "Copyright 2013, MetaPathways"
__credits__ = ["r"]
__version__ = "1.0"
__maintainer__ = "Kishori M Konwar"
__status__ = "Release"

try:
    import optparse
    import csv
    from os import makedirs, path, listdir, remove, rename
    import shutil
    import traceback
    import sys
    import logging.handlers
    import re
    from glob import glob
    from libs.python_modules.utils.utils import *
    from libs.python_modules.utils.metapathways_utils import ShortenORFId,ShortentRNAId, ShortenrRNAId, ContigID
    from libs.python_modules.utils.sysutil import pathDelim, genbankDate, getstatusoutput
    from libs.python_modules.parsers.parse  import parse_parameter_file
except:
    print """ Could not load some user defined  module functions"""
    print """ Make sure your typed 'source MetaPathwaysrc'"""
    print """ """
    sys.exit(3)

PATHDELIM = pathDelim()

def fprintf(file, fmt, *args):
    file.write(fmt % args)

def printf(fmt, *args):
    sys.stdout.write(fmt % args)

def files_exist( files ):
     for file in files:
        if not path.exists(file):
           print 'Could not read File ' +  file
           print 'Please make sure these sequences are in the \"blastDB\" folder'
           sys.exit(3)
           return False
     return True


def removeDir(origFolderName):
    folderName = origFolderName + PATHDELIM + '*'
    files = glob(folderName)
    for  f in files:
       remove(f)
    if path.exists(origFolderName):
       shutil.rmtree(origFolderName)
    

def insert_attribute(attributes, attribStr):
     rawfields = re.split('=', attribStr)
     if len(rawfields) == 2:
       attributes[rawfields[0].strip().lower()] = rawfields[1].strip()



def split_attributes(str, attributes):        
     rawattributes = re.split(';', str)
     for attribStr in rawattributes:
        insert_attribute(attributes, attribStr) 

     return attributes
     
   
def insert_orf_into_dict(line, contig_dict):
     rawfields = re.split('\t', line)
     fields = [] 
     for field in rawfields:
        fields.append(field.strip());
     
     if( len(fields) != 9):
       return

     attributes = {}
     try:
         attributes['seqname'] =  fields[0]   # this is a bit of a  duplication  
         attributes['source'] =  fields[1] 
         attributes['feature'] =  fields[2] 
         attributes['start'] =  int(fields[3])
         attributes['end'] =  int(fields[4])
     except:
         print line
         sys.exit(0)

     try:
        attributes['score'] =  float(fields[5])
     except:
        attributes['score'] =  fields[5]

     attributes['strand'] =  fields[6] 
     attributes['frame'] =  fields[7] 
     split_attributes(fields[8], attributes)
    
     if not fields[0] in contig_dict :
       contig_dict[fields[0]] = []

     #print attributes
     contig_dict[fields[0]].append(attributes)


def get_sequence_name(line): 
     fields = re.split(' ', line)
     name = re.sub('>','',fields[0])
     #print name
     return name
     

def get_sequence_number(line): 
     fields = re.split(' ', line)
     name = re.sub('>','',fields[0])
     seqnamePATT = re.compile(r'[\S]+_(\d+)$')
     result = seqnamePATT.search(line.strip())
     return result.group(1)

     #print name


note = """GFF File Format
Fields

Fields must be tab-separated. Also, all but the final field in each feature line must contain a value; "empty" columns should be denoted with a '.'

    seqname - name of the chromosome or scaffold; chromosome names can be given with or without the 'chr' prefix.
    source - name of the program that generated this feature, or the data source (database or project name)
    feature - feature type name, e.g. Gene, Variation, Similarity
    start - Start position of the feature, with sequence numbering starting at 1.
    end - End position of the feature, with sequence numbering starting at 1.
    score - A floating point value.
    strand - defined as + (forward) or - (reverse).
    frame - One of '0', '1' or '2'. '0' indicates that the first base of the feature is the first base of a codon, '1' that the second base is the first base of a codon, and so on..
    attribute - A semicolon-separated list of tag-value pairs, providing additional information about each feature.
"""

#def get_date():


def get_sample_name(gff_file_name):
     sample_name= re.sub('.annotated.gff', '', gff_file_name)
     sample_name= re.sub('.annot.gff', '', gff_file_name) # Niels: somewhere we changed the file name?
     sample_name= re.sub(r'.*[/\\]', '', sample_name)
     return sample_name


def process_gff_file(gff_file_name, output_filenames, nucleotide_seq_dict, protein_seq_dict, input_filenames, orf_to_taxonid = {},  compact_output=True):
     #print output_filenames
     try:
        gfffile = open(gff_file_name, 'r')
     except IOError:
        print "Cannot read file " + gff_file_name + " !"

     sample_name=get_sample_name(gff_file_name)

     gff_lines = gfffile.readlines()
     gff_beg_pattern = re.compile("^#")
     gfffile.close()
     
     contig_dict={} 
     count = 0
     for line in gff_lines:
        line = line.strip() 
        if gff_beg_pattern.search(line):
          continue
        """  Do not add tRNA """
        insert_orf_into_dict(line, contig_dict)

     if "gbk" in output_filenames:
       write_gbk_file(output_filenames['gbk'], contig_dict, sample_name, nucleotide_seq_dict, protein_seq_dict)

     if "ptinput" in output_filenames:
       write_ptinput_files(output_filenames['ptinput'], contig_dict, sample_name, nucleotide_seq_dict, protein_seq_dict, compact_output, orf_to_taxonid=orf_to_taxonid)

# this function creates the pathway tools input files
def  write_ptinput_files(output_dir_name, contig_dict, sample_name, nucleotide_seq_dict, protein_seq_dict, compact_output, orf_to_taxonid={}):

     try:
        #print output_dir_name
        removeDir(output_dir_name)
        #print output_dir_name
        makedirs(output_dir_name)
        genetic_elements_file = open(output_dir_name + "/.tmp.genetic-elements.dat", 'w')

        reducedpffile = open(output_dir_name + "/tmp.reduced.txt", 'w')
     except:
        print "cannot create the pathway tools files"
        print "perhaps there is already a folder " + output_dir_name
        traceback.print_exc(file=sys.stdout)

     count =0 
     outputStr=""

     # iterate over every contig sequence
     first_hits = {}
     if compact_output:
        prefix = 'O_'
     else:
        prefix = sample_name + '_'

     countError = 0
     for key in contig_dict:
        first = True
        if count %10000 == 0:
           #print "count " + str(count)
           #outputfile.write(outputStr)
           outputStr=""
        count+=1

        for attrib in contig_dict[key]:     

           id  =  attrib['id']

           shortid=""
           compactid = ""

           if attrib['feature']=='CDS':
              shortid  =  prefix + ShortenORFId(attrib['id'])
              compactid =  ShortenORFId(attrib['id'])

           if attrib['feature']=='rRNA':
              shortid  =  prefix + ShortenrRNAId(attrib['id'])
              compactid =  ShortenrRNAId(attrib['id'])

           if attrib['feature']=='tRNA':
              shortid  =  prefix + ShortentRNAId(attrib['id'])
              compactid =  ShortentRNAId(attrib['id'])


           try:
              protein_seq = protein_seq_dict[id]
           except:
              protein_seq = ""
           try:   
              if attrib['product']=='hypothetical protein':
                 continue
           except:
              print attrib
              sys.exit(0)

            
           if attrib['product']  in first_hits:
               if attrib['ec'] :
                 if attrib['ec'] in first_hits[attrib['product']]:
                     fprintf(reducedpffile,"%s\t%s\n", shortid, first_hits[attrib['product']]['n'])

                    # to  remove redundancy add "continue "
                    # continue
                 else:    
                     first_hits[attrib['product']]['ec'] =attrib['ec'] 
                     first_hits[attrib['product']]['n'] =shortid 
               else:
                 fprintf(reducedpffile,"%s\t%s\n", shortid, first_hits[attrib['product']]['n'])
                 # to  remove redundancy add "continue "
                 #continue
           else: 
                first_hits[attrib['product']] = {}
                first_hits[attrib['product']]['n'] =shortid
                first_hits[attrib['product']]['ec'] =attrib['ec']

           if compactid in orf_to_taxonid: 
               attrib['taxon'] = orf_to_taxonid[compactid]

           write_to_pf_file(output_dir_name, shortid, attrib, compact_output=compact_output)

           # append to the gen elements file
           if compact_output==False:
              append_genetic_elements_file(genetic_elements_file, output_dir_name, shortid)
        #endfor


        #write the sequence now only once per contig
        try:
           contig_seq =  nucleotide_seq_dict[key]
        except:
           #print nucleotide_seq_dict.keys()[0]
           if countError < 10:
              printf("ERROR: Contig %s missing file in \"preprocessed\" folder for sample\n", key)
              countError += 1
              if countError == 10:
                printf("...................................................................\n")
           continue

        fastaStr=wrap("",0,62, contig_seq)

           #write_ptools_input_files(genetic_elements_file, output_dir_name, shortid, fastaStr)
        if compact_output==False:
           write_input_sequence_file(output_dir_name, shortid, fastaStr)
     #endif 

     if compact_output==True:
        add_genetic_elements_file(genetic_elements_file)

     rename(output_dir_name + "/tmp.reduced.txt",output_dir_name + "/reduced.txt")

     # Niels: removing annotated.gff from sample_name
     sample_name = re.sub(".annot.gff", '', sample_name)
     sample_name = re.sub('.*/', '', sample_name)
     sample_name = re.sub(r'[\\].', '', sample_name)
     
     # Niels: trim sample_name to less than 35 characters 
     # as it causes PGDB creation to fail
     if (len(sample_name) > 35):
        sample_name = sample_name[0:35]
     
     
     if not sample_name[0].isalpha() :
        sample_name = 'E' + sample_name

     write_organisms_dat_file(output_dir_name, sample_name)

     genetic_elements_file.close()
     rename(output_dir_name + "/.tmp.genetic-elements.dat", output_dir_name + "/genetic-elements.dat")


pfFile = None

def write_to_pf_file(output_dir_name, shortid, attrib, compact_output):
    global pfFile
    if compact_output:
       if pfFile==None:
          pfFile = open(output_dir_name + "/" + "0.pf", 'w')
    else:
       pfFile = open(output_dir_name + "/" + shortid + ".pf", 'w')

    try: 
       fprintf(pfFile, "ID\t%s\n", shortid)
       fprintf(pfFile, "NAME\t%s\n", shortid)
       fprintf(pfFile, "STARTBASE\t%s\n", attrib['start'])
       fprintf(pfFile, "ENDBASE\t%s\n", attrib['end'])
    except:
       pass

    ec_nos = {}
    try: 
       prod_attributes = create_product_attributes(attrib['product'])

       for function in prod_attributes['FUNCTION']:
          fprintf(pfFile, "FUNCTION\t%s\n", function)
          #printf("FUNCTION\t%s\n", function)
       
       if 'taxon' in attrib:
          fprintf(pfFile, "TAXONOMIC-ANNOT\tTAX-%s\n", attrib['taxon'])

    
       for dblink in prod_attributes['DBLINK']:
          fprintf(pfFile, "DBLINK\t%s:%s\n", dblink[0], dblink[1])
          #printf("DBLINK\t%s:%s\n", dblink[0], dblink[1])

       for ec in prod_attributes['EC']:
          fprintf(pfFile, "EC\t%s\n", ec)
          #printf("EC\t%s\n", ec)


#       if len(prod_attributes)>=5:
#         for i in range(0, len(prod_attributes)):
#            if i==0:
#              fprintf(pfFile, "FUNCTION\t%s\n", prod_attributes[i])
#
#            if i==1:
#              fprintf(pfFile, "DBLINK\tSP:%s\n", prod_attributes[i])
#            #  printf("DBLINK\tSP:%s\n", prod_attributes[0])
#   
#            if i == 2:
#              fprintf(pfFile, "DBLINK\tMetaCyc:%s\n", prod_attributes[i])
#            #  printf("DBLINK\tMetaCyc:%s\n", prod_attributes[1])
#   
#            if i >= 4:
#              if not prod_attributes[i] in ec_nos:
#                 fprintf(pfFile, "EC\t%s\n", prod_attributes[i])
#                 ec_nos[prod_attributes[i]] = True
#       else:
#         fprintf(pfFile, "FUNCTION\t%s\n", attrib['product'])
#            #  printf("EC\t%s\n", prod_attributes[3])
    except:
       fprintf(pfFile, "FUNCTION\t%s \n", 'hypothetical protein')




    if attrib['feature']=='CDS':
       fprintf(pfFile, "PRODUCT-TYPE\tP\n")

    if attrib['feature']=='tRNA':
       fprintf(pfFile, "PRODUCT-TYPE\tTRNA\n")

    if attrib['feature']=='rRNA':
       fprintf(pfFile, "PRODUCT-TYPE\trRNA\n")
    fprintf(pfFile, "//\n")

    if compact_output:
       pass
    else:
       pfFile.close()


def  create_product_attributes(product) :
     COG_PATT = re.compile(r'(COG\d\d\d\d)')
     KEGG_PATT = re.compile(r'(K\d\d\d\d\d)')
     EC_PATTS = [ 
                  re.compile(r'EC[:\s](\d+[.]\d+[.]\d+[.]-)'),\
                  re.compile(r'EC[:\s](\d+[.]\d+[.]\d+[.]\d+)'),\
                  re.compile(r'EC[:\s](\d+[.]\d+[.]-[.]-)'),\
                  re.compile(r'EC[:\s](\d+[.]-[.]-[.]-)'),\
                  re.compile(r'(\d+[.]\d+[.]\d+[.]-)'),\
                  re.compile(r'(\d+[.]\d+[.]\d+[.]\d+)'),\
                  re.compile(r'(\d+[.]\d+[.]-[.]-)'),\
                  re.compile(r'(\d+[.]-[.]-[.]-)')
                ]

     #METACYC_PATT = re.compile(r'#\sUNIPROT\s#\s([A-Z0-9]+)\s#\s([.*])\s#')
# UNIPROT # Q9I1M2 # MetaCyc # 1.2.4.4-RXN
     METACYC_PATTS = [   # order is important 
                         re.compile(r'#\sUNIPROT\s#\s([A-Z0-9]+)\s#\sMetaCyc\s#(\s)#'),
                         re.compile(r'#\sUNIPROT\s#\s([A-Z0-9]+)\s#\sMetaCyc\s#\s(\S*)\s#'),
                         re.compile(r'#\sUNIPROT\s#\s([A-Z0-9]+)\s#\s(\S*)\s#')
                    ]
     ORGANISM_PATT = re.compile(r'#\sOrganism:\s(.*)$')
     FUNCTION_PATT = re.compile(r'#\sFunction:\s([^#]*)')

     STRAY_PATT = re.compile(r'[()#%]')

     products = []
     _products = {}
     _products['DBLINK'] =[]
     _products['FUNCTION'] =[]
     _products['ORGANISM'] =[]
     _products['EC'] =[]
     _product = product 


     seen_ec={}
     res = KEGG_PATT.search(_product) 
     if res:
        for kegg in res.groups( ):
          _products["DBLINK"].append([ "KO", kegg ])
        _product = re.sub(KEGG_PATT,'%',_product)
        

     res = COG_PATT.search(_product) 
     if res:
        for cog in res.groups( ):
          _products["DBLINK"].append([ "COG", cog ])
        _product = re.sub(COG_PATT,'%',_product)
        
     for EC_PATT in EC_PATTS:
        res = EC_PATT.search(_product) 
        if res:
          for ec in res.groups( ):
             if not  ec in seen_ec:
                _products["EC"].append( ec)
                seen_ec[ec]= True 

        _product = re.sub(EC_PATT,'%',_product)
        


     for METACYC_PATT in METACYC_PATTS:
         res = METACYC_PATT.search(_product) 
         if res:
           i=0
           for ec in res.groups():
             if i==0:
               _products["DBLINK"].append([ 'SP',  ec])
             if i==1:
               if ec.strip():
                 _products["DBLINK"].append([ 'MetaCyc',  ec])
             i+=1
           _product = re.sub(METACYC_PATT,'%',_product)
           break
        

     res = ORGANISM_PATT.search(_product) 
     if res:
         for ec in res.groups():
             _products["ORGANISM"].append( ec.strip())
         _product = re.sub(ORGANISM_PATT,'%',_product)
        
     res = FUNCTION_PATT.search(_product) 
     if res:
         for func in res.groups():
             _func  =re.sub(STRAY_PATT,'',func)
             _products["FUNCTION"].append(clean_up_function_text(_func.strip()))
         _product = re.sub(FUNCTION_PATT,'%',_product)
      
     _product = re.sub(STRAY_PATT,'', _product)
     if _product.strip():
          _products["FUNCTION"].append(clean_up_function_text(_product.strip()))


     return _products


def clean_up_function_text(_product):
     _fields = [ x.strip() for x in  _product.split(' ')]
     prev_field=''
     fields = []
     for _field in _fields:
        if _field!=prev_field and _field:
            fields.append(_field)
            prev_field = _field

     return ' '.join(fields)  


def write_input_sequence_file(output_dir_name, shortid, contig_sequence):

    seqfile = open(output_dir_name + "/" + shortid + ".fasta", 'w')
    fprintf(seqfile, ">%s\n%s",shortid, contig_sequence);
    seqfile.close()



def add_genetic_elements_file(genetic_elementsfile):
    fprintf(genetic_elementsfile,"ID\t0\n")
    fprintf(genetic_elementsfile,"NAME\t0\n")
    fprintf(genetic_elementsfile,"TYPE\t:CONTIG\n")
    fprintf(genetic_elementsfile,"ANNOT-FILE\t0.pf\n")
    fprintf(genetic_elementsfile,"//\n")


def append_genetic_elements_file(genetic_elementsfile, output_dir_name, shortid):
    fprintf(genetic_elementsfile,"ID\t%s\n", shortid)
    fprintf(genetic_elementsfile,"NAME\t%s\n", shortid)
    fprintf(genetic_elementsfile,"TYPE\t:CONTIG\n")
    fprintf(genetic_elementsfile,"ANNOT-FILE\t%s.pf\n", shortid)
    fprintf(genetic_elementsfile,"SEQ-FILE\t%s.fasta\n", shortid)
    fprintf(genetic_elementsfile,"//\n")


def write_organisms_dat_file(output_dir_name, sample_name):
    organism_paramsfile = open(output_dir_name + "/organism-params.dat", 'w')
    fprintf(organism_paramsfile,"ID\t%s\n",sample_name)
    fprintf(organism_paramsfile,"STORAGE\tFILE\n")
    fprintf(organism_paramsfile,"NAME\t%s\n",sample_name)
    fprintf(organism_paramsfile,"ABBREV-NAME\t%s\n",sample_name)
    fprintf(organism_paramsfile,"STRAIN\t1\n")
    fprintf(organism_paramsfile,"RANK\t|species|\n")
    fprintf(organism_paramsfile,"NCBI-TAXON-ID\t12908\n")
    organism_paramsfile.close()
 

def get_parameter(config_params, category, field, default = None):
     if config_params == None:
       return default
 
     if category in config_params:
        if field in config_params[category]:
             if config_params[category][field]: 
                return config_params[category][field]
             else:
                return default
        else:
             return default
     return default


#this function creates the genbank file from the gff, protein and nucleotide sequences  
def  write_gbk_file(output_file_name, contig_dict, sample_name, nucleotide_seq_dict, protein_seq_dict):

     date = genbankDate()
     output_file_name_tmp = output_file_name + ".tmp"
     outputfile = open(output_file_name_tmp, 'w')
     #print contig_dict
    
     count =0 
     outputStr=""
     for key in contig_dict:
        first = True
        if count %10000 == 0:
           #print "count " + str(count)
           outputfile.write(outputStr)
           outputStr=""
        count+=1

        for attrib in contig_dict[key]:     
           id  = attrib['id']
           try:
              protein_seq = protein_seq_dict[id]
           except:
              protein_seq = ""
              None
           
           definition = sample_name
           accession = '.'
           version = '.' +spaces(10) + "GI:."
           dblink = sample_name
           keywords = '.'
           source = sample_name
           organism = sample_name
           if first:   
              first = False
              try:
                dna_seq =  nucleotide_seq_dict[key]
                dna_seq_formatted =  format_sequence_origin(dna_seq)
                dna_length = len(dna_seq)
                sourceStr = "1.." + str(dna_length)
              except:
                dna_seq = ""
                dna_seq_formatted =  ""
                dna_length = 0
                sourceStr ="0..0"

              outputStr+=("LOCUS       %-18s  %4d bp   DNA           BCT      %-11s\n" % (key, dna_length,  date))
              outputStr+=(wrap("DEFINITION  ",12,74, definition)+'\n')
              outputStr+=(wrap("ACCESSION   ", 12, 74, accession)+'\n')
              outputStr+=(wrap("VERSION     ", 12, 74, version)+'\n')
              outputStr+=(wrap("DBLINK      ", 12, 74, dblink)+'\n')
              outputStr+=(wrap("KEYWORDS    ", 12, 74,keywords)+'\n')
              outputStr+=(wrap("SOURCE    ", 12, 74, keywords)+'\n')
              outputStr+=(wrap("  ORGANISM  ",12, 74, organism)+'\n')
              outputStr+=(wrap("", 12, 74, "Metagenome")+'\n')
              outputStr+=( wrap("REFERENCE   ",12,74, "1  (bases 1 to XXXXX)")+'\n')
              outputStr+=( wrap("  AUTHORS   ",12,74, "YYYYYY,X.")+'\n')
              outputStr+=( wrap("  CONSRTM   ",12,74, "XXXXX")+'\n')
              outputStr+=( wrap("  TITLE     ",12,74, "XXXXX")+'\n')
              outputStr+=( wrap("  JOURNAL   ",12,74,"XXXXX")+'\n')
              outputStr+=( wrap("   PUBMED   ",12,74,"XXXXX")+'\n')
              outputStr+=( wrap("  REMARK   ",12,74, "XXXXX")+'\n')
              outputStr+=( wrap("COMMENT     ", 12, 74,"PROVISIONAL REFSEQ: This record has not yet been subject to final NCBI review   COMPLETENESS: XXXXX")+'\n')
             
              outputStr+=( wrap("FEATURES ",21,74,"Location/Qualifiers") +'\n')
              outputStr+=( wrap("     source",21,74,sourceStr) +'\n')
              outputStr+=( wrap("",21,74,"/organism=\"" + sourceStr +"\"") +'\n')
              outputStr+=( wrap("",21,74,"/strain=\"1\"")+'\n')
              outputStr+=( wrap("",21,74,"/chromosome=\"1\"") +'\n')
            



           if 'start' in attrib and 'end' in attrib:
               geneLoc = str(attrib['start']) +".." + str(attrib['end'])
           else:
               geneLoc = "0..0"

           if 'strand' in attrib:
              if attrib['strand']=='-':
                 geneLoc='complement' + '(' + geneLoc +')'

           outputStr+=( wrap("     gene",21,74,geneLoc) +'\n')
           if 'locus_tag' in attrib:
               locus_tag = "/locus_tag=" + "\"" + attrib['locus_tag'] + "\""
           else:
               locus_tag = "/locus_tag" + "\"\"" 
           outputStr+=( wrap("",21,74,locus_tag) +'\n')
           outputStr+=( wrap("     CDS",21,74,geneLoc) +'\n')
           if 'product' in attrib:
              product="/product=" + attrib['product']
           else:
              product="/product=\"\""
           outputStr+=( wrap("",21,74,product) +'\n')
           outputStr+=( wrap("",21,74,locus_tag) +'\n')

           codon_start="/codon_start=1"
           translation_table="/transl_table=11"
           outputStr+=( wrap("",21,74,codon_start) +'\n')
           outputStr+=( wrap("",21,74,translation_table) +'\n')
       
           translation= "/translation="+ protein_seq
           outputStr+=( wrap("",21,74,translation) +'\n')

        outputStr+=(wrap("ORIGIN", 21, 74, "")+'\n')
        outputStr+=(dna_seq_formatted +'\n')
        outputStr+=("//\n")

     outputfile.write(outputStr)
     outputfile.close() 
     rename(output_file_name_tmp, output_file_name)


def format_sequence_origin(dna_seq):
    output=""
    Len =  len(dna_seq)
    for i in range(0, Len):    
       if i==0:
          output+= '%9d' % (i+1)
       if i%10==0:
          output+=' '
       if i!=0  and i%60==0:
          output+= '\n%9d ' % (i+1)
       output +=dna_seq[i]
       i+=1  
    return output

 
def spaces(n):
    space=''
    for  i in  range(0, n):
      space+=' '
    return space
       

def wrap(prefix, start, end, string):
    output=''
    prefixLen = len(prefix)
    i = prefixLen
    output+=prefix
    while i< start:
      output += ' '
      i += 1

    for c in string:
       if i > end:
          output+='\n' 
          i = start
          output+=spaces(i) 
       i+=1
       output+=c 
    return output

    
    

def process_sequence_file(sequence_file_name,  seq_dictionary, shortorfid=False):
     try:
        sequencefile = open(sequence_file_name, 'r')
     except IOError:
        print "Cannot read file " + sequence_file_name + " !"

     sequence_lines = sequencefile.readlines()
     sequencefile.close()
     fragments= []
     name=""

#     count = 0
     seq_beg_pattern = re.compile(">(\S+)")
     for line in sequence_lines:
        line = line.strip() 
        res = seq_beg_pattern.search(line)
        if res:
          if len(name) > 0:
             sequence=''.join(fragments)
             seq_dictionary[name]=sequence
             fragments = []
          if shortorfid:
             name=get_sequence_number(line)
          else:
             name=res.group(1)
        else:
          fragments.append(line)

    #add the final sequence
     if len(name) > 0:
         sequence=''.join(fragments)
         seq_dictionary[name]=sequence
     
#        if count > 1000:
#           sys.exit(0)
#        count=count+1
#     fields = re.split('\t', line)
        #print table[str(fields[0].strip())]
     #print blast_file + ' ' + tax_maps + ' ' + database


usage =  sys.argv[0] + """ -g gff_files -n nucleotide_sequences -p protein_sequences [--out-gbk gbkfile --out-ptinput ptinputdir]\n"""

parser = None

def read_taxons_for_orfs(ncbi_taxonomy_tree, taxonomy_table):
    num = 20

    name_to_taxonid = {}
    with open(ncbi_taxonomy_tree, 'r') as f:
        for _line in f:
           fields = [ x.strip() for x in _line.split('\t') ]
           if len(fields) == 2:
              name_to_taxon[fields[0]] = fields[1]


    tax_PATT = re.compile(r'(\d+)')
    orf_to_taxonid = {}
    tax_col = 0
    with open(taxonomy_table, 'r') as f:
        fields = [ x.strip() for x in f.readline().strip().split('\t') ]
        for i in range(0, len(fields)):
           if fields[i]=='taxonomy':
              tax_col =i
        
        for _line in f:
           fields = [ x.strip() for x in _line.split('\t') ]
           if len(fields) > tax_col:
              res = tax_PATT.search(fields[tax_col])
              if res:
                  orf_to_taxonid[fields[0]] = res.group(1)

    return orf_to_taxonid
          

def createParser():
    global parser
    epilog = """This script has three functions : (i) The functional and taxonomic annotations created for the individual ORFs are used to create the inputs required by the Pathway-Tools's Pathologic algorithm to build the ePGDBs. The input consists of 4 files that contains functional annotations and sequences with relevant information, this information is used by Pathologic to create the ePGDBs. (ii) It can create a genbank file for the ORFs and their annotationsequi"""
    epilog = re.sub(r'\s+', ' ', epilog)

    parser = optparse.OptionParser(usage=usage, epilog = epilog)

    # Input options

    input_group = optparse.OptionGroup(parser, 'input options')

    input_group.add_option('-g', '--gff', dest='gff_file',
                           metavar='GFF_FILE', 
                           help='GFF files, with annotations,  to convert to genbank format')

    input_group.add_option('-n', '--nucleotide', dest='nucleotide_sequences',
                           metavar='NUCLEOTIDE_SEQUENCE', 
                           help='Nucleotide sequences')

    input_group.add_option('-p', '--protein', dest='protein_sequences',
                           metavar='PROTEIN_SEQUENCE', 
                           help='Protein sequences')


    input_group.add_option('-o', '--output', dest='output',
                           metavar='OUTPUT', help='Genbank file')

    parser.add_option_group(input_group)

    output_options_group =  optparse.OptionGroup(parser, 'Output Options')

    output_options_group.add_option("--out-gbk", dest="gbk_file", default = None, 
                     help='option to create a genbank file')

    output_options_group.add_option("--ncbi-tree", dest="ncbi_taxonomy_tree",  default=None,
                       help='add the ncbi taxonomy tree ')

    output_options_group.add_option("--taxonomy-table", dest="taxonomy_table",  default=None,
                       help='table with taxonomy')


    output_options_group.add_option("--out-ptinput", dest="ptinput_file",  default=None,
                     help='option and directory  to create ptools input files')

    output_options_group.add_option("--compact-output", dest="compact_output",  default=False, action='store_true',
                     help='option to create compact orfid names')

    parser.add_option_group(output_options_group)


def main(argv, errorlogger = None, runstatslogger = None):
    # Parse options (many!)
    # TODO: Create option groups
    # filtering options


    global parser
    options, args = parser.parse_args(argv)

    if not(options.gff_file or options.nucleotide_sequences or options.protein_sequences or options.output):
      print help
      sys.exit(0)
    

    if not options.gff_file:
       eprintf("ERROR\tGFF file not specified\n")
       errorlogger.printf("ERROR\tGFF file not specified\n")


    if not options.gbk_file and not options.ptinput_file:
       eprintf("ERROR:No genbank or ptools input is specified\n")
       return (0,'')


    if not path.exists(options.gff_file):
        print "gff file does not exist"
        eprintf("ERROR\tGFF file %s  not found\n", options.gff_file)
        errorlogger.printf("ERROR\tGFF file %s  not found\n", options.gff_file)
        sys.exit(0)

    #if not path.exists(options.nucleotide_sequences):
    #    errorlogger.printf("ERROR\tNucloetide sequences file does not exist")
    #    sys.exit(0)

    #if not path.exists(options.protein_sequences):
    #    errorlogger.printf("ERROR\tProtein sequences file does not exist")
    #    sys.exit(0)

    output_files = {}
    input_files = {}
    if  options.gbk_file:
       output_files['gbk'] = options.gbk_file


    if  options.ptinput_file:
       output_files['ptinput'] = options.ptinput_file
    

    nucleotide_seq_dict = {}
    protein_seq_dict = {}

    if options.nucleotide_sequences  and path.exists(options.nucleotide_sequences):
       process_sequence_file(options.nucleotide_sequences, nucleotide_seq_dict) 

    if options.protein_sequences and  path.exists(options.protein_sequences):
       process_sequence_file(options.protein_sequences, protein_seq_dict) 
    
    orf_to_taxonid={}
    if options.ncbi_taxonomy_tree!=None and options.taxonomy_table !=None:
      orf_to_taxonid = read_taxons_for_orfs(options.ncbi_taxonomy_tree, options.taxonomy_table)

    process_gff_file(options.gff_file, output_files, nucleotide_seq_dict, protein_seq_dict, input_files,  orf_to_taxonid=orf_to_taxonid, compact_output=options.compact_output) 
    #print params['bitscore']
 
    sample_name = get_sample_name(options.gff_file)
    createDummyFile(options.ptinput_file + PATHDELIM + sample_name + ".dummy.txt")

def MetaPathways_create_genbank_ptinput_sequin(argv, errorlogger = None, runstatslogger = None):
    createParser()
    main(argv, errorlogger = errorlogger, runstatslogger = runstatslogger)
    return (0,'')
    
if __name__ == '__main__':
    createParser()
    main(sys.argv[1:])


