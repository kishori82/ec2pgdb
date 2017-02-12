"""This module defines classes for parsing BLAST output."""

import multiprocessing
import time
import sys
import os
import re
import math
from os import  sys
import re, traceback
from glob import glob
try:
    from libs.python_modules.utils.metapathways_utils  import parse_command_line_parameters, fprintf, printf, eprintf
    from libs.python_modules.utils.metapathways_utils  import ShortenORFId, ShortenContigId, ContigID
except:
    print """ Could not load some user defined  module functions"""
    print """ Make sure your typed 'source MetaPathwaysrc'"""
    print """ """
    sys.exit(3)

def parse_entry(entry, options):
    # Pull out details about the entry

    query = None
    lines = entry.split("\n")
    results = []
    while lines:

        line = lines.pop(0)

        # Set the name of our query if applicable
        
        if line.startswith('Query='):
            query = line[7:].strip()

        # Start pulling if our line starts with '>'

        if line.startswith('>'):
            lines.insert(0, line)
            result = dict(query=query, kegg=None, ec_numbers=[],
                          match_length=0, source_length=0, hit_coverage=0.0,
                          score=0.0, expect=1e1000, identities=0.0,
                          positives=0.0, gaps=100.0)

            # Parse the product

            product = []
            while lines and 'Length' not in lines[0]:
                product.append(lines.pop(0).strip())
            product = ' '.join(product).lstrip('>')

            # If KEGG mode is enabled, pull out the identifier

            if options['kegg_mode']:
                kegg_results = re.search(r'^([a-z]{3}:[A-Z0-9_]+)', product)
                if kegg_results:
                    options['kegg'] = kegg_results.group(1)
                    product = re.sub(r'^([a-z]{3}:[A-Z0-9_]+)', '', product)
            result['product'] = product.strip()

            # If there are any EC numbers, pull them out!

            ec_results = re.findall(r'\[EC:(.+?)\]', product)
            result['ec_numbers'] = []
            for ec_result in ec_results:
                result['ec_numbers'].extend(re.split(r'\s+', ec_result))

            # Pull out the length of the source

            if lines and 'Length' in lines[0]:
                result['source_length'] = int(lines.pop(0).strip().split(' = ')[1])

            while lines and 'Score' not in lines[0]:
                lines.pop(0)

            # Pull out statistics

            if lines and 'Score' in lines[0]:
                subline = lines.pop(0)

                score_results = re.search(r'Score =\s+(\d+(\.\d+)?)', subline)
                if score_results:
                    result['score'] = float(score_results.group(1))

                    # Skip if the score is too low
                    if result['score'] < options['minimum_score']:
                        continue

                expect_results = re.search(r'Expect =\s+(\S+)', subline)
                if expect_results:
                    expect_value = expect_results.group(1)
                    if expect_value.startswith('e'):
                        expect_value = '1%s' % expect_value
                    result['expect'] = float(expect_value)

                    # Skip if the expect value is too high
                    if result['expect'] > options['maximum_expect_value']:
                        continue

            if lines and 'Identities' in lines[0]:
                subline = lines.pop(0)

                identities_result = re.search(r'Identities =\s+(\d+)/(\d+)', subline)
                if identities_result:
                    result['identities'] = (float(identities_result.group(1)) /
                                            float(identities_result.group(2)) *
                                            100)
                    result['identities_num'] = int(identities_result.group(1))
                    
                    # Skip if the % identity is too low
                    if result['identities'] < options['minimum_identity']:
                        continue

                    result['match_length'] = int(identities_result.group(2))

                    # Skip if the match length is too short or too long
                    if (result['match_length'] < options['minimum_length'] or
                        result['match_length'] > options['maximum_length']):
                        continue

                    result['hit_coverage'] = (result['match_length'] /
                                              float(result['source_length']) *
                                              100)

                    # Skip if the coverage is too low
                    if result['hit_coverage'] < options['minimum_hit_coverage']:
                        continue

                positives_result = re.search(r'Positives =\s+(\d+)/(\d+)', subline)
                if positives_result:
                    result['positives'] = (float(positives_result.group(1)) /
                                           float(positives_result.group(2)) *
                                           100)
                    result['positives_num'] = int(positives_result.group(1))

                    # Skip if the % positives is too low
                    if result['positives'] < options['minimum_positives']:
                        continue

                gaps_result = re.search(r'Gaps =\s+(\d+)/(\d+)', subline)
                if gaps_result:
                    result['gaps'] = (float(gaps_result.group(1)) /
                                      float(gaps_result.group(2)) * 100)
                    result['gaps_num'] = int(positives_result.group(1))

                    # Skip if the % gaps is too high
                    if result['gaps'] > options['maximum_gaps']:
                        continue

            while lines and not lines[0].startswith('>'):
                lines.pop(0)

            # Calculate the heuristic
            # The heuristic is: bit score * hit coverage / 100 * identities /
            # 100 * positives / 100 * (1 - gaps / 100) * max(100,
            # -log10(expect) / 100)
            # TODO: Tweak this heuristic
            heuristic = (result['score'] * result['hit_coverage'] / 100 *
                         result['identities'] / 100 * result['positives'] / 100 *
                         (1 - (result['gaps'] / 100)) * max(100, -1 *
                                                            math.log10(result['expect']))
                         / 100)
            result['heuristic'] = heuristic

            results.append(result)

    # Pick the top n results
    results.sort(cmp=lambda x, y: cmp(y['heuristic'], x['heuristic']))
    n = 0
    ret = []
    while n < int(options['number_of_results']) and results:
        result = results.pop(0)
        ret.append((query, result['product'], result['score'],
                    result['expect'], result['match_length'],
                    result['source_length'], result['hit_coverage'],
                    result['identities_num'], result['identities'],
                    result['positives_num'], result['positives'],
                    result['gaps_num'], result['gaps'], result['ec_numbers'],
                    result['heuristic']))
        n += 1

    #sys.stderr.write('.')

    return ret



class BlastParser(object):
    """Parses BLAST output."""

    def __init__(self, handler, number_of_results=1, minimum_score=0.0,
                 minimum_hit_coverage=0.0, maximum_expect_value=1e-6,
                 minimum_length=0, maximum_length=1e1000, minimum_identity=0.0,
                 maximum_identity=100.0, minimum_positives=0.0,
                 maximum_gaps=100.0, bsr_file=None, minimum_bsr=0.0,
                 kegg_mode=False):
        self.handler = handler
        self.options = {
            'number_of_results': int(number_of_results),
            'minimum_score': float(minimum_score),
            'minimum_hit_coverage': float(minimum_hit_coverage),
            'maximum_expect_value': float(maximum_expect_value),
            'minimum_length': int(minimum_length),
            'maximum_length': float(maximum_length),
            'minimum_identity': float(minimum_identity),
            'maximum_identity': float(maximum_identity),
            'minimum_positives': float(minimum_positives),
            'maximum_gaps': float(maximum_gaps),
            'bsr_file': bsr_file,
            'minimum_bsr': float(minimum_bsr),
            'kegg_mode': bool(kegg_mode)
        }

    def __get_entry_line(self):
        line = self.handler.readline()
        if not line:
            return None
        if line.startswith('BLAST'):
            self.handler.seek(self.handler.tell() - len(line))
            return None
        return line

    def __get_results(self):
        results = []

        # Hello, pool

        pool = multiprocessing.Pool(processes=1)

        def worker_callback(result):
            results.extend(result)

        # We'll have to manually manipulate the file pointer here...

        while True:
            line = self.handler.readline()
            if line == '':
                break

            # Start pulling out an entry

            if line.startswith('BLAST'):
                entry = [line]
                entry_line = self.__get_entry_line()
                while entry_line is not None:
                    entry.append(entry_line)
                    entry_line = self.__get_entry_line()

                # Perform a dispatch to parse the entry

                pool.apply_async(parse_entry, args=[''.join(entry),
                                                    self.options],
                                 callback=worker_callback)

        pool.close()
        pool.join()

        return results

    results = property(__get_results)


class GffFileParser(object):

    def __init__(self, gff_filename, shortenorfid=False):
        self.Size = 10000
        self.i=0
        self.orf_dictionary = {}
        self.gff_beg_pattern = re.compile("^#")
        self.lines= []
        self.size=0
        self.shortenorfid = shortenorfid
        try:
           self.gff_file = open( gff_filename,'r')
        except AttributeError:
           eprintf("Cannot read the map file for database : %s\n", dbname)
           exit_process()
  
    def __iter__(self):
        return self
 
    def refillBuffer(self):
       self.orf_dictionary = {}
       i = 0
       while i < self.Size:
          line=self.gff_file.readline()
          if not line:
            break
          if self.gff_beg_pattern.search(line):
            continue
          insert_orf_into_dict(line, self.orf_dictionary, self.shortenorfid)
          #print self.orf_dictionary
          i += 1

       self.orfs = self.orf_dictionary.keys()
       self.size = len(self.orfs)
       self.i = 0

    def next(self):
        if self.i == self.size:
           self.refillBuffer()

        if self.size==0:
           self.gff_file.close()
           raise StopIteration()

        #print self.i
        if self.i < self.size:
           self.i = self.i + 1
           return self.orfs[self.i-1]
       


class BlastOutputTsvParser(object):
    def __init__(self, dbname,  blastoutput, shortenorfid=False):
        self.dbname = dbname
        self.blastoutput = blastoutput
        self.i=1
        self.data = {}
        self.fieldmap={}
        self.shortenorfid=shortenorfid
        self.seq_beg_pattern = re.compile("#")

        try:
           self.blastoutputfile = open( blastoutput,'r')
           self.lines=self.blastoutputfile.readlines()
           self.blastoutputfile.close()
           self.size = len(self.lines)
           if not self.seq_beg_pattern.search(self.lines[0]) :
              exit_process("First line must have field header names and begin with \"#\"")
           header = self.lines[0].replace('#','',1)
           fields = [ x.strip()  for x in header.rstrip().split('\t')]
           k = 0 
           for x in fields:
             self.fieldmap[x] = k 
             k+=1
           eprintf("\nProcessing database : %s\n", dbname)
           
        except AttributeError:
           eprintf("Cannot read the map file for database :%s\n", dbname)
           exit_process()
  
    def __iter__(self):
        return self
    count = 0 
    def next(self):
        if self.i < self.size:
           
           try:
              fields = [ x.strip()  for x in self.lines[self.i].split('\t')]
              #print self.fieldmap['ec'], fields, self.i,  self.blastoutput
              if self.shortenorfid:
                 self.data['query'] = ShortenORFId(fields[self.fieldmap['query']])
              else:
                 self.data['query'] = fields[self.fieldmap['query']]

              self.data['target'] = fields[self.fieldmap['target']]
              self.data['q_length'] = int(fields[self.fieldmap['q_length']])
              self.data['bitscore'] = float(fields[self.fieldmap['bitscore']])
              self.data['bsr'] = float(fields[self.fieldmap['bsr']])
              self.data['expect'] = float(fields[self.fieldmap['expect']])
              self.data['identity'] = float(fields[self.fieldmap['identity']])
              self.data['ec'] = fields[self.fieldmap['ec']]
              self.data['product'] = re.sub(r'=',' ',fields[self.fieldmap['product']])

              self.i = self.i + 1
              return self.data
           except:
              print self.lines[self.i]
              print traceback.print_exc(10)
              sys.exit(0)
              return None
        else:
           raise StopIteration()
              
def getParsedBlastFileNames(blastdir, sample_name, algorithm) :
    database_names = []
    parsed_blastouts = [] 

    dbnamePATT = re.compile(r'' + blastdir + '*' + sample_name + '*[.](.*)[.]' + algorithm.upper() + 'out.parsed.txt')

    blastOutNames = glob(blastdir + '*' + algorithm.upper() + 'out.parsed.txt')  
    for blastoutname in blastOutNames :
        result = dbnamePATT.search(blastoutname)
        if result:
            dbname = result.group(1)
            database_names.append(dbname)
            parsed_blastouts.append(blastoutname)


    return database_names, parsed_blastouts

def getrRNAStatFileNames(blastdir, sample_name, algorithm) :
    database_names = []
    parsed_blastouts = [] 

    dbnamePATT = re.compile(r'' + blastdir + '*' + sample_name + '*[.](.*)[.]' + 'rRNA.stats.txt')

    blastOutNames = glob(blastdir + '*' + 'rRNA.stats.txt')  
    for blastoutname in blastOutNames :
        result = dbnamePATT.search(blastoutname)
        if result:
            dbname = result.group(1)
            database_names.append(dbname)
            parsed_blastouts.append(blastoutname)


    return database_names, parsed_blastouts





