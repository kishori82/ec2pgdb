#!/usr/bin/env python

__author__ = "Kishori M Konwar"
__copyright__ = "Copyright 2013, MetaPathways"
__credits__ = ["r"]
__version__ = "1.0"
__maintainer__ = "Kishori M Konwar"
__status__ = "Release"


"""Contains general utility code for the metapaths project"""

try:
    import sys
    from shutil import rmtree
    from optparse import make_option
    from parameters import *
    from configuration import *
    from tools import *
    from libs.python_modules.utils.sysutil import pathDelim, getstatusoutput
    from libs.python_modules.utils.utils import *
    from libs.python_modules.utils.metapathways_utils import  fprintf
    from os import path, _exit, rename , remove
except:
    print "Cannot load some modules"
    sys.exit(0)
   
PATHDELIM = pathDelim()



def staticDiagnose(configs, params, logger = None ):
    """
    Diagnozes the pipeline basedon the configs and params for  
    binaries, scripts and resources               
    """

    """ makes sure that the choices in  parameter file are valid """
    errors = checkParams(params, logger =logger)
    if errors:
       return False

    """ Get the configurations for the executables/scripts and databasess """
    _configuration = Configuration() 
    configuration = _configuration.getConfiguration()
    
    """ the place holders for the tools required to make the run """
    _tools = Tools()
    tools = _tools.getTools()

    """ load the actual executables """
    executables = matchToolsFromConfigs(configs, tools, logger =logger )

    """ make sure all the executables exist """
    executablesExist(executables, configs,  logger =logger)

    parameters = Parameters()

    #print  parameters.getRunSteps()
    """ check if the required set of executables exists """
    #missingList = checkRequiredExecutables(parameters.getRunSteps(), _tools, params, configs, logger =logger)

    """ check if the required standard databases exists """

#    print  parameters.getRunSteps( activeOnly = True)
    if not checkForRequiredDatabases(tools, params, configs, 'functional',  logger = logger):
        return False
        
    if not checkForRequiredDatabases(tools, params, configs, 'taxonomic',  logger = logger):
        return False
    
    message = checkbinaries(configs)
    if message:
       print message
       return False


    return True

def checkbinaries(configs):
  
    message = None
    executables_dir = "---"
    if "METAPATHWAYS_PATH" in configs:
       executables_dir = configs["METAPATHWAYS_PATH"]

    if "EXECUTABLES_DIR" in configs:
       executables_dir += PATHDELIM + configs["EXECUTABLES_DIR"]
    else:
       executables_dir += PATHDELIM + "---"


    if not path.exists(executables_dir):
       message = "ERROR\tMissing executables folder under \'MetaPathways_Python/executables\' it set to " + executables_dir
       return message 

    
    binaries = {}
    binaries["LASTDB_EXECUTABLE"] = ["-h"]
    binaries["LAST_EXECUTABLE"] = ["-h"]

    binaries["FORMATDB_EXECUTABLE"] = ["-help" ]
    binaries["BLASTP_EXECUTABLE"] = ['-h' ] 
    binaries["BLASTN_EXECUTABLE"] = ['-h' ]

    binaries["PRODIGAL_EXECUTABLE"]= ["-h"]
    binaries["SCAN_tRNA_EXECUTABLE"] = ['-h' ] 
    binaries["RPKM_EXECUTABLE"] = ['-h' ]

    status = {}
    error = False
    for name in binaries.keys():

       if name in ["FORMATDB_EXECUTABLE", "BLASTP_EXECUTABLE", "BLASTN_EXECUTABLE"] and configs[name]=='':
          continue

       if not name in configs : 
          status[name] = "BINARY UNSPECIFIED"
          error = True
          continue

       executable =  executables_dir + PATHDELIM + configs[name];

       if not executable.strip():
          status[name] = "BINARY UNSPECIFIED"
          error = True
          continue

       if not path.exists(executable):
          status[name] = "BINARY MISSING"
          error = True
          continue

       result = getstatusoutput( ' '.join([ executable ] +  binaries[name]))

    message = False
    if error:
       message = "ERROR\tOS Specific executables check failed\n\n"
       message += "\tFOLDER :" + executables_dir + "\n\n" 
       message += "\tFIX    : Please correct the location for \"OS Specific Executables\" in the Setup tab\n"
       message += "\t       : Alternatively, you can update the EXECUTABLES_DIR key in the config file \"config/template_config.txt\"\n\n" 

       for name in  status.keys():
           message +=  "\t" +  name +  "  :  " +   status[name]  + "\n";

    return message

def checkForRequiredDatabases(tools, params, configs, dbType, logger =None):
    """ checks the 
          -- database folder structure
          -- checks for raw sequences
          -- checks for formatted sequences
          -- formats if necessary 
    """
    
    if dbType=='functional':
       dbstring = get_parameter(params, 'annotation', 'dbs', default=None)
       _algorithm = get_parameter(params, 'annotation', 'algorithm', default=None)


    if dbType=='taxonomic':
       dbstring = get_parameter(params, 'rRNA', 'refdbs', default=None)
       _algorithm = get_parameter(params, 'annotation', 'algorithm', default=None)


    if dbstring==None:
       eprintf("WARNING\tReference databases to annotate with is unspecified, please add it in the params file\n")
       return False

    dbs= [x.strip() for x in dbstring.split(",")  if len(x)!=0 ]

    if not dbs:
       return True

    refdbspath  = configs['REFDBS']

    """ checks refdb path """
    if not check_if_refDB_path_valid(refdbspath, logger = logger):
        return False
        
    """ checks raw sequences for dbtype functional/taxonimic """
    if isRefDBNecessary(params, dbType):
        if not check_for_raw_sequences(dbs, refdbspath, dbType,  logger = logger):
          return False

        for db in dbs:
           algorithm = ""
           if dbType=='taxonomic':
               algorithm=_algorithm
               seqType = 'nucl'
           elif dbType=='functional':
               algorithm= _algorithm
               seqType = 'prot'
           else:
               algorithm = None

           """ is db formatted ? """
           if not isDBformatted(db, refdbspath, dbType, seqType,  algorithm, logger = logger):
              """ if note formatted then format it """
              eprintf("WARNING\tTrying to format %s  database %s for algorithm %s\n", seqType, sQuote(db), sQuote(algorithm) )
              logger.printf("WARNING\tTrying to format %s database %s for algorithm %s\n", seqType,  sQuote(db), sQuote(algorithm) )

              if not formatDB(tools, db, refdbspath, seqType, dbType, algorithm, configs, logger = logger):
                 return False

           dbMapFile = configs['REFDBS'] + PATHDELIM + dbType + PATHDELIM +\
                       'formatted'  + PATHDELIM + db + "-names.txt"
           seqFilePath = configs['REFDBS'] + PATHDELIM + dbType + PATHDELIM + db
           """ check for dbmapfile """

           if not doesFileExist(dbMapFile):
              eprintf("WARNING\tDoes not have map file %s for %s\n", sQuote(dbMapFile), sQuote(db) )
              logger.printf("WARNING\tDoes not have map file %s for %s\n", sQuote(dbMapFile), sQuote(db) )
              if not createMapFile(seqFilePath, dbMapFile):
                 eprintf("ERROR\tFailed to create map file %s for %s\n", sQuote(dbMapFile),sQuote(db))
                 logger.printf("ERROR\tFailed to create map file %s for %s\n",sQuote(dbMapFile), sQuote(db) )
                 return False
              eprintf("INFO\tSuccessfully created  map file %s for %s\n", sQuote(dbMapFile), sQuote(db) )
              logger.printf("INFO\tSuccessfully created map file %s for %s\n", sQuote(dbMapFile), sQuote(db) )


    return True

def createMapFile(seqFilePath, dbMapFile):
      """ Creates the dbMapFile from sequence file seqFilePath """
      try:
           mapfile = open(dbMapFile,'w')
           seqFile = open(seqFilePath,'r')
           for line in seqFile:
                 if re.match(r'>', line):
                    fprintf(mapfile, "%s\n",line.strip())
           seqFile.close()
           mapfile.close()
      except:
           return False
      return True


def formatDB(tools, db, refdbspath, seqType, dbType, algorithm, configs, logger = None):
     """ Formats the sequences for the specified algorithm """
     EXECUTABLES_DIR = configs['METAPATHWAYS_PATH'] + PATHDELIM + configs['EXECUTABLES_DIR'] 
     formatdb_executable = EXECUTABLES_DIR + PATHDELIM + tools['FUNC_SEARCH']['exec']['BLAST']['FORMATDB_EXECUTABLE']
     if seqType=='nucl':
       if configs['FORMATDB_EXECUTABLE']:
         if algorithm=='LAST':
            formatdb_executable = EXECUTABLES_DIR + PATHDELIM + tools['FUNC_SEARCH']['exec']['LAST']['LASTDB_EXECUTABLE'] 
         if algorithm=='BLAST':
            formatdb_executable = EXECUTABLES_DIR + PATHDELIM + tools['FUNC_SEARCH']['exec']['BLAST']['FORMATDB_EXECUTABLE'] 
       else:
         if algorithm=='BLAST':
            formatdb_executable = which('makeblastdb') 
            if formatdb_executable==None:
               return False
         else:
             return False

     if seqType=='prot':
       if configs['FORMATDB_EXECUTABLE']:
          if algorithm=='LAST':
             formatdb_executable = EXECUTABLES_DIR + PATHDELIM + tools['FUNC_SEARCH']['exec']['LAST']['LASTDB_EXECUTABLE']
          if algorithm=='BLAST':
             formatdb_executable = EXECUTABLES_DIR + PATHDELIM + tools['FUNC_SEARCH']['exec']['BLAST']['FORMATDB_EXECUTABLE']
       else:
          if algorithm=='BLAST':
             formatdb_executable = which('makeblastdb') 
             if formatdb_executable==None:
                return False
          else:
             return False

     formatted_db = refdbspath + PATHDELIM + dbType + PATHDELIM + 'formatted'  + PATHDELIM + db
     raw_sequence_file = refdbspath + PATHDELIM + dbType + PATHDELIM + db

     _temp_formatted_db  =  formatted_db + "__temp__"

     """ format with 4GB file size """
     cmd = ""
     if algorithm=='BLAST':
         cmd='%s -dbtype %s -max_file_sz 2000000000  -in %s -out %s' %(formatdb_executable, seqType, raw_sequence_file, _temp_formatted_db)
         
         #cmd='%s -dbtype %s -max_file_sz 20267296  -in %s -out %s' %(formatdb_executable, seqType, raw_sequence_file, _temp_formatted_db)

     formatted_db_size = 4000000000
     if 'FORMATTED_DB_SIZE' in configs and configs['FORMATTED_DB_SIZE'].isdigit():
       formatted_db_size = int(configs['FORMATTED_DB_SIZE'])


     if algorithm=='LAST':
         # dirname = os.path.dirname(raw_sequence_file)    
         cmd=""
         if seqType=="prot":
            cmd='%s -s %s -p -c %s  %s' %(formatdb_executable, formatted_db_size, _temp_formatted_db, raw_sequence_file)
         if seqType=="nucl":
            cmd='%s -s %s -c %s  %s' %(formatdb_executable, formatted_db_size,  _temp_formatted_db, raw_sequence_file)

         eprintf("INFO\tCommand to format \"%s\"\n", cmd)
         logger.printf("INFO\tCommand to format \"%s\"\n", cmd)

     result= getstatusoutput(cmd)
     temp_fileList = glob(_temp_formatted_db + '*') 

     _formatted_db_pal = _temp_formatted_db + ".pal"
     if algorithm=='BLAST' and path.exists(_formatted_db_pal):
       try: 

         formatted_db_pal = formatted_db + ".pal"
         if seqType=="nucl":
            formatted_db_pal = formatted_db + ".nal"


         _openpal = open(_formatted_db_pal, 'r')
         openpal = open(formatted_db_pal, 'w')
         lines = _openpal.readlines()
         tempPATT =re.compile(r'__temp__')
         for line in lines:
             _result = tempPATT.search(line)
             modline = line.strip()
             if _result: 
                modline = re.sub('__temp__','', modline)
             fprintf(openpal, "%s\n", modline)
         openpal.close()
         _openpal.close()
         remove(_formatted_db_pal)
       except:
           return False

     try:
         temp_fileList = glob(_temp_formatted_db + '*') 
         for tempFile in temp_fileList:
             file = re.sub('__temp__','', tempFile)
             rename(tempFile, file);
     except:
         return False


     if result[0]==0:
        eprintf("INFO\tFormatted database %s successfully for %s\n", sQuote(db), sQuote(algorithm) )
        logger.printf("INFO\tFormatted database %s successfully for %s\n", sQuote(db), sQuote(algorithm) )
        return True 
     else:
        eprintf("INFO\tFailed to Format database %s for %s\n", sQuote(db), sQuote(algorithm) )
        eprintf("INFO\tReason for failure %s\n", result[1])
        logger.printf("INFO\tReason for failure %s\n", result[1])
        logger.printf("INFO\tFailed to Format database %s for %s\n", sQuote(db), sQuote(algorithm) )
        return False


def isRefDBNecessary(params, dbType ):
    """ decide yes or no based on the params settings yes or redo """
    if dbType=="functional": 
        status = get_parameter(params, 'metapaths_steps', 'FUNC_SEARCH', default=None)
        if status in [ 'yes', 'redo' ]:
           return True

    if dbType=="taxonomic": 
        status = get_parameter(params, 'metapaths_steps', 'SCAN_rRNA', default=None)
        if status in [ 'yes', 'redo' ]:
           return True

    return False


def isDBformatted(db, refdbspath, dbType, seqType,  algorithm, logger = None):
    """ check if the DB is formatted """
    """Checks if the formatted database for the specified algorithm exits """
    dbPath = refdbspath + PATHDELIM + dbType + PATHDELIM + 'formatted'  
    dbname = dbPath + PATHDELIM + db 
    suffixes = getSuffixes(algorithm, seqType) 

    #print algorithm, suffixes
    if not suffixes :
       return False

    status = False
    for suffix in suffixes:
       allfileList = glob(dbname + '*.' + suffix)

       fileList = []
       tempFilePattern = re.compile(r''+ dbname + '[.\d]*.' + suffix +'$');

       for aFile in allfileList:
           searchResult =  tempFilePattern.search(aFile)
           if searchResult:
             fileList.append(aFile)

       if len(fileList)==0 :
          eprintf("WARNING\tsequence for db  %s not formatted\n", dbname )
          logger.printf("WARNING\tsequence for db  %s not formatted\n", dbname )
          return False

       status = True

    return status

def check_if_refDB_path_valid(refdbspath, logger = None):
    """ it checks for the validity of the refdbs path structure 
       refdbpath  /functional
                      /formatted
                  /tanxonomic
                      /formatted
    """

    status = True
    if not doesFolderExist(refdbspath):
        eprintf("ERROR\treference sequence folder %s not found\n", sQuote(refdbspath))
        logger.printf("ERROR\treference sequence folder %s not found\n", sQuote(refdbspath))
        return False

    dbTypes = [ 'functional', 'taxonomic' ]  
    """ now check if respective dbtype folders are available """
    status = True
    for dbType in dbTypes:
       if not doesFolderExist(refdbspath + PATHDELIM + dbType):
          eprintf("ERROR\tfolder %s for reference type %s not found\n", sQuote(refdbspath + PATHDELIM + dbType), dbType)
          logger.printf("ERROR\tfolder %s for reference type %s not found\n", sQuote(refdbspath + PATHDELIM + dbType), dbType)
          status = False

    if status == False: 
       return status;

    """ now check if path to drop the formatted dbs are available """
    for dbType in dbTypes:
       if not doesFolderExist(refdbspath + PATHDELIM + dbType + PATHDELIM + 'formatted'):
          eprintf("ERROR\tsubfolder %s not found under the folder %s\n" , sQuote('formatted'), \
                   sQuote(refdbspath + PATHDELIM + dbType + PATHDELIM) )
          logger.printf("ERROR\tsubfolder %s not found under the folder %s\n" , sQuote('formatted'), \
                   sQuote(refdbspath + PATHDELIM + dbType + PATHDELIM) )
          status = False

    return status


def check_for_raw_sequences(dbs, refdbspath, dbType,  logger = None):
    """ check for the raw sequence file """
    status = True
    for db in dbs:
       fullPath =  refdbspath + PATHDELIM + dbType + PATHDELIM +  db 
       if not doesFolderExist(fullPath):
            eprintf("ERROR\tRaw sequences %s expected for %s references\n", fullPath, dbType)
            logger.printf("ERROR\tRaw sequences %s expected for %s references\n", fullPath, dbType)
            status = False

    return status 
    

def get_parameter(params, category, field, default = None):
    """  gets the parameter value from a category 
       as specified in the  parameter file """

    if params == None:
      return default

    if category in params:
        if field in params[category]:
            return params[category][field]
        else:    
            return default
    return default



def checkRequiredExecutables(steps, tools, params, configs, logger = None):
    """  check the required executables in the steps """
    missingList = []
    for step in steps:
       missingList +=  executablesExist(tools.getExecutables(step, params), configs,  logger)

    return missingList


def executablesExist( executables, configs, logger = None ):
    missingList = []
    names = executables.keys()
    
    for name in names:
      script = executables[name]

      if name!='PATHOLOGIC_EXECUTABLE' and  path.exists(configs['METAPATHWAYS_PATH'] +\
         PATHDELIM  + script):
           executables[name] = configs['METAPATHWAYS_PATH'] + PATHDELIM + script
           continue

      if name!='PATHOLOGIC_EXECUTABLE' and  path.exists(configs['METAPATHWAYS_PATH'] +\
         PATHDELIM + configs['EXECUTABLES_DIR'] + PATHDELIM + script):

           executables[name] = configs['METAPATHWAYS_PATH'] +  PATHDELIM + configs['EXECUTABLES_DIR'] + PATHDELIM + script
           continue

      if name=='PATHOLOGIC_EXECUTABLE' and  path.exists(script):
           #print "FIX ME: diagnoze"
           eprintf("ERROR\tif you do not wish to install the Pathway-Tools and  run the ePGDB building step \n" + 
                       "\tyou might want to create the place holder file by using the \"touch %s\" (%s) \n",script, name)
           continue

      
      eprintf("ERROR\tscript %s for %s not found\n",script, name)
      logger.printf("ERROR\tscript %s for %s not found\n",script, name)
      missingList.append(script)

    return missingList

def matchToolsFromConfigs(configs, tools, logger = None ):
    """ iterate througs each of the configs item and fill it to 
        to the actual value in the tools 
    """
    executables = {}
    """ iterate through each config key """
    for config_key, config_value in configs.iteritems():
       for param_step, placeHolder in tools.iteritems(): 
         if param_step in tools:
           for script in tools[param_step]['exec']: 

               """ does not have alternatives """
               if not type(tools[param_step]['exec'][script]) is dict: 
                   if config_key == script: 
                      tools[param_step]['exec'][script]=config_value
                      executables[script]= config_value
               else: 
                   """ go a level deeper """
                   for sub_script in tools[param_step]['exec'][script]: 
                     if config_key == sub_script: 
                        tools[param_step]['exec'][script][sub_script]=config_value
                        executables[sub_script]= config_value

    return executables


def getRequiredTools(params, configs,  tools, configuration):
    if not 'metapaths_steps' in params:
       return None

    for key, value in params['metapaths_steps'].iteritems():
        if value in [  'skip', 'redo', 'yes' ]:

           if not key in tools:
              #print "ERROR : " + key + " is missing in class Tools file!"
              continue

           if type(tools[key]['exec'][key]) is dict: 
              for execname in tools[key]['exec']: 
                 print execname
          
           #if not tools[key]['exec'] in configs:
              #print "ERROR : Exec in " + tools[key]['exec']  + " is missing in class Configuration file!"
           #   tools[key]['exec'] = None
           #   continue

           #print configs[tools[key]['exec']]
           #tools[key]['exec'] = [ configs[tools[key]['exec']] ]


def _checkParams(params, paramsAccept, logger = None, errors= None):

    """  make sure that every parameter in the params is valid recursively 
     This is initialed by the checkParams() function 
     store the erros in the erros dictionary 
     """
    """ if not level to go deeper  then the leaves of the dict are reached"""

    if not type(params) is dict and  type(paramsAccept) is dict:
        #print  'type ',  params, paramsAccept,  (not params in paramsAccept), (len(paramsAccept.keys())!=0)
        try:
           if (not params in paramsAccept) and len(paramsAccept.keys())!=0:
               errors[params] = False
               choices = ', '.join(paramsAccept.keys()) 
               eprintf("ERROR\tValue for key %s, in param file,  is not set propertly must be one of %s \t %s\n", sQuote(params), sQuote(choices), __name__)
               logger.printf("ERROR\tValue for key %s, in param file,  is not set propertly must be one of %s\t%s\n", sQuote(params), sQuote(choices),  __name__)
        except:
               pass
        return

    """  make sure that every parameter in the params is valid recursively """
    for key, value in params.iteritems(): 
        if type(paramsAccept) is dict:
           if len(key) and key in paramsAccept:
               _checkParams(params[key], paramsAccept[key], logger= logger, errors = errors)

def checkParams(params, logger = None):
    """ makes sure that all the params provides are valid or acceptable """
    """ when the choices are not any of the acceptable 
    values then it is considered erroneous"""

    _paramsAccept = Parameters()
    paramsAccept = _paramsAccept.getAcceptableParameters() 
    errors = {}

    for key, value in params.iteritems(): 
       if key in paramsAccept:
          _checkParams(params[key], paramsAccept[key], logger = logger, errors = errors)

    return errors


def getSuffixes(algorithm, seqType) :
    """ Get the suffixes for the right algorithm with the right 
        sequence type 
    """

    suffixes = {}
    suffixes['LAST'] = {}
    suffixes['BLAST'] = {}
    suffixes['BLAST']['nucl'] = ['nhr', 'nsq', 'nin']
    suffixes['BLAST']['prot'] = ['phr', 'psq', 'pin']

    suffixes['LAST']['nucl'] = [ 'des', 'sds', 'suf', 'bck', 'prj', 'ssp', 'tis' ]
    suffixes['LAST']['prot'] = [ 'des', 'sds', 'suf', 'bck', 'prj', 'ssp', 'tis' ]

    if not algorithm in suffixes:
        return None


    if not seqType in suffixes[algorithm]:
        return None

    return suffixes[algorithm][seqType]

