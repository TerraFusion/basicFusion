class granule:
    def __init__ (self, tarFile='', untarDir='', orbit=0, pathFileList='', year=0, BFoutputDir='', BFfileName = '', logFile = '', \
                    inputFileList = '' ):
        self.untarDir   = untarDir
        self.orbit      = orbit
        self.pathFileList   = pathFileList
        self.year       = year
    
        # BFoutputDir
        # Defines the top level output directory of the basic fusion product.
        self.BFoutputDir = BFoutputDir

        # BFfileName
        # Defines the file name of this specific granule
        self.BFfileName  = BFfileName

        # orbit_start_time
        # Defines the start time of the orbit. This should be a string in the form of:
        # YYYYMMDDhhmmss
        self.orbit_start_time = ''

        self.logFile     = logFile 
        self.inputFileList = inputFileList
        self.BF_exe     = None
        self.orbit_times_bin = None
        self.sourceTarPath = None
        self.destTarPath   = tarFile
        self.proc_log_dir = None
        self.metadata_dir = None
        self.ddl_path   = None
        self.cdl_path   = None

        # outputFilePath
        # Determines the absolute path of the output file on the host machine. This will include
        # at least the BFoutputDir and the BFfileName, but may include any arbitrary
        # directory structure in between those two attributes.
        self.outputFilePath = ''

        self.hash_status = -1
        
        # copyList will be a list that contains:
        # 1. The list of MISR path files to copy over to the untar directory
        # 2. Any other files that need to be copied to the untar directory
        self.copyList = None

class processQuanta:
    '''
    This is a class that contains all relevant information for submitting one "quanta"
    of work in the Basic Fusion workflow. One quanta of work involves three separate jobs:
    1. Pulling tar data from Nearline (pull)
    2. Generating the new Basic Fusion files (process)
    3. Pushing the new Basic Fusion files back to Nearline. (push)
    '''
    def __init__( self, pullPBS='', processPBS='', pushPBS='', orbitStart=0, orbitEnd=0):
        self.orbitStart = orbitStart
        self.orbitEnd = orbitEnd
        self.PBSfile = { 'pull' : pullPBS, 'process' : processPBS, 'push' : pushPBS }
        self.jobID   = { 'pull' : '',      'process' : '',         'push' : ''      }
