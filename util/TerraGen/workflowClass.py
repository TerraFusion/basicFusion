from mpi4py import MPI

class inputData:
    def __init__ (self, tarFile='', untarDir='', orbit=0, pathFileList='', year=0, BFoutputDir='', BFfileName = '', logFile = '' \
                    inputFileList = '' ):
        self.tarFile    = tarFile
        self.untarDir   = untarDir
        self.orbit      = orbit
        self.pathFileList   = pathFileList
        self.year       = year
        self.BFoutputDir = BFoutputDir
        self.BFfileName  = BFfileName
        self.logFile     = logFile 
        self.inputFileList = inputFileList

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

