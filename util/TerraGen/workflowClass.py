from mpi4py import MPI


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

