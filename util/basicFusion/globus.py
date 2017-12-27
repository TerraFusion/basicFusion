import os

class GlobusTransfer:
    def __init__(self, sourceID, destID ):
        self.sourceID = sourceID
        self.destID = destID
    
        self._file_list = []

    def add_file( self, src_path, dest_path ):
        '''
        Append a file to self's file list.
        '''
        file = os.path.abspath( file )
        self._file_list.append( file )

    
    def download( self, granuleList, remoteID, hostID, numTransfer, retry=False ):
        '''
        Initiate a Globus transfer request using the files in self's file list.
        '''

        CHECK_SUM='--no-verify-checksum'
       
        num_lines=len(granuleList) 
        logger.debug("Number of orbits to transfer: {}".format( len(granuleList) ))
        
        # ==========================
        # = SPLIT THE GRANULE LIST =
        # ==========================

        # Find how to split the granuleList
        linesPerPartition = len(granuleList) / numTransfer
        extra   = len(granuleList) % numTransfer

        logger.debug("linesPerPartition: {}".format(linesPerPartition))
        logger.debug("extra: {}".format(extra))

        # Contains the path of each globus batch transfer text file. Will contain numTransfer elements
        splitList = []

        # Set the directory where our globus batch transfer files will go
        transferPath = os.path.join( logDirs['misc'], 'globus_batch' )
        try:
            os.makedirs(transferPath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        for i in xrange(numTransfer):
            # Add the filename of our new split-file to the list
            if retry == True:
                splitList.append( os.path.join( transferPath, progName + "_globus_list_retry_{}.txt".format(i)) )
            else:
                splitList.append( os.path.join( transferPath, progName + "_globus_list_{}.txt".format(i)) )
                
            logger.debug("Making new split file: {}".format(splitList[i]))

            with open( splitList[i], 'w' ) as partition:
                logger.debug("Writing {}".format( os.path.basename(splitList[i])))
                
                # Slice the granule list appropriately
                for j in granuleList[i * linesPerPartition : i*linesPerPartition + linesPerPartition]:
                    line = '{} {}'.format( j.sourceTarPath, j.destTarPath )
                    # Write the line to the globus batch transfer file
                    partition.write( line + os.linesep )

        # Write the extra files to the last partition
        with open( splitList[numTransfer-1], 'a') as partition:
            for j in granuleList[ numTransfer*linesPerPartition: ]:
