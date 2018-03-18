from bfutils.globus import constants
import os
import errno
from dateutil.parser import parse
import subprocess
import re
import uuid

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu'

class GlobusTransfer:
    '''
    This class acts as a wrapper utility around the Globus Python CLI.
    This class interacts with the CLI via subprocesses.
    '''
    def __init__(self, sourceID, destID ):
        self.sourceID = sourceID
        self.destID = destID
        self._file_list = []
        self._submitIDs = [] 

    def add_file( self, src_path, dest_path, recursive=False):
        '''
Append a file to self's file list.

**ARGUMENTS:**  
    *src_path (str)* -- The source path of the file. Must be absolute.  
    *dest_path (str)* -- The destination path of the file. Can be either
                       a directory path or a file path. Must be absolute.  
    *recursive (bool)* -- If src_path is a directory, set recursive to True
                        to transfer entire directory.  
        '''

        if type(recursive) is not bool:
            raise TypeError("Argument 'recursive' is not a boolean.")
            
        self._file_list.append( { 'src' : src_path, 
                                  'dest' : dest_path, 
                                  'recursive' : recursive 
                                } )

    def transfer( self, parallel=1, sync_level=None, verify_checksum=False, 
                  deadline=None, label=None, retry=False, 
                  batch_dir=constants.BATCH_DIR ):
        '''
Initiate a Globus transfer request using the files in self's file list.

**ARGUMENTS:**  
    *parallel (int)* -- Number of concurrent Globus transfer requests to use
                      for self's file list. e.g. parallel=3 will split the 
                      file list 3 way.  
    *sync_level (str)* -- Synchronization level. Set the level at which the 
                        Globus CLI determines whether the files between
                        source and destination differ. Allowable values are:  
                        'exists' will copy if file doesn't exist at dest  
                        'size' will copy if size of file differs  
                        'mtime' will copy if modification times differ  
                        'checksum' will copy if checksums differ  
    *verify_checksum (bool)* -- After transfer, verify that the checksums of
                              the file between source and dest match. If
                              they differ, automatically retry file until
                              this check succeeds.
    *deadline (str)* -- Transfer UTC deadline in the format
                        'YYYY-MM-DD hh:mm:ss' or 'YYYY-MM-DD'. Can't be
                        larger than 30 days from present. If the latter 
                        format, time is assumed to be 00:00:00.  
    *label (str)*    -- The label for the transfer. Defines the name for the batch
                      transfer files and to Globus itself. Non-unique
                      names may cause old batch files to be overwritten.  
    *retry (bool)* -- Retry failed files once.  
    *batch_dir (str)* -- If not none, will save the Globus batch files to
                       this directory.  
'''

        # Perform argument checking
        if type(parallel) is not int:
            raise TypeError(
                "Argument 'parallel' is not an integer.")

        if parallel < 1:
            raise ValueError(
                "Argument 'parallel' can't be less than 1! \
                 Received value: {}".format(parallel))

        support_sync = [ 'exists', 'size', 'mtime', 'checksum' ]
        if sync_level not in support_sync and sync_level is not None:
            raise TypeError(
                "Argument 'sync_level' has unsupported value. \
                Type: {}".format( type( sync_level ) ) )

        if type( verify_checksum ) is not bool:
            raise TypeError(
                "Argument 'verify_checksum' is not a bool.")

        if deadline is not None:
            # Attempt to parse deadline. Will raise exception if it's not
            # valid ISO format.
            _ = parse(deadline) 

        if label is not None and type(label) is not str:
            raise TypeError(
                "Argument 'label' is not a string.")

        if type(retry) is not bool:
            raise TypeError(
                "Argument 'retry' is not a bool.")

        if not os.path.isdir( batch_dir ):
            raise ValueError(
            "Argument 'batch_dir' is not a directory.")
            
        CHECK_SUM='--no-verify-checksum'
       
        num_lines=len( self._file_list ) 
        
        # =======================
        # = SPLIT THE FILE LIST =
        # =======================

        if len( self._file_list ) <= 0:
            raise ValueError('No files added to transfer!')

        while len( self._file_list ) < parallel:
            parallel -= 1
        # Find how to split the granuleList
        linesPerPartition = num_lines  / parallel
        extra   = num_lines / parallel

        # Contains the path of each globus batch transfer text file. Will contain numTransfer elements
        splitList = []
 
        # Set the directory where our globus batch transfer files will go
        try:
            os.makedirs(constants.BATCH_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        if label is None:
            label = str(uuid.uuid4())

        for i in xrange( parallel ):
            # Add the filename of our new split-file to the list
            
            splitList.append( os.path.join( batch_dir, label + "_{}.txt".format(i)) )
                
            with open( splitList[i], 'w' ) as partition:
                
                # Slice the file list appropriately
                if i == parallel - 1:
                    tail = len( self._file_list )
                else:
                    tail = i*linesPerPartition + linesPerPartition
                
                for file in self._file_list[i * linesPerPartition : tail ]:
                    recurse = ''
                    if file['recursive']:
                        recurse = '--recursive '
                    
                    line = '{}{} {}'.format( recurse, file['src'], file['dest'] )
                    
                    # Write the line to the globus batch transfer file
                    partition.write( line + os.linesep )


        # Do a sanity check to make sure we didn't lose any files in the process.
        # Sum the number of lines in all of our batch transfer files. If that does
        # not equal to the length of granuleList, we missed something.
        num_lines = 0
        for batch in splitList:
            num_lines += sum(1 for line in open( batch ) )

        if num_lines != len( self._file_list ):
            raise ValueError("num_lines ({}) != length of file list ({})".format(num_lines, len( self._file_list )))

        # ================================
        # = SUBMIT SPLIT LISTS TO GLOBUS =
        # ================================

        # We have finished splitting up the transferList file. We can now submit these to Globus to download.
        
        i = 0
        for splitFile in splitList:
            
            args = []
            args.append('globus')
            args.append('transfer')
            if verify_checksum is False:
                args.append('--no-verify-checksum')
            else:
                args.append('--verify-checksum')
            
            
            args.append( '--label' )
            args.append( os.path.basename( splitFile ).split('.')[0]  )

            if sync_level:
                args.append('--sync-level')
                args.append( sync_level )

            if deadline:
                args.append('--deadline')
                args.append( deadline )

            args.append( self.sourceID + ':/' )
            args.append( self.destID + ':/' )
            args.append( '--batch' )

            print( ' '.join(args) )
            with open( splitFile, 'r' ) as stdinFile:
                subproc = subprocess.Popen( args, stdin=stdinFile, \
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE ) 

            subproc.wait()
            output = subproc.stdout.read() + subproc.stderr.read()

            if subproc.returncode != 0:
                print( output )
                raise RuntimeError('Globus subprocess failed with retcode {}'.\
                    format( subproc.returncode ) )
            # Extract the submission IDs from stdout
            startLine = 'Task ID: '
            startIdx = output.find( startLine )
            submitID = output[ startIdx + len(startLine):].strip()

            self._submitIDs.append(submitID)

            i += 1
 
    def wait( self ):
        '''
        Wait for all of the Globus transfers in self's submitID list to complete.
        ARGUMENTS:
            None
        '''
    
        if len( self._submitIDs ) == 0:
            raise RuntimeError('No Globus transfers have been submitted.')

        for id in self._submitIDs:
            args = ['globus', 'task', 'wait', id ]
            output = subprocess.check_output( args ) 
            
