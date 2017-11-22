import argparse
import logging
import sys, os, errno
import pickle
import subprocess
from mpi4py import MPI

FORMAT='%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s'
dateformat='%d-%m-%Y:%H:%M:%S'
progName=''


#==========================================================================

def submitTransfer( transferList, remoteID, hostID, hashDir, summaryLog, numTransfer=2 ):
    '''
    DESCRIPTION:
        This function submits for Globus transfer the files located in the transferList text file.
        The first entry in each line is the remote path, the second entry is the host path (including file name).
        This function calls the Globus Python CLI, so the current environment must have visibility to that CLI.
        After the transfers complete, it makes a hash of each file and compares it to the hashes in hashDir.
        If they differ, it attempts to download the files once more, repeating the hash check. If they differ
        a second time, the file is marked as failed.
    ARGUMENTS:
        transferList (str)      -- Path to the text file containing all files to transfer.
        remoteID (str)          -- The Globus ID for the remote endpoint.
        hostID (str)            -- The Globus ID for the host endpoint.
        hashDir (str)           -- Directory where the hash files are.
        numTransfer (int)       -- The number of concurrent Globus transfer requests to make.
        summaryLog (str)        -- Where high-level log information goes.
    EFFECTS:
        Creates temporary text file. Submits numTransfer number of Globus transfer requests. Waits
        until all transfers complete.
    RETURN:
        Undefined.
    '''

    if numTransfer < 1:
        raise ValueError("numTransfer can't be less than 1!")

    # Get a logger
    logger = logging.getLogger( progName + "_xfer" )
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logFormatter = logging.Formatter(FORMAT, dateformat)
    ch.setFormatter(logFormatter)
    logger.addHandler(ch)

    # Find number of lines in transferList
    with open( transferList, 'r' ) as f:
        numLines = sum(1 for line in f)

    logger.debug("Number of lines in transferList: {}".format(numLines))

    # Find how to split the transferList
    linesPerPartition = numLines / numTransfer
    extra   = numLines % numTransfer

    logger.debug("linesPerPartition: {}".format(linesPerPartition))
    logger.debug("extra: {}".format(extra))

    splitList = []
    # transferPath is path of transferList. We will store our temporary text files in the same place
    transferPath = os.path.dirname( os.path.abspath(transferList) )

    with open( transferList, 'r' ) as tList:
        for i in xrange(numTransfer):
            # Add the filename of our new split-file to the list
            splitList.append( os.path.join( transferPath, progName + "_temp_list_{}.txt".format(i)) )
            logger.debug("Making new split file: {}".format(splitList[i]))

            with open( splitList[i], 'w' ) as partition:
                logger.debug("Writing {}".format( os.path.basename(splitList[i])))
                for j in xrange( linesPerPartition ):
                    partition.write( tList.readline() )

        # Write the extra files to the last partition
        with open( splitList[numTransfer-1], 'a') as partition:
            for j in xrange( extra ):
                line = tList.readline()
                logger.debug("Writing extra line to {}".format( os.path.basename(splitList[numTransfer-1])))
                partition.write( line )

    # We have finished splitting up the transferList file. We can now submit these to Globus to bring down
    
            

    
    
#===========================================================================

def main():
    
    parser = argparse.ArgumentParser(description="This script handles pulling data from a remote Globus endpoint onto this \
    host machine. It uses the official Globus Python CLI with a key addition: Data is downloaded with Globus checksumming \
    turned off. This script performs its own checksumming and then compares it with the passed checksum. If they differ, it \
    attempts to download the file again, repeating the checksum a second time. If it fails again, the file is marked as corrupt \
    in the BasicFusion workflow logs.")
    parser.add_argument("LOG_TREE", help="Path to a Python Pickle containing the workflow log tree. This Pickle is a \
        Python dict that contains the following keys: 'summary', 'globus_pull', 'misc', 'process', 'globus_push'. \
        The value for each key is the absolute path to the respective log directory.", type=str)
    parser.add_argument("TRANSFER_LIST", help="This is a text file where each line contains FIRST an absolute path of the \
        file on the remote machine to transfer, and then SECOND the absolute path on the host machine where the file will be \
        transferred to (including file name), separated by a single space.", type=str )
    parser.add_argument("REMOTE_ID", help="The remote Globus endpoint.", type=str )
    parser.add_argument("HOST_ID", help="The host Globus endpoint (the machine running this script).", type=str)
    parser.add_argument("HASH_DIR", help="Directory where the hashes for all orbits reside. This directory has subdirectories \
        for each year, named after the year. In each of those subdirectories, there are hash files for each orbit of that year \
        named #hash.md5 where # is the orbit number.", type=str)
    parser.add_argument("SUMMARY_LOG", help="The summary log file where high-level log information will be saved.", \
        type=str )

    parser.add_argument("PROG_NAME", help="Unique identifier for this job. Used to create unique log file names and to label \
        Globus transfers.", default="globus_pull")
    parser.add_argument("-g", "--num-transfer", help="The number of Globus transfer requests to split the TRANSFER_LIST into. \
        Defaults to 2.", type=int, default=2)
    parser.add_argument("-l", "--log", help="Set the log level. Allowable values are INFO, DEBUG. Absence of this parameter \
        sets debug level to WARNING.", type=str, choices=['INFO', 'DEBUG' ] , default="WARNING")

    args = parser.parse_args()
   
    # Extract the pickle
    print(args.LOG_TREE)
    with open( args.LOG_TREE, 'rb' ) as f :
        logDirs = pickle.load(f)

    # Define log level
    if args.log == 'DEBUG':
        ll = logging.DEBUG
    elif args.log == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING
     
    #
    # ENABLE LOGGING
    #

    logFormatter = logging.Formatter(FORMAT, dateformat)
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler( os.path.join( logDirs['globus_pull'], args.PROG_NAME + '.log'), mode='w' )
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    rootLogger.setLevel(ll)
 
    global progName 
    progName = args.PROG_NAME

    logger = logging.getLogger(progName)

    #
    # SUBMIT GLOBUS TRANSFERS
    #
    submitTransfer( args.TRANSFER_LIST, args.REMOTE_ID, args. HOST_ID, args.HASH_DIR, args.SUMMARY_LOG, args.num_transfer)
    
if __name__ == '__main__':
    main()
