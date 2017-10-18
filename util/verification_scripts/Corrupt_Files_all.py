"""
@author : Shashank Bansal
@email : sbansal6@illinois.edu

Date : June 22, 2017

Modified: Oct 16, 2017
    Landon Clipp
    Added MPI, restructured code for more resiliance.
"""

import subprocess
import os
import sys
import argparse
import numpy as np
from mpi4py import MPI
from BFfile import isBFfile

# MPI VARIABLES
mpi_comm = MPI.COMM_WORLD
mpi_rank = MPI.COMM_WORLD.Get_rank()
mpi_size = MPI.COMM_WORLD.Get_size()
        

def findJobPartition( numProcess, numTasks):
    """         findJobPartition()
    
    DESCRIPTION:
        This function determines which MPI jobs will be responsible for how many orbits. It loops through the
        list "orbits" and increments each element in the "processBin" list, up to len(orbits) number of orbits.
    
    ARGUMENTS:
        numProcess (int)        -- The number of MPI processes (i.e. the number of bins to fill with orbits)
        numTasks                -- Number of individual tasks to perform

    EFFECTS:
        None

    RETURN:
        Returns a list of size numProcess. Each element in the list tells how many orbits each MPI process will
        handle.
    """

    processBin = [ 0 ] * numProcess     # Each element tells which process how many orbits it should handle
    
    pIndex = 0

    # Loop through all the orbits to fill up the processBin
    for i in xrange(0, numTasks ):
        if ( pIndex == numProcess ):
            pIndex = 0

        processBin[pIndex] = processBin[pIndex] + 1
        pIndex = pIndex + 1

    return processBin

#=========================================================================

def isCorrupt( filePath, subProc_call, corrupt_if_less = -1):
    """
    DESCRIPTION:
        This function determines whether or not the file in filePath is corrupt according to whatever command-line
        utility is passed into subProc_call. It will also mark the file as corrupt if filePath has a size less than
        corrupt_if_less.
    ARGUMENTS:
        filePath (str)          -- Path to the HDF file
        subProc_call (list)     -- List containing the command-line utility to perform the corruption check with.
                                   Each element is a single command-line argument. For instance:
                                   [ 'util_name', 'arg1', 'arg2' ]
        corrupt_if_less         -- If the file has a size less than this number in bytes, it will be marked as corrupt
    EFFECTS:
        None
    Return:
        0 if not corrupt.
        Non-zero if corrupt.
    """

    if not isinstance(subProc_call, list):
        raise ValueError("Rank {}: subProc_call argument must be a list!".format(mpi_rank))
 
    # Perform ncdump on the file
    dump = subprocess.Popen(
        subProc_call,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    # Get the output and return codes
    streamdata = dump.communicate()
    ret_code = dump.returncode

    if corrupt_if_less > 0.0: 
        hdfinfo = os.stat(filePath)

        # If the file size is below the given threshold, mark it as corrupt
        if (hdfinfo.st_size < corrupt_if_less ):
            return 1

    # Do a string search for 'HDP ERROR' if the return code is 0.
    # We do this when 0 just for a redundancy check. If non-zero,
    # we already know that file is corrupt.
    if ret_code != 0:
        return 1
    # Only do HDP ERROR check if we are running the hdp utility!
    elif 'hdp' in subProc_call[0]:
        check = np.array(streamdata)
        for i in range(len(check)):
            if 'HDP ERROR' in check[i]:
                return 1

    return 0

#===========================================================================


def worker():
    """
    DESCRIPTION:
        This is the worker function for all of the MPI processes that performs the corruption checks.
        Will send a Python list of corrupted files back to the master process.
    ARGUMENTS:
        None
    EFFECTS:
        MPI send command back to master.
    RETURN:
        None
    """

    # Wait for the master to send our job list
    print("Rank {}: Receiving job...".format(mpi_rank))
    fileList = mpi_comm.recv( source=0 )
    print("Rank {}: Job received.".format(mpi_rank))

    corruptList = []
    hdpCall = ["hdp", "dumpsds", "-h", "[file path]" ]
    ncCall  = ["ncdump", "-h", "[file path]"]

    # Perform the corruption checks on the file list
    for i in xrange( len(fileList) ):
        # Report every 100 files
        if (i % 100) == 0:
            print("Rank {}: on i = {} of {}".format(mpi_rank, i, len(fileList) ))

        if fileList[i][1] > 0:
            hdpCall[3] = fileList[i][0]
            corrupt = isCorrupt( fileList[i][0], hdpCall )
        elif fileList[i][1] == 0:
            ncCall[2] = fileList[i][0]
            corrupt = isCorrupt( fileList[i][0], ncCall, 1000000 )
        else:
            raise ValueError("Rank {}: fileList has a bad element! File marked as < 0!".format(mpi_rank))

        if corrupt:
            corruptList.append( [ fileList[i][0], fileList[i][1] ] )

    print("Rank {}: Sending back data...".format(mpi_rank))
    # Done checking all of the files. Time to send corrupt list back to master.
    mpi_comm.send( corruptList, dest=0 )

#==============================================================================


def master( terraDir, outDir ):
    """
    DESCRIPTION:
        This is the function for the master MPI rank. It recursively searches the terraDir directory
        for "proper" Basic Fusion input files (according to the isBFfile module) and distributes the
        files amongst the MPI processes. The MPI processes will then send the list of corrupted files
        back to the master process. Once master has collected all of these lists from the worker processes,
        it writes out the lists to log files in outDir.
    ARGUMENTS:
        terraDir        -- Directory to recursively search for HDF files.
        outDir          -- Directory where the output log files will be saved.
    EFFECTS:
        Creates log files.
    RETURN:
        None.
    """

    fileList = []
    corruptList = []
    numRunning = mpi_size - 1           # Keep track of how many mpi processes are still running
    logs = [ "MOPITTcorrupt.txt", "CEREScorrupt.txt", "MODIScorrupt.txt", "ASTERcorrupt.txt", "MISRcorrupt.txt" ]
    logFileObj = []

    # Recurse through all the files in terraDir
    for root, directories, filenames in os.walk( terraDir ):
        for filename in filenames:
            # Determine if the file is proper
            isProper = isBFfile( [ filename ] )
            
            # For your own reference, this is from the isBFfile docstring:
            # 
            #    -1 indicates that the file is not proper.
            #    0 indicates a proper MOPITT file match.
            #    1 indicates a proper CERES file match.
            #    2 indicates a proper MODIS file match.
            #    3 indicates a proper ASTER file match.
            #    4 indicates a proper MISR GRP file match.
            #    5 indicates a proper MISR AGP file match.
            #    6 indicates a proper MISR GMP file match.
            #    7 indicates a proper MISR HRLL file match.

            if isProper[0] != -1:
               # If it is, add the file to the list. First element is name, second is which file it belongs to.
               fileList.append( [ os.path.join(root, filename), isProper[0] ] )

    # We have all the files to process for this directory. Find how to split the jobs
    print("Rank {}: Finding job partition...".format(mpi_rank))
    processBin = findJobPartition( mpi_size - 1, len(fileList) )
    print("Rank {}: Done finding job partition.".format(mpi_rank))
    sIdx = 0
    eIdx = 0
    # Send the jobs to the worker processes
    print("Rank {}: Sending jobs...".format(mpi_rank))
    for i in xrange( 1, mpi_size ):
        # Get the process end index
        eIdx = processBin[i-1]
        mpi_comm.send( fileList[sIdx:sIdx + eIdx], dest=i )
        sIdx=eIdx
    print("Rank {}: Done sending jobs.".format(mpi_rank))


    # Wait for the returning data from each process
    print("Rank {}: Waiting for data...".format(mpi_rank, i))
    for i in xrange( 0, mpi_size-1 ):
        corruptSubList = mpi_comm.recv()
        corruptList = corruptList + corruptSubList
        print("Rank {}: Received {} lists.".format(i+1))

    # Open the log files for writing
    for i in xrange(len(logs)):
        logFileObj.append( open(logs[i], 'a') )

    # Distribute all of these into output log files
    for line in corruptList:
        if line[1] == 0:
            logFileObj[0].write( line[0] )
        elif line[1] == 1:
            logFileObj[1].write( line[0] )
        elif line[1] == 2: 
            logFileObj[2].write( line[0] )
        elif line[1] == 3:
            logFileObj[3].write( line[0] )
        elif line[1] >= 4 or line[1] <= 7:
            logFileObj[4].write( line[0] )
        else:
            raise ValueError("Passed line has a bad identifier!")

    # Close the log files
    for i in xrange(len(logs)):
        logFileObj[i].close()

#====================================================================================

def main():

    parser = argparse.ArgumentParser(description="This script checks the Basic Fusion HDF files in TERRA_DIR for corruption. This is done by running \"hdp dumpsds\" on all HDF4 files, and \"ncdump\" on all HDF5 files. Only the files determined to be \"proper\" by the isBFfile module are checked for corruption.")

    parser.add_argument("TERRA_DIR", help="The directory to investigate for Basic Fusion files.", type=str)
    parser.add_argument("OUT_DIR", help="The output directory where all files determined to be corrupt will be saved. The log files produced will be MOPITTcorrupt.txt, CEREScorrupt.txt, ASTERcorrupt.txt, MISRcorrupt.txt, and MODIScorrupt.txt.", type=str)
    args = parser.parse_args()


    assert (mpi_size > 1),"This script needs more than 1 MPI process to run!"

    # Master process needs to determine how to split the processes 
    if mpi_rank == 0:
        master( args.TERRA_DIR, args.OUT_DIR )    
    else:
        worker()
        
main()
