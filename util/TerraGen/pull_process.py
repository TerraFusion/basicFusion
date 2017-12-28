import argparse
import logging
import sys, os, errno
import pickle
import subprocess
from subprocess import PIPE
from mpi4py import MPI
import hashlib
from basicFusion.orbit import orbitYear
import tarfile
import shutil
import workflowClass
import re
import time

FORMAT            = '%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s'
dateformat        = '%d-%m-%Y:%H:%M:%S'
progName          = ''
globPullLog       = ''
summaryLog        = ''
logDirs           = {}
genFusInput       = ''
MODIS_MISSING     = None
logger            = None
hashDir           = None
STAGGER_HASH_JOBS = True    # Set to true to make master rank sleep 0.01 seconds before sending out
                            # a hash job.

#MPI Variables
mpi_comm = MPI.COMM_WORLD
mpi_rank = MPI.COMM_WORLD.Get_rank()
mpi_size = MPI.COMM_WORLD.Get_size()
rank_name = "MPI_Rank {}".format(mpi_rank)

MPI_MASTER              = 0
MPI_SLAVE_IDLE          = 0
MPI_DO_HASH             = 1
MPI_DO_UNTAR            = 2
MPI_SLAVE_HASH_COMPLETE = 3
MPI_SLAVE_HASH_FAIL     = 4
MPI_SLAVE_HASH_SUCCEED  = 5
MPI_SLAVE_TERMINATE     = 6
MPI_UNTAR_FAIL          = 7
MPI_DO_COPY             = 8
MPI_DO_GENERATE         = 9

#============================================================================================
    
def mpi_rank_hash( data ):
    '''
    DESCRIPTION:
        MPI ranks will call this function to perform a hash on a tar file.
    ARGUMENTS:
        data (granule object) -- A granule object that contains attributes describing
                                 the destination granule location
    EFFECTS:
        None
    RETURN:
        Updates data.hash_status with MPI_SLAVE_HASH_FAIL or MPI_SLAVE_HASH_SUCCEED.
        Sends data back to master rank with tag MPI_SLAVE_HASH_COMPLETE.
    '''

    # Find the appropriate hash file. First extract the orbit from the file name
    year = int(data.orbit_start_time[0:4])

    logger.info("Hashing orbit {}".format(data.orbit))
    # We now have the year of the orbit. We have enough information to find the hash file
    hashFile = os.path.join( hashDir, str(year), str(data.orbit) + "hash.md5" )

    hasher = hashlib.md5()
    with open( data.destTarPath, 'rb' ) as tarFile:
        buf = tarFile.read( hashReadSize )
        while buf:
            hasher.update(buf)
            buf = tarFile.read( hashReadSize )

    # Get the actual hash of the tar file
    hashHex = hasher.hexdigest()

    # Read the stored hash value
    with open( hashFile, 'r' ) as hashFile:
        storedHash = hashFile.read()

    if storedHash != hashHex:
        logger.debug("Hashes differ! Stored: {} Generated: {}".format(storedHash, hashHex))
        data.hash_status = MPI_SLAVE_HASH_FAIL
        mpi_comm.send( data, dest=MPI_MASTER, tag=MPI_SLAVE_HASH_COMPLETE)
    else:
        logger.debug("Hashes match.")
        data.hash_status = MPI_SLAVE_HASH_SUCCEED
        mpi_comm.send( data, dest=MPI_MASTER, tag=MPI_SLAVE_HASH_COMPLETE)

#============================================================================================
def mpi_rank_untar( data ):
    '''
    DESCRIPTION:
        MPI ranks will call this function to untar tar files. The tar file is described by
        data.destTarPath, and the untarring location is described by data.untarDir.
    ARGUMENTS:
        data (granule object) -- Granule object whose attributes contains tar location
                                 and the untar directory. Note that data.untarDir must already
                                 exit.
    EFFECTS:
        Untars tar file at data.untarDir.
        Deletes tar file when untarring has succeeded.
    RETURN:
        mpi.send the data object to master rank with tag MPI_UNTAR_FAIL upon failure.
        None upon success.
    '''
    untarDir = data.untarDir
    logger.info("Untarring {}".format( os.path.split( data.destTarPath )[1] ) )

    try:
        tar = tarfile.TarFile( data.destTarPath )

        # Get all the members (stored files) of the tar file
        tarMembers = tar.getmembers()

        # For every member, extract it and destroy its directory structure
        for member in tarMembers:
            member.name = os.path.basename(member.name)
            tar.extract( member, data.untarDir )

    except Exception as e:
        logger.error("Failed to untar: {}".format(data.destTarPath))
        logger.error(str(e))
        mpi_comm.send( data, MPI_MASTER, tag = MPI_UNTAR_FAIL )

    # Now delete the tar file
    os.remove( data.destTarPath )

#==============================================================================================
   
def mpi_rank_generate( data ):
    '''
    DESCRIPTION:
        MPI ranks will call this function to generate a single Basic Fusion granule.
    ARGUMENTS:
        data (granule object)   -- The granule object that contains all of the metadata necessary to generate
                                   the BF granule.
    EFFECTS:
        Creates an input file list for the Basic Fusion program at data.inputFileList.
        Generates a Basic Fusion granule at data.outputFilePath.
    RETURN:
        None
    '''


    # Need to query genFusionInput.sh to make input file. Then,
    # call basic fusion program
    # Make the basic fusion input file list
  
    
    logger.debug("Generating BF input file for orbit {}".format(data.orbit))
    args = [ genFusInput, data.untarDir, data.orbit, data.inputFileList, '--dir' ]
    with open( data.logFile, 'a+' ) as logFile:
        retCode = subprocess.call( args, stdout=logFile, stderr=logFile )
    
    if retCode != 0:
        with open( summaryLog, 'a') as sumLog:
            sumLog.write('{} input file generation error.'.format( data.orbit ))
        logger.error("Orbit {} failed to process input file list.".format( data.orbit) )
  
    # Try to remove the output file if it exists. This is to prevent the BF code from breaking
    # if the file already exists.
    try:
        os.remove( data.outputFilePath )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

    logger.debug("Generating BF granule orbit {}".format(data.orbit))
    with open( data.logFile, 'w' ) as logFile:
        args =  [ data.BF_exe, data.outputFilePath, data.inputFileList, data.orbit_times_bin ] 
        retCode = subprocess.call( args, stdout=logFile, stderr=logFile ) 
    if retCode != 0:
        with open( summaryLog, 'a' ) as f:
            print("{} granule generation error.".format( data.orbit))
        logger.error("Orbit {} failed to generate granule.".format(data.orbit))

    # Delete the untarred input data. We don't need it anymore.
    shutil.rmtree( data.untarDir )

#=========================================================================

def mpi_rank_copy( data ):
    '''
    DESCRIPTION:
        MPI ranks will call this function to copy a list of files to a certain directory.
    ARGUMENTS:
        data    -- List of tuples. Each element i contains:
                    i[0]: File to copy
                    i[1]: Destination file or directory
    EFFECTS:
        Makes copy of files to new directory
    RETURN:
        None
    '''

    logger.info("Copying data.")
    for i in data:
        shutil.copy( i[0], i[1] )

    logger.info('Copying missing MODIS files.')

#=========================================================================

def mpi_rank_terminate( data ):
    '''
    DESCRIPTION:
        This function terminates the MPI rank.
    ARGUMENTS:
        data    -- Not used
    EFFECTS:
        Sends sys.exit() to interpreter.
    RETURN:
        None
    '''

    logger.debug("Terminating.")
    sys.exit()    
 
#=========================================================================
def worker():
    '''
    DESCRIPTION:
        This is the worker function for all MPI ranks designated as a slave (i.e., != 0).
    '''

    
    # I was running into issues when reading the entire tar file into memory. Instead,
    # let us read hashReadSize bytes of the tar file at a time and update the hash
    # incrementally.
    hashReadSize = 100000000

    while True:
        logger.debug("Waiting to receive job")
        stat = MPI.Status()
        mpi_comm.send( 0, dest = MPI_MASTER, tag = MPI_SLAVE_IDLE )
        data = mpi_comm.recv( source = MPI_MASTER, status=stat )
        func = data[0]
        data = data[1]

        logger.info("Received job {}.".format( func ) )

        func( data )


#==========================================================================
def slaveCopyFile( copyList, orbit=0 ):
    '''
    DESCRIPTION:
        Handles sending out a file copying job to a free slave rank.
    ARGUMENTS:
        copyList (list) -- Each element is a tuple. First element in tuple is
                           the file to be copied. Second element in tuple is the
                           directory to copy the file to.
        orbit (int)     -- Optional argument. Specify this if you want a useful
                           logging message to be sent.
    EFFECTS:
        Copies all files in copyList
    RETURN:
        None
    '''

    # Check for slaves that want a job
    stat = MPI.Status()                 # Class that keeps track of MPI information    
    mpi_comm.probe( status = stat, tag = MPI_SLAVE_IDLE )

    # Ensure the copyList contains real files and directories
    for i in copyList:
        if not os.path.isfile( i[0] ):
            raise ValueError("Passed file: {} does not exist.".format(i[0]))
        if not os.path.isdir( i[1] ):
            raise ValueError("Passed directory: {} does not exist.".format(i[1]))

    logger.debug("Sending slave {} copy job: {}.".format( stat.source, orbit ) )

    mpi_comm.recv( source=stat.source, tag = MPI_SLAVE_IDLE )
    mpi_comm.send( (mpi_rank_copy, copyList), dest = stat.source )



#==========================================================================
def globusDownload( granuleList, remoteID, hostID, numTransfer, retry=False ):

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
            line = '{} {}'.format( j.sourceTarPath, j.destTarPath )
            logger.debug("Writing extra line to {}".format( os.path.basename(splitList[numTransfer-1])))
            partition.write( line + os.linesep ) 

    # Do a sanity check to make sure we didn't lose any files in the process.
    # Sum the number of lines in all of our batch transfer files. If that does
    # not equal to the length of granuleList, we missed something.
    num_lines = 0
    for batch in splitList:
        num_lines += sum(1 for line in open( batch ) )

    if num_lines != len(granuleList):
        raise ValueError("num_lines ({}) != length of granuleList ({})".format(num_lines, len(granuleList)))

    # ================================
    # = SUBMIT SPLIT LISTS TO GLOBUS =
    # ================================

    # We have finished splitting up the transferList file. We can now submit these to Globus to download.
    # I admit that the way I'm calling the Globus CLI (i.e., not from within Python itself) is clunky, but
    # calling it through a subprocess works well enough. I don't have time to integrate their CLI into this
    # program.
    submitIDs = []
    # Open the log file so that subprocesses can write to it
    with open( globPullLog, 'a' ) as log:
        for splitFile in splitList:
            args = ['globus', 'transfer', CHECK_SUM, remoteID +':/', hostID + ':/', '--batch', \
                    '--label', os.path.basename(splitFile).replace('.txt', '') ]

            logger.info("Submitting Globus transfer batch {}.".format(splitFile))            
            with open( splitFile, 'r' ) as stdinFile:
                subProc = subprocess.Popen( args, stdin=stdinFile, stdout=PIPE, stderr=PIPE ) 
                subProc.wait()
            
            # We need to retrieve the stdout/stderr of this globus call to extract the submission ID.
            stdout = subProc.stdout
            stderr = subProc.stderr

            # stdout and stderr are files, so simply read the file contents into the variables themselves
            stdout = stdout.read()
            stderr = stderr.read()

            # Extract the submission IDs from stdout
            startLine = 'Task ID: '
            startIdx = stdout.find( startLine )
            submitID = stdout[ startIdx + len(startLine):].strip()

            submitIDs.append(submitID)

            # stdout and stderr are now strings. Concatenate the strings together
            stdout += stderr

            # Go ahead and write these to the log file
            log.write(stdout)

            retCode = subProc.returncode

            if retCode != 0:
                summaryLog.write("ERROR: Globus transfer failed! See {} for more details.\n".format(globPullLog))
                raise RuntimeError("Globus transfer failed.")
 
        # The transfers have been submitted. Need to wait for them to complete...
        for id in submitIDs:
            logger.info("Waiting for Globus task {}".format(id))
            args = ['globus', 'task', 'wait', id ]

            retCode = subprocess.call( args, stdout=log, stderr=log ) 
            
            # The program will not progress past this point until 'id' has finished
            if retCode != 0:
                summaryLog.write("Globus task failed with retcode {}! See: {}".format(retCode, globPullLog))

            logger.info("Globus task {} completed with retcode: {}.".format(id, retCode))

#=======================================================================================================

def hashCheck( granuleList ):

    # Now for the fun part. All of our files have been supposedly downloaded by Globus. We need to hash them
    # and compare them with the saved hash files. Split the files evenly amongst all the MPI ranks.
    # We use mpi_size - 1 because the master rank is not considered a slave.
    logger.info("Submitting the files for hashing.")
    hashing      = []           # List of lines currently being hashed
    succeedHash  = []           # List of lines that passed the hash check
    failedHash   = []           # List of lines that failed the hash check

    numFiles = len(granuleList)

    # Make a copy of granuleList. We want to delete elements in the list as they're successfully processed.
    newGranuleList = granuleList[:]
    num_orig = len(newGranuleList)

    # We will send the files to the slaves one-by-one. We define some global tags for the master-slave communication
    # so that they can effectively converse with each other.
    while len(newGranuleList) > 0 or len(hashing) > 0:

        # Check for slaves that want a job
        stat = MPI.Status()                 # Class that keeps track of MPI information

        # No more jobs to send. Only accept hash messages
        if len(newGranuleList) == 0:
            mpi_comm.probe( tag = MPI_SLAVE_HASH_COMPLETE, status = stat )
        
        # Else, accept messages from any tag
        else:
            mpi_comm.probe( status = stat )
            

        # A slave wants a job and we have more jobs to send.
        if stat.Get_tag() == MPI_SLAVE_IDLE:
            granule = newGranuleList.pop()
            
            # Grab the orbit from the transferLine for logging purposes

            logger.debug("Sending slave {} hash job: {}.".format( stat.source, granule.orbit ) )

            # There have been issues with this script causing network congestion. NCSA suspects it is
            # the hashing that does it. If global variable is set to true
            if STAGGER_HASH_JOBS:
                time.sleep(0.01)

            mpi_comm.recv( source=stat.source, tag = MPI_SLAVE_IDLE )
            mpi_comm.send( ( mpi_rank_hash, granule ), dest = stat.source )
            # Save this line in the hashing dict to keep track of what is currently being hashed
            hashing.append( granule )


        # Slave has finished hashing a file. Check its status.            
        elif stat.Get_tag() == MPI_SLAVE_HASH_COMPLETE:
            
            # worker will send back the granule it was given
            recvGranule = mpi_comm.recv( source=stat.source, tag = stat.Get_tag() )

            if recvGranule.hash_status == MPI_SLAVE_HASH_FAIL:

                # Remove this line from the hashing list
                found_elem=0
                for i in hashing:
                    if i.orbit == recvGranule.orbit:
                        hashing.remove(i)
                        found_elem=1
                if found_elem == 0:
                    raise RuntimeError("Failed to find proper element to remove in list.")

                # Append to the list of failed hashes
                failedHash.append( recvGranule )

            elif recvGranule.hash_status == MPI_SLAVE_HASH_SUCCEED:
                # Remove this line from the hashing list
                found_elem=0
                for i in hashing:
                    if i.orbit == recvGranule.orbit:
                        hashing.remove(i)
                        found_elem=1
                if found_elem == 0:
                    raise RuntimeError("Failed to find proper element to remove in list.")
                
                # Append to the list of successful hashes
                succeedHash.append( recvGranule )
            else:
                raise ValueError("Slave gave invalid number {} for hash status.".format( recvGranule.hash_status ) )

    logger.info("{} hashes failed, {} hashes successful.".format( len(failedHash), len(succeedHash)))

    # Do a sanity check to make sure no files were lost in this process
    if num_orig != len(failedHash) + len(succeedHash):
        errMsg="num_orig ({}) does not equal failed + succeed ({}) hashes!".format(num_orig,len(failedHash) + len(succeedHash))
        logger.error( errMsg )
        raise ValueError(errMsg )

    return failedHash

#===============================================================================

def submitTransfer( granuleList, remoteID, hostID, hashDir, numTransfer ):
    '''
    DESCRIPTION:
        This function submits for Globus transfer the files located in the transferList text file.
        The first entry in each line is the remote path, the second entry is the host path (including file name).
        This function calls the Globus Python CLI, so the current environment must have visibility to that CLI.
        After the transfers complete, it makes a hash of each file and compares it to the hashes in hashDir.
        If they differ, it attempts to download the files once more, repeating the hash check. If they differ
        a second time, the file is marked as failed.
    ARGUMENTS:
        granuleList (str)       -- A list of granule objects to process
        remoteID (str)          -- The Globus ID for the remote endpoint.
        hostID (str)            -- The Globus ID for the host endpoint.
        hashDir (str)           -- Directory where the hash files are.
        numTransfer (int)       -- The number of concurrent Globus transfer requests to make.
    EFFECTS:
        Creates temporary text file. Submits numTransfer number of Globus transfer requests. Waits
        until all transfers complete.
    RETURN:
        Undefined.
    '''

    if numTransfer < 1:
        raise ValueError("numTransfer can't be less than 1!")

    #====================
    # DEFAULT VARIABLES =
    #====================
    global globPullLog

    # Get a logger

    logger.info("Calling globusDownload")
    globusDownload( granuleList, remoteID, hostID, numTransfer ) 
    logger.info("globusDownload complete")

    # ===========================================
    # =               HASHING FUN               =
    # ===========================================

    logger.info("Calling hashCheck()")
    failedHash = hashCheck( granuleList )
    logger.info("hashCheck() complete.")

    
    batchPath = logDirs['misc']
    failedBatch = os.path.join( batchPath, progName + "_failedHash.txt" )

    if len(failedHash) > 0 :
        logger.info("Retrying the failed hashes")

        globusDownload( failedHash, remoteID, hostID, retry=True )

        failedHash = hashCheck( failedHash )

    # These failed hashes need to be logged
    if len(failedHash) > 0 :
        with open( failedBatch, 'w' ) as batchFile:
            for i in failedHash:
                batchFile.write(i.destTarFile)

        errMsg="Files failed hash. See {} for list of failed orbits.".format(failedBatch)
        logger.error(errMsg)
        summaryLog.write(errMsg)


#==========================================================================        
def untarData( untarList ):
    '''
    DESCRIPTION:
        This function takes as input a list where each element is a tuple, the first element
        of the tuple being the file to untar, and the second element being the directory to
        place the untarred data.
    ARGUMENTS:
        untarList (list)     -- List of granule objects. Each granule object will contain an
                                attribute destTarPath that contains the location of the tar file
                                to untar.
    EFFECTS:
        Creates new, untarred files.
    RETURN:
        List of elements in untarList that failed to untar.
    '''

    logger.info("Untarring input data.")
    failUntar = []

    idx = 0
    for i in untarList:
         
        # Check for slaves that want a job
        stat = MPI.Status()                 # Class that keeps track of MPI information

        mpi_comm.probe( status = stat )
        if stat.Get_tag() == MPI_UNTAR_FAIL:
            data = mpi_comm.recv( source = stat.Get_source() )
            logger.error("Rank {} failed to untar {}.".format( stat.Get_source(), data[0] ) )
            
        elif stat.Get_tag() == MPI_SLAVE_IDLE: 
            logger.info("Sending untar job {} to rank {}".format( os.path.split( i.destTarPath )[1], stat.Get_source()))        
            mpi_comm.recv( source=stat.Get_source(), tag = MPI_SLAVE_IDLE )
            mpi_comm.send( (mpi_rank_untar, i), dest = stat.Get_source() )
        else: 
            errMsg='Received unexpected tag from slave {}: {}'.format( stat.Get_source(), stat.Get_tag() )
            logger.error( errMsg )
            raise ValueError( errMsg )

    logger.info("All tar files processed. Waiting for slaves to finish.")

    # Wait for all MPI children to complete
    for i in xrange(1, mpi_size):
        stat = MPI.Status()
        data = mpi_comm.probe( source = i, status=stat)
        if stat.Get_tag() == MPI_UNTAR_FAIL:
            logger.error("Rank {} failed to untar {}.".format( stat.Get_source(), data[0] ) )
            
            # Log this in the failUntar list
            failUntar.append( data )

        elif stat.Get_tag() == MPI_SLAVE_IDLE:
            continue
        else:
            errMsg='Received unexpected tag from slave {}: {}'.format( i, stat.Get_tag() )
            logger.error( errMsg )
            raise ValueError( errMsg )

    return failUntar

#=========================================================================
def MPI_wait():
    
    # Wait for all MPI children to complete
    for i in xrange(1, mpi_size):
        stat = MPI.Status()
        data = mpi_comm.probe( source = i, status=stat)
        if stat.Get_tag() != MPI_SLAVE_IDLE :
            # We don't know how to handle this.
            raise StandardError("Discovered tag: {} while trying to wait for slave.".format( stat.Get_tag() ))


#==========================================================================


def findFile( name, path ):
    '''
    DESCRIPTION:
        Recursively searches path for a file.
    ARGUMENTS:
        name (str)  -- Name of the file
        path (str)  -- The directory to search for name
    EFFECTS:
        None
    RETURN:
        Absolute path of file if found.
        None if not found
    '''
    name = name.strip()
    path = path.strip()
    for root, dirs, files in os.walk( path ):
        if name in files:
            return os.path.abspath( os.path.join(root, name) )

#===========================================================================

def findOrbitStart( orbit, orbit_info ):
    '''
    DESCRIPTION:
        This function finds the starting time of the orbit according to the Orbit_Info.txt file. Please
        see the GitHub documentation on how to generate this file.
    ARGUMENTS:
        orbit (int) -- Orbit to find starting time of
        orbit_info (str)    -- Path to the orbit_info.txt file
    EFFECTS:
        None
    RETURN:
        String containing the starting time.
    '''

    with open( orbit_info, 'r') as file:
        for line in file:
            if re.search( "^{}".format(orbit), line ):
                match=line
                break

    if match == None:
        return None
    
    match = match.split(' ')[2].strip()
    match = match.replace('-','')
    match = match.replace('T','')
    match = match.replace(':','')
    match = match.replace('Z','')

    return match
 

#===========================================================================

def slaveProcessBF( data ): 
    '''
    DESCRIPTION:
        Handles sending out a Basic Fusion generation job to a free slave rank.
    ARGUMENTS:
        data    -- A tuple that contains the data necessary for the slave to process
                   Basic Fusion (see: worker() function for more details)
    EFFECTS:
        Generates BF file.
    RETURN:
        None
    '''

    # Check for slaves that want a job
    stat = MPI.Status()                 # Class that keeps track of MPI information    
    mpi_comm.probe( status = stat, tag = MPI_SLAVE_IDLE )
    logger.debug("Sending slave {} generate job: {}.".format( stat.source, data.orbit ) )

    mpi_comm.recv( source=stat.source, tag = MPI_SLAVE_IDLE )
    mpi_comm.send( (mpi_rank_generate, data ), dest = stat.source )

#===========================================================================
 
def generateBF( granuleList ):
    '''
    DESCRIPTION:
        This function generates the Basic Fusion files.
    ARGUMENTS:
        granuleList     -- A list of granule objects to process
        genFusionInput     -- Path to the genFusionInput.sh script
        orbit_info_bin      -- Path to the orbit_info binary file
        orbit_info_txt      -- Path to the orbit_info text file
        BF_exe              -- Path to the basic fusion executable
    '''

 
    for granule in granuleList:
        slaveProcessBF( granule )

    
    # Wait for all slaves to finish computation
    MPI_wait()

#===========================================================================

def main():
    
    parser = argparse.ArgumentParser(description="This script handles pulling data from a remote Globus endpoint onto this \
    host machine. It uses the official Globus Python CLI with a key addition: Data is downloaded with Globus checksumming \
    turned off. This script performs its own checksumming and then compares it with the passed checksum. If they differ, it \
    attempts to download the file again, repeating the checksum a second time. If it fails again, the file is marked as corrupt \
    in the BasicFusion workflow logs.")
    parser.add_argument("LOG_TREE", help="Path to a Python Pickle containing the workflow log tree. This Pickle is a \
        Python dict that contains the following keys: 'summary', 'pull_process', 'misc', 'process', 'globus_push'. \
        The value for each key is the absolute path to the respective log directory.", type=str)
    parser.add_argument("TRANSFER_LIST", help="This is a text file where each line contains FIRST an absolute path of the \
        file on the remote machine to transfer, and then SECOND the absolute path on the host machine where the file will be \
        transferred to (including file name), separated by a single space.", type=str )
    parser.add_argument("SCRATCH_SPACE", help="A directory where this program is allowed to create and delete files and \
        directories as it sees fit.", type=str)
    parser.add_argument("REMOTE_ID", help="The remote Globus endpoint.", type=str )
    parser.add_argument("HOST_ID", help="The host Globus endpoint (the machine running this script).", type=str)
    parser.add_argument("HASH_DIR", help="Directory where the hashes for all orbits reside. This directory has subdirectories \
        for each year, named after the year. In each of those subdirectories, there are hash files for each orbit of that year \
        named #hash.md5 where # is the orbit number.", type=str)
    parser.add_argument("SUMMARY_LOG", help="The summary log file where high-level log information will be saved.", \
        type=str )
    parser.add_argument("JOB_NAME", help="A name for this  job. Used to make unique log files, intermediary files, and any \
        other unique identifiers needed.", type=str)
    parser.add_argument("LOG_FILE", help="The text file to send all log output to.", type=str )
    parser.add_argument("MISR_PATH_FILES", help="Directory where all the MISR HRLL and AGP files are stored. Directory \
        structure does not matter, script will perform recursive search for the files.", type=str )
    parser.add_argument("OUT_GRAN_LIST", help="The output granule list. This file will be a Python pickle of a list of \
        granule objects produced by this workflow.", type=str )
    parser.add_argument("-g", "--num-transfer", help="The number of Globus transfer requests to split the TRANSFER_LIST into. \
        Defaults to 1.", dest='num_transfer', type=int, default=1)
    parser.add_argument("-l", "--log-level", help="Set the log level. Allowable values are INFO, DEBUG. Absence of this parameter \
        sets debug level to WARNING.", dest='log', type=str, choices=['INFO', 'DEBUG' ] , default="WARNING")
    parser.add_argument('--include-missing', help='Path to the missing MODIS files, where passed directory contains subdirectories named \
        after each orbit. Each of those subdirectories contains the MODIS files to include in the final BF granule.', type=str, \
        dest='modis_missing' )
    group_ex = parser.add_mutually_exclusive_group()
    group_ex.add_argument('--only-hash', help='Only perform the hash check.', dest='ONLY_HASH', action='store_true')
    group_ex.add_argument('--only-download', help='Only download the files from the source Globus endpoint.', dest='ONLY_DOWNLOAD', \
        action='store_true')

    args = parser.parse_args()
  
    global logDirs 
    # Extract the pickle
    with open( args.LOG_TREE, 'rb' ) as f :
        logDirs = pickle.load(f)

    # Define log level
    if args.log == 'DEBUG':
        ll = logging.DEBUG
    elif args.log == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING
    
    # Argument sanitizing
    args.LOG_FILE      = os.path.abspath( args.LOG_FILE )
    args.HASH_DIR      = os.path.abspath( args.HASH_DIR )
    args.LOG_TREE      = os.path.abspath( args.LOG_TREE )
    args.OUT_GRAN_LIST = os.path.abspath( args.OUT_GRAN_LIST )
    # --------------------
    # - USEFUL VARIABLES -
    # --------------------    
    input_list_dir      = os.path.join( logDirs['misc'], 'BFinputListing' )
    FILENAME            = 'TERRA_BF_L1B_O{0}_{1}_F000_V000.h5'
    scriptPath          = os.path.realpath(__file__)
    projPath            = ''.join( scriptPath.rpartition('basicFusion/')[0:2] )
    global genFusInput
    genFusInput         = os.path.join( projPath, "metadata-input", "genInput", "genFusionInput.sh" )
    orbit_info_txt      = os.path.join( projPath, "metadata-input", "data", "Orbit_Path_Time.txt" )
    orbit_info_bin      = os.path.join( projPath, "metadata-input", "data", "Orbit_Path_Time.bin" ) 
    BF_exe              = os.path.join( projPath, "bin", "basicFusion" )
    BFoutputDir         = os.path.join( args.SCRATCH_SPACE, "BFoutput" )
    try:
        os.makedirs( BFoutputDir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    global MODIS_MISSING
    MODIS_MISSING = args.modis_missing

    # Make the directory to store input file listings
    try:
        os.makedirs( input_list_dir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    #
    # ENABLE LOGGING
    #

    logFormatter = logging.Formatter(FORMAT, dateformat)
    rootLogger = logging.getLogger()

    global globPullLog
    globPullLog = args.LOG_FILE
    fileHandler = logging.FileHandler( args.LOG_FILE, mode='a' )
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    rootLogger.setLevel(ll)

    global progName 
    progName=args.JOB_NAME

    global summaryLog
    global hashDir
    hashDir = args.HASH_DIR
    
    if mpi_rank != 0:
        worker( )
        return
    
    # Check that our useful variables exist in the file system
    if not os.path.isdir( projPath ):
        errMsg="The project path: {} does not exist!".format(projPath)
        logger.critical(errMsg)
        raise RuntimeError( errMsg )
    if not os.path.isfile( genFusInput ):
        errMsg="The genFusionInput.sh script was not found at: {}".format(genFusInput)
        logger.critical(errMsg)
        raise RuntimeError( errMsg )
    if not os.path.isfile( orbit_info_txt ):
        errMsg="The orbit time file was not found at: {}".format( orbit_info_txt )
        logger.critical(errMsg)
        raise RuntimeError( errMsg )

    if not os.path.isfile( orbit_info_bin ):
        errMsg="The orbit time binary file was not found at: {}".format( orbit_info_bin )
        logger.critical(errMsg)
        raise RuntimeError( errMsg )

    if not os.path.isfile( BF_exe ):
        errMsg="The basic fusion executable was not found at: {}".format( BF_exe)
        logger.critical(errMsg)
        raise RuntimeError( errMsg )

    if args.num_transfer < 1:
        raise ValueError("--num-transfer argument can't be less than 1!")


    
    # Open the summary log as a simple file
    with open( args.SUMMARY_LOG, 'a' ) as sumLog:
        summaryLog = sumLog 
        # Prepare the granule list
         
        granuleList = []
        # The tar data is now (hopefully) all on the system. Untar the data.
        with open( args.TRANSFER_LIST, 'r' ) as tList:
            for i in tList:
                # Name of the tar file
                sourceTarPath = i.split(' ')[0].strip()
                destTarPath   = i.split(' ')[-1].strip()
                orbit = os.path.split( sourceTarPath )[1].strip()
                orbit = orbit.replace('archive.tar', '')
                year = orbitYear( int(orbit) )

                # the start time of the orbit
                orbit_start_time = findOrbitStart( orbit, orbit_info_txt )


                # BFdirStructure is the year/month/day path inside the output directory for this particular granule
                BFdirStructure= os.path.join( str( orbit_start_time[0:4]), \
                                str( orbit_start_time[4:6]), \
                                str( orbit_start_time[6:8] ) )

                # directory where the untarred data will go for this granule
                untarDir = os.path.join( args.SCRATCH_SPACE, "untarData", BFdirStructure, str(orbit) )
                
                # outFileDir is the complete, absolute directory to store the basic fusion file in
                outFileDir = os.path.join( BFoutputDir, BFdirStructure)

                # directory where the log file will reside
                yearLogDir = os.path.join( logDirs['process'], BFdirStructure )

                # log file for the granule generation
                logFile = os.path.join( yearLogDir, "{}process_log.txt".format( orbit ) )
                
                # location of the input file lsit for the BF granule
                inputFileList = os.path.join( input_list_dir, BFdirStructure, "{}input.txt".format( orbit ))
                
                # Name of the output basic fusion file
                BFfileName = FILENAME.format( orbit, orbit_start_time )
                
                # Finally, save the absolute path of the output file
                outFilePath = os.path.join( outFileDir, BFfileName )

                # Create our location for untarring
                if not os.path.isdir( untarDir ):
                    logger.debug("Creating untar location at: {}".format(untarDir))
                    os.makedirs( untarDir )

                # Make the log file directory
                try:
                    os.makedirs( yearLogDir )
                    logger.debug("Making process dir: {}".format(yearLogDir))
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
               
                # Make the input file list directory
                try:
                    os.makedirs( os.path.dirname( inputFileList ) )
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
 
                # Make the file output directory
                try:
                    os.makedirs( outFileDir )
                    logger.debug("Making the output directory: {}".format( outFileDir) )
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                
                #
                # PREPARE THE GRANULE ATTRIBUTES
                #
                curGranule = workflowClass.granule( destTarPath, untarDir, orbit, year=year )
                curGranule.sourceTarPath       = sourceTarPath
                curGranule.destTarPath         = destTarPath
                curGranule.orbit_start_time = orbit_start_time
                curGranule.pathFileList     = os.path.join( untarDir, "MISR_PATH_FILES_{}.txt".format(orbit) )
                curGranule.year             = year
                curGranule.BFoutputDir      = BFoutputDir
                curGranule.BFfileName       = BFfileName 
                curGranule.logFile          = logFile
                curGranule.inputFileList    = inputFileList
                curGranule.BF_exe           = BF_exe
                curGranule.orbit_times_bin  = orbit_info_bin                
                curGranule.outputFilePath   = outFilePath
                

                granuleList.append( curGranule )
        

        if args.ONLY_DOWNLOAD:
            logger.info('--only-download enabled.')
            logger.info("Calling globusDownload")
            globusDownload( granuleList, args.REMOTE_ID, args.HOST_ID, args.num_transfer ) 
            logger.info("globusDownload complete")
            return
      
        if args.ONLY_HASH:
            logger.info('--only-hash enabled.')
            logger.info('Calling hashCheck()')
            hashCheck(granuleList)
            logger.info('hashCheck() complete.')
            return

 
        # ===========================
        # = SUBMIT GLOBUS TRANSFERS =
        # ===========================
       
        logger.debug("Calling submitTransfer().") 
        submitTransfer( granuleList, args.REMOTE_ID, args. HOST_ID, args.HASH_DIR, args.num_transfer)
        logger.debug("submitTransfer() complete.")
        
        logger.info("Untarring data.") 
        failUntar = untarData( granuleList )
        logger.info("Untarring complete.")

        if len(failUntar) != 0:
            sumLog.write("Failed to untar the following files:")
        for i in failUntar:
            granuleList.remove(i)
            sumLog.write(i.tarFile)

        logger.info("Copying over MISR path files.")

        # ========================
        # = READ MISR_PATH_FILES =
        # ========================

        # For every untarred orbit in our granuleList, we need to read the MISR_PATH_FILES.txt
        # file and copy over those two files that it lists. granuleList is a list of granule
        # class objects. Each of these objects contains:
        # 1. Directory where all untarred data for a single orbit is stored
        # 2. Orbit of its directory
        # 3. The tar file where these untarred files were extracted from (at this point in the program,
        #    this tar file should be deleted. Files have been extracted.)

        for i in granuleList:
            pathCopyList = []
            with open( i.pathFileList, 'r' ) as pfList:
                for file in pfList:
                    # Find the absolute path of this pathfile
                    pathAbs = findFile( file, args.MISR_PATH_FILES )
                    if pathAbs == None:
                        errMsg="Failed to find file {} in path {}.".format(file, args.MISR_PATH_FILES)
                        logger.error(errMsg)
                        raise ValueError(errMsg)

                    pathCopyList.append( (pathAbs, i.untarDir) )

            if args.modis_missing:
                logger.info('Copying missing MODIS files for all orbits.')
                orbit_dir = os.path.join( args.modis_missing, str( i.orbit ) )
                
                for subdir, dirs, files in os.walk( orbit_dir ):
                    for file in files:
                        pathCopyList.append( ( os.path.join( subdir, file ),  i.untarDir ) )
                
            # Send this job to the worker ranks
            slaveCopyFile( pathCopyList, i.orbit )
                    

        # Wait for all slaves to complete
        MPI_wait()

        for granule in granuleList:
            slaveProcessBF( granule )

        
        # Wait for all slaves to finish computation
        MPI_wait()

        # Pickle the granuleList. This is done because I may make generateBF() a separate script, and would
        # need to pass around a pickle instead of the actual list.
        logger.info("Saving the input data list pickle at {}".format(args.OUT_GRAN_LIST))
        with open( args.OUT_GRAN_LIST, 'wb' ) as f:
            pickle.dump( granuleList, f )

 
if __name__ == '__main__': 
    logger = logging.getLogger( rank_name )
    try:
        main()
    except Exception as e:
        logger.exception( "Encountered exception." )
        mpi_comm.Abort()
    finally:     
        logger.info("Successful completion")
   
        if mpi_rank == 0:
            # We need to terminate all slaves, or else the entire process might hang!
            # mpi_comm.Abort()
            for i in xrange(1, mpi_size ):
                logger.debug("Terminating slave {}".format(i))
                mpi_comm.recv( source = i, tag = MPI_SLAVE_IDLE )
                mpi_comm.send( 0, dest = i, tag = MPI_SLAVE_TERMINATE )

