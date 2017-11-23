"""
This script is the final-ish workflow for the BasicFusion project. The workflow assumes as such:
1. This is being run on Blue Waters
2. The Basic Fusion input tar data is being stored on Nearline in the following directory format:
    2000
        archive1000.tar
        archive1001.tar
        ...
    2001
        archive...
    ...

3. The MISR HRLL and AGP files are tarred separately, such that all HRLL are in a single tar,
   and all AGP files are in a single tar.

The workflow takes as input a range of orbits to process, then splits the work as such:
1. Pull tar data from Nearline
2. Untar data
3. Hash data and compare with saved hash.
    a. If hashes differ, try to download file again.
    b. If hashes fail a second time, log as an error and skip orbit.
4. Generate input file list for BF program, then process orbit.
5. Submit resultant BF file to transfer back to Nearline.

These 5 steps are considered to be a quanta in the pipeline.
"""

__author__ = "Landon Clipp"
__email__  = "clipp2@illinois.edu"

import argparse
import logging
import sys, os, errno
from basicFusion import makeRunDir, orbitYear
from workflowClass import processQuanta
import pickle

#=================================
logName='BF_jobFiles'
logger = 0
#=================================

def makePBS_globus(transferList, PBSpath, remoteID, hostID, jobName, logDir, summaryLog, oLimits, picklePath):
    '''
    DESCRIPTION:
        This function makes the PBS scripts and other files necessary to pull the proper orbits from Nearline
        to the host machine. Calling function must pass in a list of files to transfer.
        Essentially, this is just some simple meta-programming. This makes use of the Globus Python Command 
        Line Interface.
    ARGUMENTS:
        transferList (list) -- A list containing a list of files to transfer from remote to the host machine.
                               Each element contains absolute path of single file on remote machine, then absolute
                               path of the file on the host machine, the paths delimited by a space. Note that BOTH
                               paths must include a filename!
        PBSpath (str)       -- An absolute path of a directory to where the PBS script and the transferList
                               text file will be created.
        remoteID (str)      -- The Globus identifier of the remote site to download the data from.
        hostID (str)        -- The Globus identifier of the host machine.
        jobName (str)       -- Serves two purposes: will define the prefix of the PBS script file, as well as
                               defining the name of the job to the PBS scheduler.
        logDir (str)        -- Directory where Globus CLI output will be redirected to.
        summaryLog (str)    -- This log file contains summary information for everything that goes wrong in this process.
        oLimits (list, int) -- A list containing the lower and upper bounds of these orbits.
        picklePath (str)    -- Path to the Python dictionary pickle that contains the paths of the log directories.
    EFFECTS:
        Creates a PBS file at PBSpath. Creates a text file at the directory of PBSpath that contains all
        the entries in transferList.
    RETURN:
        Undefined
    '''

    logger = logging.getLogger('BFprocess')
    
    # ======================
    # = default parameters =
    # ======================
    WALLTIME      = "48:00:00"
    NUM_NODES     = 1
    PPN           = 1            # processors (cores) per node
    VIRT_ENV      = os.environ['VIRTUAL_ENV']
    POLL_INTERVAL = 60
    GLOBUS_LOG    = os.path.join( logDir, jobName + ".log" )
    GLOBUS_PBS    = os.path.join( PBSpath, jobName + "_pbs.pbs" )

    logger.debug("Globus log saved at {}.".format(GLOBUS_LOG))
    logger.debug("Globus PBS script saved at {}.".format(GLOBUS_PBS))
    logger.debug("jobName: {}".format(jobName))
    logger.debug("Using virtual environment: {}".format(VIRT_ENV))

    # Argument check
    if not isinstance( oLimits, list ) or len( oLimits) != 2:
        raise ValueError("oLimits must a list of length 2!")
    
    # Convert the transferList into a file that is needed by the Globus python API for batch transfers.
    # Refer to their online documentation for batch transfers.

    batchFile = os.path.join( PBSpath, jobName + "_batch.txt")
    with open( batchFile, 'w' ) as f:
        for i in transferList:
            f.write(i + '\n')

   
    PBSfile = '''#!/bin/bash 
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}

# Source the Basic Fusion python virtual environment
source {}

logFile={}
remoteID={}
hostID={}
jobName={}
batchFile={}
pollInt={}
summaryLog={}
oLower={}
oMax={}
globPull={}
picklePath={}
hashDir={}
nodes={}
ppn={}

python ${{globPull}} -l DEBUG $picklePath $batchFile $remoteID $hostID $hashDir $summaryLog $jobName $logFile

retVal=$?
if [ $retVal -ne 0 ]; then
    echo \"Failed to transfer Globus task ${{jobName}}.\" > $logFile
    echo \"FAIL: ${{oLower}}_${{oMax}}: Failed to pull data from nearline. See: $logFile\" > $summaryLog
    exit 1
fi


'''.format( NUM_NODES, PPN, WALLTIME, jobName, os.path.join( VIRT_ENV, "bin", "activate"), GLOBUS_LOG, remoteID, hostID, \
    jobName, batchFile, POLL_INTERVAL, summaryLog, oLimits[0], oLimits[1], \
    '/u/sciteam/clipp/basicFusion/util/TerraGen/globus_pull.py', picklePath, '/no/hash', NUM_NODES, PPN )

    with open ( GLOBUS_PBS, 'w' ) as f:
        f.write( PBSfile )

    return GLOBUS_PBS

#==========================================================================
    
def main():
    # Define the argument parser
    parser = argparse.ArgumentParser(description='''This script is the final-ish workflow for the BasicFusion project. It retrieves the tar files from Nearline and processes all orbits from ORBIT_START to ORBIT_END. It is recommended that users wrap this script with a shell script since the arguments to this program are lengthy. \
         \
        REQUIREMENTS: \
            1. This program assumes it can submit 4 active Globus transfer requests. The default from Globus is 3. \
               If you have not asked Globus to increase this limit, either change the default values in this script \
               or ask for an increase from Globus.\
        ''')
    parser.add_argument("ORBIT_START", help="The starting orbit to process.", type=int)
    parser.add_argument("ORBIT_END", help="The ending orbit to process.", type=int)
    parser.add_argument("REMOTE_ID", help="The Globus endpoint identifier of Nearline, or where the tar files reside \
        and where the final BasicFusion files will be stored.", type=str)
    parser.add_argument("REMOTE_TAR_DIR", help="The directory on Nearline (or other valid Globus endpoint) where the tar files reside. \
        This directory contains the subdirectories \'2000\', \'2001\', \'2002\' etc, in which contain the tar files for each orbit of that year. \
        Each tar file is named #archive.tar, where # is the orbit number.", type=str )
    parser.add_argument("REMOTE_BF_DIR", help="Where the Basic Fusion files will be stored on Nearline. Leaf directory \
        need not exist, script will create it.", type=str)
    parser.add_argument("HOST_ID", help="The globus endpoint identifier of this current machine.", type=str )
    
    parser.add_argument("HOST_STAGE", help="Directory where intermediary files are located. This includes tar files, \
        input file lists, and Basic Fusion files. Will create one directory inside this path \
        for all stage files.", type=str)
    parser.add_argument("LOG_OUTPUT", help="Directory where the log directory tree will be created. This includes \
        PBS scripts, log files, and various other administrative files needed for the workflow. Creates directory \
        called {}. Defaults to {}".format(logName, os.getcwd()), nargs='?', type=str, default=os.getcwd())
    parser.add_argument("-g", "--job-granularity", help="Determines the maximum number of orbits each job will be responsible for. \
        Define this value if you need to increase/decrease the amount of disk space the intermediary files take up. Defaults to 5000.", \
        dest="granule", type=int, default=5000)

    ll = parser.add_mutually_exclusive_group(required=False)
    ll.add_argument("-l", "--log", help="Set the log level. Allowable values are INFO, DEBUG. Absence of this parameter \
        sets debug level to WARNING.", type=str, choices=['INFO', 'DEBUG' ] , default="WARNING")

    args = parser.parse_args()

    # Argument sanity check
    if args.ORBIT_START > args.ORBIT_END:
        raise ValueError("Starting orbit can't be greater than ending orbit!")
    if args.granule < 0:
        raise ValueError("Job granularity can't be less than 0!")

    # Define the logger
    if args.log == 'DEBUG':
        ll = logging.DEBUG
    elif args.log == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING

    # ======================
    # = VARIABLES / MACROS =
    # ======================
    VIRT_ENV      = os.environ['VIRTUAL_ENV']
    

    


    # Create all of the necessary PBS scripts and the logging tree for every job.
    # We use try/except because the log directory may already exist. If it does,
    # we want to discard the exception created by os.makedirs.
    logDir = os.path.join( args.LOG_OUTPUT, logName) 
    try:
        os.makedirs( logDir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    # Make the run directory
    runDir     = makeRunDir( logDir )
    # Make the rest of the log directory
    PBSdir          = os.path.join( runDir, 'PBSscripts' )
    PBSdirs         = { 'globus_push' : os.path.join( PBSdir, 'globus_push' ), \
                        'globus_pull' : os.path.join( PBSdir, 'globus_pull' ), \
                        'process'     : os.path.join( PBSdir, 'process' ) \
                      }
    logDir          = os.path.join( runDir, 'logs' )
    logDirs         = { 'summary'     : os.path.join( logDir, 'summary' ), \
                        'globus_pull' : os.path.join( logDir, 'globus_pull' ), \
                        'misc'        : os.path.join( logDir, 'misc' ), \
                        'process'     : os.path.join( logDir, 'process'), \
                        'globus_push' : os.path.join( logDir, 'globus_push') \
                      }

    os.makedirs( PBSdir )
    os.makedirs( logDir )
    for i in PBSdirs:
        os.makedirs( PBSdirs[i] )
    for i in logDirs:
        os.makedirs( logDirs[i] )

    #
    # ENABLE LOGGING
    #

    FORMAT='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    dateformat='%d-%m-%Y:%H:%M:%S'
    logFormatter = logging.Formatter(FORMAT, dateformat)
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler( os.path.join( logDirs['misc'], 'BFprocess.log') )
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(ll)
 
    logger = logging.getLogger('BFprocess')

    logger.debug("Using virtual environment detected here: {}".format(VIRT_ENV))
    #
    # SAVE THE LOG DIRECTORY PICKLE
    #
    picklePath = os.path.join( logDirs['misc'], 'logPickle.txt' )
    logger.info("Saving the log directory as pickle at: {}".format(picklePath))

    with open( picklePath, 'wb' ) as f:
        pickle.dump( logDirs, f )

    #
    # FIND JOB PARTITION
    #

    # Time to determine how to split up the jobs. Each job quanta will be responsible for
    # at most args.granule number of orbits.
    numOrbits = args.ORBIT_END - args.ORBIT_START + 1
    logger.info("Processing {} number of orbits.".format(numOrbits))

    numFullJobs = numOrbits / args.granule
    lastJobOrbits = numOrbits % args.granule
    logger.debug("{} jobs with {} number of orbits. Last job has {} orbits".format(numFullJobs, args.granule, lastJobOrbits))

    # A "quanta" is described in the docstring of this file. It's just a fancy word I'm using to describe the action of:
    # 1. Pulling data from Nearline
    # 2. Processing the data
    # 3. Pushing output back to Nearline

    # This if statement checks that if there is a remainder in the numOrbits / args.granule division,
    # that we add an extra quanta that will be responsible for this remainder.
    numQuanta = numFullJobs
    if lastJobOrbits != 0:
        numQuanta += 1

    logger.info("Number of processing quanta: {}".format(numQuanta))

    sOrbit = args.ORBIT_START
    quantas = []
    i = 0
    for i in xrange(numFullJobs):
        eOrbit = sOrbit + args.granule - 1 
        quantas.append( processQuanta( orbitStart=sOrbit, orbitEnd=eOrbit) )
        logger.info("{}: starting orbit: {} ending orbit: {}".format(i, sOrbit, eOrbit))
        sOrbit = eOrbit + 1

    # The remainder of numOrbits / args.granule needs to be accounted for
    if lastJobOrbits != 0 :
        quantas.append( processQuanta( orbitStart=sOrbit, orbitEnd=sOrbit + lastJobOrbits - 1 ) )
        logger.info("{}: starting orbit: {} ending orbit: {}".format(i+1, sOrbit, sOrbit + lastJobOrbits - 1))

    # ======================================
    # =          MAKE PBS SCRIPTS          =
    # ======================================


    # Make the PBS scripts for all the jobs
    for i in quantas:
        transferList = []
        globPullName = "NL-BW_{}_{}".format(i.orbitStart, i.orbitEnd)
        summaryLog = os.path.join( logDirs['summary'], globPullName + "_summary.log" )

        # We need to make the transfer list required by makePBS_globus
        for orbit in xrange( i.orbitStart, i.orbitEnd + 1 ):
            
            # Find what year this orbit belongs to
            year = orbitYear( orbit )

            # We should now have the year for this orbit
            remoteFile = os.path.join( args.REMOTE_TAR_DIR, str( year ), "{}archive.tar".format(orbit) )
            hostFile   = os.path.join( args.HOST_STAGE, str( year ), "{}archive.tar".format(orbit) )

            transferList.append( remoteFile + " " + hostFile )
     
        #
        # MAKE THE GLOBUS PULL PBS SCRIPTS
        #

        logger.info("Making Globus pull PBS script for job {}.".format(globPullName)) 
        i.PBSfile['pull'] = makePBS_globus( transferList,  PBSdirs['globus_pull'], args.REMOTE_ID, args.HOST_ID, \
                            globPullName, logDirs['globus_pull'], summaryLog, [ i.orbitStart, i.orbitEnd], picklePath )

   
         
        
if __name__ == '__main__':
    main()
