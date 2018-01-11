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
from bfutils import makeRunDir, orbitYear
import pickle
import errno
import subprocess
from workflowClass import granule, processQuanta
import json

#=================================
logName='BF_jobFiles'
logger = 0
#=================================


def makePBS_globus(transferList, PBSpath, remoteID, hostID, jobName, logDir, \
    summaryLog, oLimits, json_log, scratchSpace, MISR_path_files, hashDir, \
    projPath, globus_parallelism, nodes, ppn, output_granule_list, modis_missing ):
    '''
**DESCRIPTION:**
    This function makes the PBS scripts and other files necessary to pull the proper orbits from Nearline
    to the host machine. Calling function must pass in a list of files to transfer.
    Essentially, this is just some simple meta-programming. This makes use of the Globus Python Command 
    Line Interface.

**ARGUMENTS:**
    *transferList (list)* -- A list containing a list of files to transfer from remote to the host machine.
                           Each element contains absolute path of single file on remote machine, then absolute
                           path of the file on the host machine, the paths delimited by a space. Note that BOTH
                           paths must include a filename!  
    *PBSpath (str)*       -- An absolute path of a directory to where the PBS script and the transferList
                           text file will be created.  
    *remoteID (str)*      -- The Globus identifier of the remote site to download the data from.
    *hostID (str)*        -- The Globus identifier of the host machine.  
    *jobName (str)*       -- Serves two purposes: will define the prefix of the PBS script file, as well as
                           defining the name of the job to the PBS scheduler.  
    *logDir (str)*        -- Directory where Globus CLI output will be redirected to.  
    *summaryLog (str)*    -- This log file contains summary information for everything that goes wrong in this process.  
    *oLimits (list, int)* -- A list containing the lower and upper bounds of these orbits.  
    *json_log (str)*      -- Path to the JSON dump that contains the paths of the log directories.  
    *scratchSpace (str)*  -- Directory mounted on a high-performance, high-capacity filesystem for scratch work.  
    *MISR_path_files (str)*   -- Where the MISR HRLL and AGP files are stored.  
    *hashDir (str)*       -- Directory of the hashes of the input tar files.  
    *modis_missing (str)* -- If not none, this is the directory where the missing MODIS file reside for each orbit.
                           This directory contains subdirectories named after the orbit number, where all files within
                           each orbit subdirectory will be included in that BF granule.

**EFFECTS:**
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
    NUM_NODES     = nodes
    PPN           = ppn            # processors (cores) per node
    VIRT_ENV      = os.environ['VIRTUAL_ENV']
    POLL_INTERVAL = 60
    GLOBUS_LOG    = os.path.join( logDir, jobName + ".log" )
    GLOBUS_PBS    = os.path.join( PBSpath, jobName + "_pbs.pbs" )
    mpi_exec      = 'aprun'
    pull_process_script  = os.path.join( projPath, 'util', 'TerraGen', 'pull_process.py' )

    logger.debug("Globus log saved at {}.".format(GLOBUS_LOG))
    logger.debug("Globus PBS script saved at {}.".format(GLOBUS_PBS))
    logger.debug("jobName: {}".format(jobName))
    logger.debug("Using virtual environment: {}".format(VIRT_ENV))
    
    if not os.path.isfile( pull_process_script ):
        raise RuntimeError("pull_process.py not found at: {}".format( pull_process_script) )

    # Argument check
    if not isinstance( oLimits, list ) or len( oLimits) != 2:
        raise ValueError("oLimits must a list of length 2!")
    
    # Convert the transferList into a file that is needed by the Globus python API for batch transfers.
    # Refer to their online documentation for batch transfers.

    batchFile = os.path.join( PBSpath, jobName + "_batch.txt")
    with open( batchFile, 'w' ) as f:
        for i in transferList:
            f.write(i + '\n')

   
    if not modis_missing is None:
        modis_missing='modis_missing={}'.format(modis_missing)
    else:
        modis_missing='modis_missing='

    print modis_missing

    PBSfile = '''#!/bin/bash 
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}
#PBS -l geometry=1x4x2/2x2x2/2x4x1/4x1x2/4x2x1

# Even with commtransparent, NCSA told me the jobs are still causing network conjestion.
# So they told me to add this export command. Don't know what it does.
export APRUN_BALANCED_INJECTION=64


# Need to source this file before calling module command
source /opt/modules/default/init/bash

# Load proper python modules
module load bwpy
module load bwpy-mpi

# Set the environment variables for Basic Fusion code
export USE_GZIP=1
export USE_CHUNK=1
 
# Source the Basic Fusion python virtual environment
source {}

logFile={}
remoteID={}
hostID={}
jobName={}
batchFile={}
summaryLog={}
oLower={}
oMax={}
pull_process_script={}
json_log={}
hashDir={}
nodes={}
ppn={}
scratchSpace={}
MISR_path_files={}
globus_parallelism={}
output_granule_list={}

# This variable is the modis_missing. Need to allow Python meta-programmer to completely write the variable
# declaration.

{}

# Write a variable that will contain the actual python '--modis_missing [dir]' parameter.
# We let the bash script do this logic because it's easier to let it determine how
# to pass in the --include_missing parameter instead of the meta-programmer.

if [ ${{#modis_missing}} -ge 1 ]; then
    modis_miss_param="--include-missing ${{modis_missing}}"
else
    modis_miss_param=""
fi

{{ {} -n {} -N $ppn -d 1 python ${{pull_process_script}} ${{modis_miss_param}} -l DEBUG $json_log $batchFile $scratchSpace $remoteID $hostID $hashDir $summaryLog $jobName $MISR_path_files --num-transfer $globus_parallelism $output_granule_list ; }} &> $logFile

retVal=$?
if [ $retVal -ne 0 ]; then
    echo \"FAIL: ${{oLower}}_${{oMax}}: Failed to pull data from nearline. See: $logFile\" > $summaryLog
    exit 1
fi


'''.format( NUM_NODES, PPN, WALLTIME, jobName, os.path.join( VIRT_ENV, "bin", "activate"), \
    GLOBUS_LOG, remoteID, hostID, jobName, batchFile, summaryLog, oLimits[0], oLimits[1], \
    pull_process_script, json_log, hashDir, NUM_NODES, PPN, scratchSpace, MISR_path_files, \
    globus_parallelism, output_granule_list, modis_missing, mpi_exec, PPN * NUM_NODES )

    with open ( GLOBUS_PBS, 'w' ) as f:
        f.write( PBSfile )

    return GLOBUS_PBS

#===========================================================================

def makePBS_push( output_granule_list, PBSpath, remote_id, host_id, remote_dir, \
    job_name, log_pickle_path, summary_log, projPath, logFile, num_transfer ):
    
    # ======================
    # = default parameters =
    # ======================
    WALLTIME      = "48:00:00"
    NUM_NODES     = 1
    PPN           = 1            # processors (cores) per node
    VIRT_ENV      = os.path.join( projPath, 'externLib', 'BFpyEnv', 'bin', 'activate' )
    GLOBUS_LOG    = logFile
    GLOBUS_PBS    = os.path.join( PBSpath, job_name + "_pbs.pbs" )
    MPI_EXEC      = 'aprun'
    globus_push_script  = os.path.join( projPath, 'util', 'TerraGen', 'globus_push.py' )

    PBSfile = '''#!/bin/bash 
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}
#PBS -l flags=commtransparent

# Even with commtransparent, NCSA told me the jobs are still causing network conjestion.
# So they told me to add this export command. Don't know what it does.
export APRUN_BALANCED_INJECTION=64

ppn={}
nodes={}
globus_push_script={}
num_transfer={}
output_granule_list={}
remote_id={}
remote_dir={}
host_id={}
job_name={}
log_pickle_path={}
summary_log={}
log_file={}

# Source the Basic Fusion python virtual environment
source {}


{{ {} -n $(( $ppn * $nodes )) -N $ppn -d 1 python "${{globus_push_script}}" -l DEBUG -g $num_transfer "$output_granule_list" $remote_id "$remote_dir" $host_id "$job_name" "$log_pickle_path" "$summary_dir" ; }} &> $log_file

retVal=$?
if [ $retVal -ne 0 ]; then
    echo \"${{job_name}}: Failed to push data to nearline. See: $log_file\" > $summary_log
    exit 1
fi
    '''.format( NUM_NODES, PPN, WALLTIME, job_name, PPN, NUM_NODES, globus_push_script, \
        num_transfer, output_granule_list, remote_id, remote_dir, host_id, job_name, \
        log_pickle_path, summary_log, GLOBUS_LOG, VIRT_ENV, MPI_EXEC )

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
    parser.add_argument("SCRATCH_SPACE", help="Directory where all intermediary tar and Basic Fusion files will reside. \
        Should be directory to a high-performance, high-capacity file system.", type=str )
    parser.add_argument("MISR_PATH", help="Where the MISR HRLL and AGP files reside. No specific subdirectory structure \
        is necessary.", type=str)
    parser.add_argument("HASH_DIR", help="Where the hashes for all of the tar files reside. Subdirectory structure \
        should be: one directory for each year from 2000 onward, named after the year. Each directory has hash files \
        for each orbit of that year, named #hash.md5 where # is the orbit number.", type=str ) 
    parser.add_argument("LOG_OUTPUT", help="Directory where the log directory tree will be created. This includes \
        PBS scripts, log files, and various other administrative files needed for the workflow. Creates directory \
        called {}. Defaults to {}".format(logName, os.getcwd()), nargs='?', type=str, default=os.getcwd())
    parser.add_argument("NODES", help="Number of XE nodes to request from scheduler.", type=int )
    parser.add_argument("PPN", help="Processors per node.", type=int)
    parser.add_argument("-g", "--job-granularity", help="Determines the maximum number of orbits each job will be responsible for. \
        Define this value if you need to increase/decrease the amount of disk space the intermediary files take up. Defaults to 5000.", \
        dest="granule", type=int, default=5000)
    parser.add_argument("-p", "--globus-parallel", help="Globus parallelism. Defines how many Globus transfer requests are submitted \
        for any given transfer job. Defaults to 1.", dest='GLOBUS_PRL', type=int, default=1)
    parser.add_argument('--include-missing', help='Path to the missing MODIS files, where passed directory contains subdirectories named \
        after each orbit. Each of those subdirectories contains the MODIS files to include in the final BF granule.', type=str, \
        dest='modis_missing' )
    
    ll = parser.add_mutually_exclusive_group(required=False)
    ll.add_argument("-l", "--log", help="Set the log level. Allowable values are INFO, DEBUG. Absence of this parameter \
        sets debug level to WARNING.", type=str, choices=['INFO', 'DEBUG' ] , default="WARNING")

    args = parser.parse_args()

    # Argument sanity check
    if args.ORBIT_START > args.ORBIT_END:
        raise ValueError("Starting orbit can't be greater than ending orbit!")
    if args.granule < 0:
        raise ValueError("Job granularity can't be less than 0!")
    if args.GLOBUS_PRL < 1:
        raise ValueError("GLOBUS_PRL argument can't be less than 1!")

    # Define the logger
    if args.log == 'DEBUG':
        ll = logging.DEBUG
    elif args.log == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING


    # Create all of the necessary PBS scripts and the logging tree for every job.
    # We use try/except because the log directory may already exist. If it does,
    # we want to discard the exception created by os.makedirs.
    logDir = os.path.join( args.LOG_OUTPUT, logName )
 
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
                        'pull_process' : os.path.join( PBSdir, 'pull_process' ), \
                        'process'     : os.path.join( PBSdir, 'process' ) \
                      }
    logDir          = os.path.join( runDir, 'logs' )
    logDirs         = { 'submit_workflow'   : os.path.join( logDir, 'submit_workflow' ), \
                        'summary'           : os.path.join( logDir, 'summary' ), \
                        'pull_process'       : os.path.join( logDir, 'pull_process' ), \
                        'misc'              : os.path.join( logDir, 'misc' ), \
                        'process'           : os.path.join( logDir, 'process'), \
                        'globus_push'       : os.path.join( logDir, 'globus_push') \
                      }

    os.makedirs( PBSdir )
    os.makedirs( logDir )
    for i in PBSdirs:
        os.makedirs( PBSdirs[i] )
    for i in logDirs:
        os.makedirs( logDirs[i] )

    # ======================
    # = VARIABLES / MACROS =
    # ======================
    VIRT_ENV      = os.environ['VIRTUAL_ENV']
    scriptPath    = os.path.realpath(__file__)
    projPath      = ''.join( scriptPath.rpartition('basicFusion/')[0:2] )
    orbit_times_txt = os.path.join( projPath, 'metadata-input', 'data', 'Orbit_Path_Time.txt' )
    SUBMIT_LOG      = os.path.join( logDirs['submit_workflow'], 'submit_workflow.log')
    qsub_dep_specifier='afterany'
    
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
    fileHandler = logging.FileHandler( SUBMIT_LOG , mode='a' )
    fileHandler.setFormatter(logFormatter)
    
    rootLogger.addHandler(fileHandler)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(ll)
 
    logger = logging.getLogger('submit_workflow')

    logger.debug("Using virtual environment detected here: {}".format(VIRT_ENV))
    #
    # SAVE THE LOG DIRECTORY PICKLE
    #
    json_log = os.path.join( logDirs['misc'], 'log_dirs.json' )
    logger.info("Saving the log directory as JSON dump at: {}".format(json_log))

    with open( json_log, 'wb' ) as f:
        json.dump( logDirs, f, sort_keys=True, indent=4, separators=(',', ': ') )

    # Check that all directory arguments actually exist in the filesystem
    if not os.path.isdir( args.HASH_DIR ):
        raise RuntimeError( "{} is not a valid directory!".format( args.HASH_DIR ) )
    if not os.path.isdir( args.LOG_OUTPUT ):
        raise RuntimeError( "{} is not a valid directory!".format( args.LOG_OUTPUT ) )
    if not os.path.isdir( args.SCRATCH_SPACE ):
        raise RuntimeError( "{} is not a valid directory!".format( args.SCRATCH_SPACE ) )
        
    # Argument sanitizing
    args.HASH_DIR = os.path.abspath( args.HASH_DIR )
    args.LOG_OUTPUT = os.path.abspath( args.LOG_OUTPUT )
    args.SCRATCH_SPACE = os.path.abspath( args.SCRATCH_SPACE )
    
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
        globPullName = "pull_process_{}_{}".format(i.orbitStart, i.orbitEnd)
        globPushName = "push_{}_{}.".format( i.orbitStart, i.orbitEnd )
        summaryLog = os.path.join( logDirs['summary'], globPullName + "_summary.log" )
        summary_log_push = os.path.join( logDirs['summary'], globPushName + "_summary.log")
        globPushName = "push_{}_{}".format(i.orbitStart, i.orbitEnd)

        # We need to make the transfer list required by makePBS_globus
        for orbit in xrange( i.orbitStart, i.orbitEnd + 1 ):
            
            # Find what year this orbit belongs to
            year = orbitYear( orbit )

            # We should now have the year for this orbit
            remoteFile = os.path.join( args.REMOTE_TAR_DIR, str( year ), "{}archive.tar".format(orbit) )
            hostFile   = os.path.join( args.SCRATCH_SPACE, "tar", str( year ), "{}archive.tar".format(orbit) )

            transferList.append( remoteFile + " " + hostFile )
     
        # ------------------------------------
        # - MAKE THE GLOBUS PULL PBS SCRIPTS -
        # ------------------------------------

        output_granule_list = os.path.join( logDirs['misc'], "{}_output_granule_list.p".format(globPullName) )
        logger.info("Making Globus pull PBS script for job {}.".format(globPullName))
        i.PBSfile['pull'] = makePBS_globus( transferList,  PBSdirs['pull_process'], args.REMOTE_ID, args.HOST_ID, \
                            globPullName, logDirs['pull_process'], summaryLog, [ i.orbitStart, i.orbitEnd], json_log, \
                            args.SCRATCH_SPACE, args.MISR_PATH, args.HASH_DIR, projPath, args.GLOBUS_PRL, args.NODES, args.PPN, \
                            output_granule_list, args.modis_missing )

        # ------------------------------------
        # - MAKE THE GLOBUS PUSH PBS SCRIPTS -
        # ------------------------------------

        i.PBSfile['push'] = makePBS_push( output_granule_list, PBSdirs['globus_push'], args.REMOTE_ID, args.HOST_ID, \
            args.REMOTE_BF_DIR, globPushName, json_log, summary_log_push, projPath, os.path.join( logDirs['globus_push'], \
            globPushName + "_log.log"), args.GLOBUS_PRL ) 

    
    # Now that all quantas have been prepared, we can submit their jobs to the queue

    # Keep track of the last 2 quanta
    prevQuant=None
    prev_2_Quant=None
    for i in quantas:    
       
        # Submit this to the scheduler, taking care to save stdout (which will contain the job ID)
        # I'm using the shell command cd {} && qsub because qsub is going to make stderr and stdout
        # files. I just want those files to be in the same directory as the PBS files. Of course, these
        # two log files should not have any substantial information in them since this workflow should
        # have all output redirected to the proper log file.
         
        if prevQuant is not None: 
            logger.info('Calling qsub on {} with {} dependency {}'.format( i.PBSfile['pull'], \
                        qsub_dep_specifier, prevQuant.jobID['pull'] ) )
            qsubCall = 'cd {} && qsub -W depend={}:{} {}'.format( os.path.dirname(i.PBSfile['pull']), \
                qsub_dep_specifier, prevQuant.jobID['pull'], i.PBSfile['pull']) 
        else:
            logger.info('Calling qsub on {}'.format( i.PBSfile['pull'] ) )
            qsubCall = 'cd {} && qsub {}'.format( os.path.dirname(i.PBSfile['pull']),  i.PBSfile['pull']) 

        logger.debug( qsubCall )
        child = subprocess.Popen( qsubCall, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
        retCode = child.wait()

        # Get the stderr and stdout
        stderrFile = child.stderr
        stderr = stderrFile.read()
        stdoutFile = child.stdout
        stdout = stdoutFile.read()

        logger.info( 'QSUB OUTPUT: ' + '\n' + stdout + '\n' + stderr )
        
        if retCode != 0:
            errMsg="qsub command exited with retcode {}".format(retCode)
            logger.error(errMsg)
            raise RuntimeError(errMsg)

        i.jobID['pull'] = stdout.strip()

        # --------------------
        # - GLOBUS PUSH QSUB -
        # --------------------
        if prevQuant is not None:
            logger.info("Calling qsub on {} with {} dependency {}:{}".format( \
                i.PBSfile['push'], qsub_dep_specifier, i.jobID['pull'], \
                prevQuant.jobID['push'] ) )
            qsubCall = 'cd {} && qsub -W depend={}:{}:{} {}'.format( \
                os.path.dirname(i.PBSfile['push']),  qsub_dep_specifier, \
                i.jobID['pull'], prevQuant.jobID['push'], i.PBSfile['push']) 
        
        else:
            logger.info("Calling qsub on {} with {} dependency {}".format( i.PBSfile['push'], qsub_dep_specifier, i.jobID['pull'] ) ) 
            qsubCall = 'cd {} && qsub -W depend={}:{} {}'.format( os.path.dirname(i.PBSfile['push']),  qsub_dep_specifier, \
                                                                  i.jobID['pull'], i.PBSfile['push']) 

        logger.debug( qsubCall )
        child = subprocess.Popen( qsubCall, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
        retCode = child.wait()

        # Get the stderr and stdout
        stderrFile = child.stderr
        stderr = stderrFile.read()
        stdoutFile = child.stdout
        stdout = stdoutFile.read()

        logger.info( 'QSUB OUTPUT: ' + '\n' + stdout + '\n' + stderr )
        
        if retCode != 0:
            eMsg="qsub command exited with retcode {}".format(retCode)
            logger.error( eMsg )
            raise RuntimeError(eMsg)

        i.jobID['push'] = stdout.strip()

        prev_2_Quant = prevQuant
        prevQuant=i

if __name__ == '__main__':
    main()
