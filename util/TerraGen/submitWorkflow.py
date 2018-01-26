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
from bfutils import workflow
import bfutils
import pickle
import errno
import subprocess
from workflowClass import granule, processQuanta
import json
import workflowClass
from multiprocessing import Pool

#=================================
logName='BF_jobFiles'
logger = 0
VIRT_ENV = None
#=================================


def makePBS_process(granule_list, PBSpath, remoteID, hostID, jobName, logDir, \
    summaryLog, oLimits, json_log, scratchSpace, MISR_path_files, hashDir, \
    projPath, globus_parallelism, nodes, ppn, modis_missing, save_interm, \
    queue ):
    '''
**DESCRIPTION:**
    This function makes the PBS scripts and other files necessary to pull the proper orbits from Nearline
    to the host machine. Calling function must pass in a list of files to transfer.
    Essentially, this is just some simple meta-programming. This makes use of the Globus Python Command 
    Line Interface.

**ARGUMENTS:**
    *granule_list (str)* -- Path to a Python pickle that contains a list of
        granule objects that define BasicFusion granules (both input and output).  
    *PBSpath (str)*       -- An absolute path of a directory to where the PBS
        script will be created.  
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
    *save_interm* (bool)    -- Save the intermediary files on scratch.
    *queue* (str)      -- Sets the queue requested to PBS. Allowable 
        values are defined by the system's PBS scheduler.  

**EFFECTS**  
    Creates a PBS file at PBSpath.
**RETURN**  
    Undefined
    '''

    logger = logging.getLogger('BFprocess')
    
    # ======================
    # = default parameters =
    # ======================
    WALLTIME      = "48:00:00"
    NUM_NODES     = nodes
    PPN           = ppn            # processors (cores) per node
    POLL_INTERVAL = 60
    GLOBUS_LOG    = os.path.join( logDir, jobName + ".log" )
    GLOBUS_PBS    = os.path.join( PBSpath, jobName + "_pbs.pbs" )
    mpi_exec      = 'aprun'
    pull_process_script  = os.path.join( projPath, 'util', 'TerraGen', 'pull_process.py' )

    logger.debug("Globus log saved at {}.".format(GLOBUS_LOG))
    logger.debug("Globus PBS script saved at {}".format(GLOBUS_PBS))
    logger.debug("jobName: {}".format(jobName))
    logger.debug("Using virtual environment: {}".format(VIRT_ENV))
    
    save_interm_str = ''
    if save_interm:
        save_interm_str = '--save-interm'
    
    if not os.path.isfile( pull_process_script ):
        raise RuntimeError("pull_process.py not found at: {}".format( pull_process_script) )

    # Argument check
    if not isinstance( oLimits, list ) or len( oLimits) != 2:
        raise ValueError("oLimits must a list of length 2!")
    
    if not modis_missing is None:
        modis_missing='modis_missing={}'.format(modis_missing)
    else:
        modis_missing='modis_missing='

    PBSfile = '''#!/bin/bash 
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}
#PBS -q {}
##PBS -l geometry=1x4x2/2x2x2/2x4x1/4x1x2/4x2x1

##PBS -l flags=commtransparent

# Even with commtransparent, NCSA told me the jobs are still causing network conjestion.
# So they told me to add this export command. Don't know what it does.
# export APRUN_BALANCED_INJECTION=64


# Need to source this file before calling module command
source /opt/modules/default/init/bash

# Load proper python modules
module load bwpy
module load bwpy-mpi

module unload hdf5
module unload hdf4
module unload netcdf

module load cray-netcdf

# Set the environment variables for Basic Fusion code
export USE_GZIP=1
export USE_CHUNK=1
 
# Source the Basic Fusion python virtual environment
# source {}

# Need to source this file before calling module command
source /opt/modules/default/init/bash

# Load proper python modules
module load bwpy
module load bwpy-mpi

logFile={}
remoteID={}
hostID={}
jobName={}
granule_list={}
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
save_interm={}

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

{{ {} -n $(( $ppn * $nodes )) -N $ppn bwpy-environ -- python ${{pull_process_script}} ${{save_interm}} ${{modis_miss_param}} -l DEBUG $json_log ${{granule_list}} $scratchSpace $remoteID $hostID $hashDir $summaryLog $jobName $MISR_path_files --num-transfer $globus_parallelism ; }} 1>> $logFile 2>> $logFile

retVal=$?
if [ $retVal -ne 0 ]; then
    echo \"FAIL: ${{oLower}}_${{oMax}}: Failed to pull data from nearline. See: $logFile\" >> $summaryLog
    exit 1
fi


'''.format( NUM_NODES, PPN, WALLTIME, jobName, queue, VIRT_ENV, \
    GLOBUS_LOG, remoteID, hostID, jobName, granule_list, summaryLog, oLimits[0], oLimits[1], \
    pull_process_script, json_log, hashDir, NUM_NODES, PPN, scratchSpace, MISR_path_files, \
    globus_parallelism, save_interm_str, modis_missing, mpi_exec )

    with open ( GLOBUS_PBS, 'w' ) as f:
        f.write( PBSfile )

    return GLOBUS_PBS

#===========================================================================
def makePBS_pull( job_name, pull_script, log_file, sum_log, granule_list, \
    src_id, dest_id, log_dirs, save_interm, queue ):
    
    # ----------------------
    # - default parameters -
    # ----------------------

    walltime    = '48:00:00'
    nodes       = 1
    ppn         = 1
    num_ranks   = 1

    save_interm_str = ''
    if save_interm:
        save_interm_str = '--save-interm'

    PBSfile = '''#!/bin/bash
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}
#PBS -q {}


# Need to source this file before calling module command
source /opt/modules/default/init/bash

# Load proper python modules
module load bwpy
module load bwpy-mpi

# source {}

job_name={}
pull_script={}
ppn={}
num_ranks={}
log_file={}
sum_log={}
granule_list={}
src_id={}
dest_id={}
log_dirs={}
save_interm={}

{{ aprun -n ${{num_ranks}} -N $ppn -d 1 bwpy-environ -- python ${{pull_script}} ${{save_interm}} ${{granule_list}} ${{src_id}} ${{dest_id}} ${{log_dirs}} ${{job_name}} ; }} 1>> $log_file 2>> $log_file

retVal=$?
if [ $retVal -ne 0 ]; then
    echo "FAIL: ${{job_name}}: Failed to pull data from nearline. See: $log_file" >> $sum_log
    exit 1
fi
'''.format( nodes, ppn, walltime, job_name, queue, VIRT_ENV, job_name, pull_script, \
    ppn, num_ranks, log_file, sum_log, granule_list, src_id, dest_id, log_dirs, \
    save_interm_str, job_name )

    return PBSfile

#===========================================================================

def makePBS_push( output_granule_list, PBSpath, remote_id, host_id, remote_dir, \
    job_name, log_pickle_path, summary_log, projPath, logFile, num_transfer, \
    save_interm, queue ):
    
    # ======================
    # = default parameters =
    # ======================
    WALLTIME      = "48:00:00"
    NUM_NODES     = 1
    PPN           = 1            # processors (cores) per node
    GLOBUS_PBS    = os.path.join( PBSpath, job_name + "_pbs.pbs" )
    MPI_EXEC      = 'aprun'
    push_script  = os.path.join( projPath, 'util', 'TerraGen', 'globus_push.py' )

    save_interm_str = ''
    if save_interm:
        save_interm_str = '--save-interm'
    
    PBSfile = '''#!/bin/bash 
#PBS -l nodes={}:ppn={}
#PBS -l walltime={}
#PBS -N {}
#PBS -l flags=commtransparent
#PBS -q {}
# Even with commtransparent, NCSA told me the jobs are still causing network conjestion.
# So they told me to add this export command. Don't know what it does.
export APRUN_BALANCED_INJECTION=64

ppn={}
nodes={}
push_script={}
num_transfer={}
output_granule_list={}
remote_id={}
remote_dir={}
host_id={}
job_name={}
log_pickle_path={}
summary_log={}
log_file={}
save_interm={}

# Source the Basic Fusion python virtual environment
# source {}


{{ {} -n $(( $ppn * $nodes )) -N $ppn bwpy-environ -- python "${{push_script}}" -l DEBUG -g $num_transfer ${{save_interm}} "$output_granule_list" $remote_id "$remote_dir" $host_id "$job_name" "$log_pickle_path" "$summary_log" ; }} 1>> $log_file 2>> $log_file

retVal=$?
if [ $retVal -ne 0 ]; then
    echo \"${{job_name}}: Failed to push data to nearline. See: $log_file\" >> $summary_log
    exit 1
fi
    '''.format( NUM_NODES, PPN, WALLTIME, job_name, queue, PPN, NUM_NODES, push_script, \
        num_transfer, output_granule_list, remote_id, remote_dir, host_id, job_name, \
        log_pickle_path, summary_log, logFile, save_interm_str, \
        VIRT_ENV, MPI_EXEC )

    with open ( GLOBUS_PBS, 'w' ) as f:
        f.write( PBSfile )

    return GLOBUS_PBS

#==========================================================================
def make_granule( arg_tuple ):
   
    orbit = arg_tuple[0]
    bf_metadata = arg_tuple[1]
    FILENAME = arg_tuple[2]
    BFoutputDir = arg_tuple[3]
    BF_exe = arg_tuple[4]
    orbit_info_bin = arg_tuple[5]
    args = arg_tuple[6]
    logDirs = arg_tuple[7]

    # Find what year this orbit belongs to
    year = bfutils.orbitYear( orbit )

    # We should now have the year for this orbit
    sourceTarPath = os.path.join( args.REMOTE_TAR_DIR, str( year ), "{}archive.tar".format(orbit) )
    destTarPath   = os.path.join( args.SCRATCH_SPACE, "tar", str( year ), "{}archive.tar".format(orbit) )
    # the start time of the orbit
    orbit_start_time = bfutils.orbit_start( orbit )
    
    # BFdirStructure is the year/month/day path inside the output directory for this particular granule
    BFdirStructure= os.path.join( str( orbit_start_time[0:4]), \
                    str( orbit_start_time[4:6]), \
                    str( orbit_start_time[6:8] ) )

    # directory where the untarred data will go for this granule
    untarDir = os.path.join( args.SCRATCH_SPACE, "untarData", BFdirStructure, str(orbit) )
    # outFileDir is the complete, absolute directory to store the basic fusion file in
    outFileDir = os.path.join( BFoutputDir, BFdirStructure)
    # directory where the log file will reside
    proc_log_dir = os.path.join( logDirs['process'], BFdirStructure )

    # Name of the output basic fusion file
    BFfileName = FILENAME.format( orbit, orbit_start_time )
    
    # Finally, save the absolute path of the output file
    outFilePath = os.path.join( outFileDir, BFfileName )

    # Create our location for untarring
    try:
        os.makedirs( untarDir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    # Make the log file directory
    try:
        os.makedirs( proc_log_dir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
   
    # Make the file output directory
    try:
        os.makedirs( outFileDir )
        #logger.debug("Making the output directory: {}".format( outFileDir) )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    #
    # PREPARE THE GRANULE ATTRIBUTES
    #
    curGranule = workflowClass.granule( destTarPath, untarDir, orbit, year=year )
    curGranule.sourceTarPath    = sourceTarPath
    curGranule.destTarPath      = destTarPath
    curGranule.orbit_start_time = orbit_start_time
    curGranule.pathFileList     = os.path.join( untarDir, "MISR_PATH_FILES_{}.txt".format(orbit) )
    curGranule.year             = year
    curGranule.BFoutputDir      = BFoutputDir
    curGranule.BFfileName       = BFfileName 
    curGranule.logFile          = os.path.join( proc_log_dir, '{}process_log.txt'.format(orbit))
    curGranule.metadata_dir     = os.path.join( bf_metadata, BFdirStructure )
    curGranule.inputFileList    = os.path.join( proc_log_dir, '{}input.txt'.format(orbit ) )
    curGranule.BF_exe           = BF_exe
    curGranule.orbit_times_bin  = orbit_info_bin                
    curGranule.outputFilePath   = outFilePath 
    curGranule.proc_log_dir     = proc_log_dir
    curGranule.ddl_path         = os.path.join( proc_log_dir, curGranule.BFfileName + '.ddl')
    curGranule.cdl_path         = os.path.join( proc_log_dir, curGranule.BFfileName + '.cdl')

    return curGranule

#==========================================================================
def pickle_granules( arg ):
    granule_list_pickle = arg[0]
    granuleList = arg[1]
    with open( granule_list_pickle, 'wb' ) as f:
        pickle.dump( granuleList, f )

#==========================================================================
    
def main():
    # Define the argument parser
    parser = argparse.ArgumentParser(description='''This script is the final-ish workflow for the BasicFusion project. It retrieves the tar files from Nearline and processes all orbits from ORBIT_START to ORBIT_END. It is recommended that users wrap this script with a shell script since the arguments to this program are lengthy. \
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
    parser.add_argument("--log_dir", help="Directory where the log directory tree will be created. This includes \
        PBS scripts, log files, and various other administrative files needed for the workflow. Creates directory \
        called {}. Defaults to {}".format(logName, os.getcwd()), type=str, default=os.getcwd(),\
        dest="LOG_OUTPUT" )
    parser.add_argument("--hash_dir", dest="HASH_DIR", help="Where the hashes for all of the tar files reside. Subdirectory structure \
        should be: one directory for each year from 2000 onward, named after the year. Each directory has hash files \
        for each orbit of that year, named #hash.md5 where # is the orbit number.", type=str ) 
    parser.add_argument('--save-interm', help='Don\'t delete any intermediary \
        files on the scratch directory. WARNING! This option prevents script \
        from clearing out files no longer needed, thus increasing disk usage.',
        dest='save_interm', action='store_true')
    parser.add_argument('--queue', help='Set the queue to submit the jobs to.', \
        type=str, choices=['low', 'normal', 'high'], default='normal', dest='queue' )
    ll = parser.add_mutually_exclusive_group(required=False)
    ll.add_argument("-l", "--log", help="Set the log level. Defaults to \
        WARNING.", type=str, choices=['INFO', 'WARNING', 'DEBUG' ] , default="WARNING")

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
    runDir     = bfutils.makeRunDir( logDir )
    # Make the rest of the log directory
    PBSdir          = os.path.join( runDir, 'PBSscripts' )
    PBSdirs         = { 'push' : os.path.join( PBSdir, 'push' ), \
                        'pull_process' : os.path.join( PBSdir, 'pull_process' ), \
                        'process'     : os.path.join( PBSdir, 'process' ), \
                        'pull'  : os.path.join( PBSdir, 'pull' ) \
                      }
    logDir          = os.path.join( runDir, 'logs' )
    logDirs         = { 'submit_workflow'   : os.path.join( logDir, 'submit_workflow' ), \
                        'summary'           : os.path.join( logDir, 'summary' ), \
                        'pull_process'       : os.path.join( logDir, 'pull_process' ), \
                        'misc'              : os.path.join( logDir, 'misc' ), \
                        'process'           : os.path.join( logDir, 'process'), \
                        'push'       : os.path.join( logDir, 'push'), \
                        'pull'              : os.path.join( logDir, 'pull' ) \
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
    scriptPath          = os.path.realpath(__file__)
    projPath            = ''.join( scriptPath.rpartition('basicFusion/')[0:2] )
    orbit_times_txt     = os.path.join( projPath, 'metadata-input', 'data', 'Orbit_Path_Time.txt' )
    SUBMIT_LOG          = os.path.join( logDirs['submit_workflow'], 'submit_workflow.log')
    global VIRT_ENV
    VIRT_ENV            = os.path.join( projPath, 'externLib', 'BFpyEnv', 'bin', 'activate' )
    BFoutputDir         = os.path.join( args.SCRATCH_SPACE, "BFoutput" )
    bf_metadata         = os.path.join( logDirs['misc'], 'BF_metadata' )
    FILENAME            = 'TERRA_BF_L1B_O{0}_{1}_F000_V001.h5'
    BF_exe              = os.path.join( projPath, "bin", "basicFusion" )
    orbit_info_txt      = os.path.join( projPath, "metadata-input", "data", "Orbit_Path_Time.txt" )
    orbit_info_bin      = os.path.join( projPath, "metadata-input", "data", "Orbit_Path_Time.bin" ) 
    FORMAT='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    dateformat='%d-%m-%Y:%H:%M:%S'
    
    #
    # ENABLE LOGGING
    #

    logFormatter = logging.Formatter(FORMAT, dateformat)
    rootLogger = logging.getLogger()

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    fileHandler = logging.FileHandler( SUBMIT_LOG , mode='a' )
    fileHandler.setFormatter(logFormatter)
    
    rootLogger.addHandler(fileHandler)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(ll)
 
    logger = logging.getLogger('submit_workflow')

    logger.info('Arguments: \n{}'.format(sys.argv))
    logger.debug("Using virtual environment detected here: {}".format(VIRT_ENV))
    
    #          ---------------
    # CHECK VARIOUS ENVIRONMENT ATTRIBUTES
    #          ---------------

    try:
        os.makedirs( BFoutputDir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    # Make the directory to store input file listings
    try:
        os.makedirs( bf_metadata )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    if not os.path.isfile( BF_exe ):
        errMsg="The basic fusion executable was not found at: {}".format( BF_exe)
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


    # ----------------------------
    # SAVE THE LOG DIRECTORY JSON
    # ----------------------------
    
    json_log = os.path.join( logDirs['misc'], 'log_dirs.json' )
    logger.info("Saving the log directory as JSON dump at: {}".format(json_log))

    with open( json_log, 'wb' ) as f:
        json.dump( logDirs, f, sort_keys=True, indent=4, separators=(',', ': ') )

    # Check that all directory arguments actually exist in the filesystem
    if args.HASH_DIR and not os.path.isdir( args.HASH_DIR ):
        raise RuntimeError( "{} is not a valid directory!".format( args.HASH_DIR ) )
    if not os.path.isdir( args.LOG_OUTPUT ):
        raise RuntimeError( "{} is not a valid directory!".format( args.LOG_OUTPUT ) )
    if not os.path.isdir( args.SCRATCH_SPACE ):
        raise RuntimeError( "{} is not a valid directory!".format( args.SCRATCH_SPACE ) )
        
    # Argument sanitizing
    if args.HASH_DIR:
        args.HASH_DIR = os.path.abspath( args.HASH_DIR )
    args.LOG_OUTPUT = os.path.abspath( args.LOG_OUTPUT )
    args.SCRATCH_SPACE = os.path.abspath( args.SCRATCH_SPACE )
    
    # ----------------------------
    # FIND JOB PARTITION
    # ----------------------------

    # Time to determine how to split up the jobs. Each job quanta will be responsible for
    # at most args.granule number of orbits.
    numOrbits = args.ORBIT_END - args.ORBIT_START + 1
    logger.info("Processing {} number of orbits.".format(numOrbits))

    numFullJobs = numOrbits / args.granule
    lastJobOrbits = numOrbits % args.granule
    logger.debug("{} jobs with {} orbits. Last job has {} orbits".format(numFullJobs, args.granule, lastJobOrbits))

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

    # =============================
    # =     MAKE GRANULE LIST     =
    # =============================

    logger.info('Preparing the granule list.')
    # Prepare the granule list
    count = 0
    p = Pool(10)
    pickle_list = []
    for i in quantas:
        logger.info('Making granule list for quanta {}'.format(count))
        arg_list = []

        # ADDITIONS #############################################
#        orbits = []
#        orbits += range( 21200, 21250 )
#        orbits += range( 37380, 38330 )
#        orbits += range( 2560, 2610 )
#        orbits += range( 3675, 3725 )
#        orbits += range( 3875, 3925 )
#        orbits += range( 4910, 4960 )
#        orbits += range( 6130, 6180 )
#        for orbit in orbits:
#            if orbit % 10 == 0:
#                print( orbit )
#            arg_list.append( (orbit, bf_metadata, FILENAME, BFoutputDir, BF_exe, \
#                orbit_info_bin, args, logDirs ) )
        #########################################################
        for orbit in range( i.orbitStart, i.orbitEnd + 1 ):
            arg_list.append( (orbit, bf_metadata, FILENAME, BFoutputDir, BF_exe, \
                orbit_info_bin, args, logDirs ) )

        granuleList = p.map( make_granule, arg_list )
        # Remove all None elements from granuleList
        granuleList = [x for x in granuleList if x is not None ]

        granule_list_pickle = os.path.join( logDirs['misc'], \
            'granule_list_{}_{}.P'.format(i.orbitStart, i.orbitEnd) )
        pickle_list.append( (granule_list_pickle, granuleList) )

        i.granule_list = granule_list_pickle
        count += 1

    # Pickle the granule lists
    p.map( pickle_granules, pickle_list )

    p.close()

    for pick in pickle_list: 
        with open( pick[0], 'wb' ) as f:
            pickle.dump( pick[1], f )

    # ======================================
    # =          MAKE PBS SCRIPTS          =
    # ======================================


    # Make the PBS scripts for all the jobs
    for i in quantas:
        pull_name    = 'pull_{}_{}'.format( i.orbitStart, i.orbitEnd )
        process_name = "process_{}_{}".format(i.orbitStart, i.orbitEnd)
        push_name = "push_{}_{}".format( i.orbitStart, i.orbitEnd )
        
        summaryLog = os.path.join( logDirs['summary'], process_name + "_summary.log" )
        pull_pbs = os.path.join( PBSdirs['pull'], pull_name + '.pbs' )
        pull_script = os.path.join( os.path.dirname(scriptPath), 'pull.py' )
        log_file = os.path.join( logDirs['pull'], pull_name + '.log' )

     
        # -----------------------------------
        # - MAKE THE GLOBUS PULL PBS SCRIPT -
        # -----------------------------------
        with open( pull_pbs, 'w' ) as f:
            pull_args = { \
                     'pull_name'    : pull_name,        \
                     'pull_script'  : pull_script,      \
                     'log_file'     : log_file,         \
                     'summary_log'  : summaryLog,       \
                     'granule_list' : i.granule_list,   \
                     'src_id'       : args.REMOTE_ID,   \
                     'dest_id'      : args.HOST_ID,     \
                     'json_log'     : json_log,         \
                     'save_interm'  : args.save_interm, \
                     'queue'        : args.queue        \
                   }
            pbs_str = makePBS_pull( pull_name, pull_script, log_file, summaryLog, \
                i.granule_list, args.REMOTE_ID, args.HOST_ID, json_log, args.save_interm, \
                args.queue )
            f.write( pbs_str )
        i.PBSfile['pull'] = pull_pbs
        
        # --------------------------------------
        # - MAKE THE GLOBUS PRCESS PBS SCRIPTS -
        # --------------------------------------

        logger.info("Making Globus pull PBS script for job {}.".format(process_name))
        i.PBSfile['process'] = makePBS_process( i.granule_list, \
            PBSdirs['process'], args.REMOTE_ID, args.HOST_ID, \
            process_name, logDirs['process'], summaryLog, \
            [ i.orbitStart, i.orbitEnd], json_log, args.SCRATCH_SPACE, \
            args.MISR_PATH, args.HASH_DIR, projPath, args.GLOBUS_PRL, \
            args.NODES, args.PPN, args.modis_missing, args.save_interm, \
            args.queue )

        # ------------------------------------
        # - MAKE THE GLOBUS PUSH PBS SCRIPTS -
        # ------------------------------------

        i.PBSfile['push'] = makePBS_push( i.granule_list, PBSdirs['push'], args.REMOTE_ID, args.HOST_ID, \
            args.REMOTE_BF_DIR, push_name, json_log, summaryLog, projPath, os.path.join( logDirs['push'], \
            push_name + "_log.log"), args.GLOBUS_PRL, args.save_interm, args.queue ) 


    # ==================================
    # = DEFINE AND SUBMIT THE WORKFLOW =
    # ==================================

    chain = workflow.JobChain('pbs')

    prevQuant = None  
    prevQuant_2 = None  
    # Now that all quantas have been prepared, we can submit their jobs to the queue
    for i in quantas:
        i.pull_job = workflow.Job( i.PBSfile['pull'] )
        i.proc_job = workflow.Job( i.PBSfile['process'] )
        i.push_job = workflow.Job( i.PBSfile['push'] )

        chain.add_job( i.pull_job )
        chain.add_job( i.proc_job )
        chain.add_job( i.push_job )

        chain.set_dep( i.proc_job, i.pull_job, 'afterany' )
        chain.set_dep( i.push_job, i.proc_job, 'afterany' )

        if prevQuant:
            chain.set_dep( i.pull_job, prevQuant.pull_job, 'afterany' )
            chain.set_dep( i.proc_job, prevQuant.proc_job, 'afterany' )

        if prevQuant_2:
            chain.set_dep( i.pull_job, prevQuant_2.push_job, 'afterany' )

        prevQuant_2 = prevQuant
        prevQuant = i

    logger.info('Submitting jobs to the queue')
    # Submit the job chain, then print the map representation
    map_str = chain.submit( print_map = True )
    logger.info('\n' + map_str)

if __name__ == '__main__':
    main()
