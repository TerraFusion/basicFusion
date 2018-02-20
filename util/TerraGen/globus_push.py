import argparse
import workflowClass
import subprocess
import logging
import pickle
import os
import sys
import errno
import json
import datetime as dt

summaryLog=''
jobName=''

FORMAT='%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s'
dateformat='%d-%m-%Y:%H:%M:%S'

def main():
    parser = argparse.ArgumentParser(description="This script handles transferring the Basic Fusion output granules to \
        the remote Globus endpoint.")
    parser.add_argument("OUT_GRAN_LIST", help="A Python pickle containing a list of workflowClass.granule objects. This list \
        contains, among other things, paths to the Basic Fusion granules.", type=str)
    parser.add_argument("REMOTE_ID", help="The Globus endpoint identifier of the destination file system.", type=str )
    parser.add_argument("REMOTE_DIR", help="The top level directory on the remote machine where the Basic Fusion files \
        will be transferred to. This directory will be populated with the canonical BasicFusion directory structure.", \
        type=str )
    parser.add_argument("HOST_ID", help="The Globus endpoint identifier of the machine running this script.", type=str )
    parser.add_argument("JOB_NAME", help="A name for this  job. Used to make unique log files, intermediary files, and any \
        other unique identifiers needed.", type=str)
    parser.add_argument("LOG_TREE", help="A JSON dump of the job log dictionary.", type=str)
    parser.add_argument("SUMMARY_LOG", help="Path to the summary log file.", type=str)
    parser.add_argument("-g", "--num-transfer", help="The maximum number of Globus transfer requests to make. Defaults \
        to 1.", dest='num_transfer', type=int, default=1)    
    parser.add_argument("-l", "--log-level", help="Set the log level. Defaults to WARNING.", type=str, \
        choices=['INFO', 'DEBUG' ] , default="WARNING", dest='log_level')
    parser.add_argument('--save-interm', help='Don\'t delete any intermediary \
        files on the scratch directory. WARNING! This option prevents script \
        from clearing out files no longer needed, thus increasing disk usage.',
        dest='save_interm', action='store_true')
    args = parser.parse_args()
    print( type(args) )
    #
    # ENABLE LOGGING
    #

    # Define log level
    if args.log_level == 'DEBUG':
        ll = logging.DEBUG
    elif args.log_level == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING
    
    logFormatter = logging.Formatter(FORMAT, dateformat)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    logFormatter = logging.Formatter(FORMAT, dateformat)
    logger = logging.getLogger( __name__ )
    logger.addHandler(consoleHandler)
    fileHandler = logging.FileHandler( args.SUMMARY_LOG )
    fileHandler.setLevel( logging.WARNING )
    fileHandler.setFormatter(logFormatter)
    loggerogger.addHandler( fileHandler )
    logger.setLevel(ll)

    # -------------
    # - VARIABLES -
    # -------------
    CHECK_SUM='--no-verify-checksum'

    global summaryLog
    summaryLog = args.SUMMARY_LOG

    global jobName
    jobName = args.JOB_NAME

    # Unpickle the out_gran_list
    with open( args.OUT_GRAN_LIST, 'rb' ) as f:
        granuleList = pickle.load( f )

    # load the log tree
    with open( args.LOG_TREE, 'rb' ) as f:
        logTree = json.load( f )

    if args.num_transfer < 1:
        raise ValueError("--num-transfer argument can't be less than 1!")

    # Iterate through the granuleList and split it up into num_transfer lists
    numGran = len( granuleList )
    granPerTransfer = numGran / args.num_transfer
    granExtra       = numGran % args.num_transfer
         
    logger.debug("granPerTransfer: {}".format(granPerTransfer))
    logger.debug("granExtra: {}".format(granExtra))

    # This will store the path for each batch transfer file for Globus
    splitList = [] 
    transferPath = logTree['misc']  # This will store the directory of our batch files

    for i in xrange( args.num_transfer ):
        # Add the filename of our new split-file to the list
        batchPath = os.path.join( transferPath, args.JOB_NAME + "_push_list_{}.txt".format(i))
        splitList.append( batchPath  )

        logger.info("Making new Globus batch file: {}".format(splitList[i]))

        # Open the new batch file
        with open( splitList[i], 'w' ) as batchFile:
            logger.debug("Writing {}".format( os.path.basename(splitList[i])))
            
            # For each granule, gather all the information we need to write a line in the
            # batch file.
            for j in xrange( granPerTransfer ):
                curGranule = granuleList[ i * granPerTransfer + j ]
                orbit_start = curGranule.orbit_start_time

                year    = orbit_start[0:4]
                month   = orbit_start[4:6]
                day     = orbit_start[6:8]

                remote_dir       = os.path.join( args.REMOTE_DIR, '{}.{}'.format(year, month) )
                remote_file_path = os.path.join( remote_dir, curGranule.BFfileName )

                if os.path.isfile( curGranule.outputFilePath ):
                    line = '{} {}'.format( curGranule.outputFilePath, remote_file_path )
                    batchFile.write( str(line) + '\n' )

                
    logger.info("Writing the extra files to last Globus batch file.")
    # Write the extra files to the last Globus batch file
    with open( splitList[args.num_transfer - 1], 'a') as batchFile:
        for j in xrange( granExtra ):
            curGranule = granuleList[ args.num_transfer * granPerTransfer + j ]
            orbit_start = curGranule.orbit_start_time

            year    = orbit_start[0:4]
            month   = orbit_start[4:6]
            day     = orbit_start[6:8]

            remote_dir       = os.path.join( args.REMOTE_DIR, '{}.{}'.format(year, month) )
            remote_file_path = os.path.join( remote_dir, curGranule.BFfileName )

            
            if os.path.isfile( curGranule.outputFilePath ):
                line = '{} {}'.format( curGranule.outputFilePath, remote_file_path )            
                batchFile.write( str(line) + '\n' )

    logger.info("Submitting the transfer tasks to Globus...")

    submitIDs = []

    # Set deadline for 2 days
    delta = dt.timedelta(days=2)
    now = dt.datetime.utcnow()
    deadline = delta + now
    
    for i in splitList:
    
        subprocCall = ['globus', 'transfer', CHECK_SUM, args.HOST_ID +':/', args.REMOTE_ID + ':/', '--batch', \
                       '--label', os.path.basename(i).replace('.txt', ''), '--deadline' , deadline.strftime("%Y-%m-%d %H:%M:%S") ]
        with open( i, 'r' ) as stdinFile:
            subProc = subprocess.Popen( subprocCall, stdin=stdinFile, stdout=subprocess.PIPE, stderr=subprocess.PIPE ) 
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

        # Go ahead and write these to stdout
        print( stdout )

        retCode = subProc.returncode

        if retCode != 0:
            with open( args.SUMMARY_LOG, 'a') as summaryLog:
                summaryLog.write("ERROR: Globus transfer failed! See {} for more details.\n".format(globPullLog))

    
    # The transfers have been submitted. Need to wait for them to complete...
    for id in submitIDs:
        logger.info("Waiting for Globus task {}".format(id))
        subprocCall = ['globus', 'task', 'wait', id ]

        retCode = subprocess.call( subprocCall ) 
        
        # The program will not progress past this point until 'id' has finished
        if retCode != 0:
            with open( args.SUMMARY_LOG, 'a') as summaryLog:
                summaryLog.write("Globus task failed with retcode {}! See: {}\n".format(retCode, globPullLog))

        logger.info("Globus task {} completed with retcode: {}.".format(id, retCode))

    if not args.save_interm:
        logger.info("Deleting all Basic Fusion granules on the host machine.")
        for i in granuleList:
            try:
                os.remove( i.outputFilePath )
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
    
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        with open( summaryLog, 'a') as f:
            f.write('{}: Failed to push data. See log files for more info.\n'.format(jobName))
        raise
