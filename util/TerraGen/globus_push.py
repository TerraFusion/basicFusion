import argparse
from workflowClass import granule
import subprocess
import logging

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
    parser.add_argument("LOG_TREE", help="A Python pickle of the job log dictionary.", type=str)
    parser.add_argument("-g", "--num-transfer", help="The maximum number of Globus transfer requests to make. Defaults \
        to 1.", dest='num_transfer', type=int, default=1)    
    parser.add_argument("-l", "--log-level", help="Set the log level. Defaults to WARNING.", type=str, \
        choices=['INFO', 'DEBUG' ] , default="WARNING")
    args = parser.parse_args()

    #
    # ENABLE LOGGING
    #

    # Define log level
    if args.log == 'DEBUG':
        ll = logging.DEBUG
    elif args.log == 'INFO':
        ll = logging.INFO
    else:
        ll = logging.WARNING
    
    logFormatter = logging.Formatter(FORMAT, dateformat)
    logger = logging.getLogger()
    logger.setLevel(ll)

    # Unpickle the out_gran_list
    with open( args.OUT_GRAN_LIST, 'rb' ) as f:
        granuleList = pickle.load( f )

    # Unpickle the log tree
    with open( args.LOG_TREE, 'rb' ) as f:
        logTree = pickle.load( f )

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
        batchPath = os.path.join( transferPath, progName + "_push_list_{}.txt".format(i))
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

                remote_dir       = os.path.join( args.REMOTE_DIR, str(year), str(month), str(day) )
                remote_file_path = os.path.join( remote_dir, granule.BFfile )

                line = '{} {}'.format( granule.outputFilePath, remote_file_path )
                batchFile.write( line )

    # Write the extra files to the last Globus batch file
    with open( splitList[args.num_transfer - 1], 'a') as batchFile:
        for j in xrange( granExtra ):
            curGranule = granuleList[ args.num_transfer * granPerTransfer + j ]
            orbit_start = curGranule.orbit_start_time

            year    = orbit_start[0:4]
            month   = orbit_start[4:6]
            day     = orbit_start[6:8]

            remote_dir       = os.path.join( args.REMOTE_DIR, str(year), str(month), str(day) )
            remote_file_path = os.path.join( remote_dir, granule.BFfile )

            line = '{} {}'.format( granule.outputFilePath, remote_file_path )
            
            batchFile.write( line ) 

if __name__ == '__main__':
    main()
