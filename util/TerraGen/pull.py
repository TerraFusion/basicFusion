import argparse
import errno
import pickle
from bfutils import globus
import workflowClass
import sys
import logging
import os
import json
import datetime as dt

logDirs = None
logger = None

FORMAT='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
dateformat='%d-%m-%Y:%H:%M:%S'
logFormatter = logging.Formatter(FORMAT, dateformat)

fileHandler = logging.FileHandler( LOG_FILE )
fileHandler.setFormatter(logFormatter)

rootLogger = logging.getLogger()
rootLogger.addHandler(fileHandler)
rootLogger.setLevel(logging.DEBUG)
logger = logging.getLogger(__file__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("GRANULE_LIST", help="Path to a Python pickle \
        containing a list of granule objects that defines the workflow \
        for each granule.", type=str)
    parser.add_argument("SRC_ID", help='The source Globus endpoint.')
    parser.add_argument('DEST_ID', help='The destination Globus endpoint.')
    parser.add_argument("LOG_DIRS", help="Path to a JSON dump \
        containing paths to the workflow log directories.", \
        type=str)
    parser.add_argument('JOB_NAME', help="Name of the job.", type=str )
    parser.add_argument('--save-interm', help='Don\'t delete any intermediary \
        files on the scratch directory. WARNING! This option prevents script \
        from clearing out files no longer needed, thus increasing disk usage.',
        dest='save_interm', action='store_true')
    parser.add_argument("-g", "--globus-split", help="Globus parallelism. Defines how many Globus transfer requests are submitted \
        for any given transfer job. Defaults to 1.", dest='GLOBUS_SPLIT', type=int, default=1)
    

    args = parser.parse_args()
        
    global logDirs
    with open( args.LOG_DIRS, 'rb' ) as f:
        logDirs = json.load( f )

    with open( args.GRANULE_LIST, 'rb' ) as f:
        granule_list = pickle.load( f )

    LOG_FILE = os.path.join( logDirs['pull'], args.JOB_NAME + '.log' )
    #
    # ENABLE LOGGING
    #
    fileHandler = logging.FileHandler( args.SUMMARY_LOG )
    fileHandler.setLevel( logging.WARNING )
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler( fileHandler )

    transfer = globus.GlobusTransfer( args.SRC_ID, args.DEST_ID )

    orbit_min = sys.maxsize
    orbit_max = -sys.maxsize - 1
    for granule in granule_list:
        transfer.add_file( granule.sourceTarPath, granule.destTarPath )
        if granule.orbit < orbit_min:
            orbit_min = granule.orbit
        if granule.orbit > orbit_max:
            orbit_max = granule.orbit

    batch_dir = os.path.join( logDirs['pull'], 'batch' )
    try:
        os.makedirs( batch_dir )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # Set deadline for 2 days
    delta = dt.timedelta(days=2)
    now = dt.datetime.utcnow()
    deadline = delta + now

    logger.info('Submitting Globus Transfer and waiting for completion...')
    transfer.transfer( parallel=args.GLOBUS_SPLIT, label='tar_pull_{}_{}'.format( orbit_min, \
        orbit_max), batch_dir = batch_dir, \
        deadline = deadline.strftime("%Y-%m-%d %H:%M:%S") )

    transfer.wait()
    logger.info('Done')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.exception('Fatal exception!')
        raise
