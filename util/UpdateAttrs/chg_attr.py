import argparse
import subprocess as subproc
import bfutils.orbit as orbit
import bfutils
import os
import logging
import sys
from multiprocessing import Pool
import uuid

MAX_WORKERS = 32
#NCDUMP = '/u/sciteam/clipp/basicFusion/externLib/hdf/bin/ncdump'
NCDUMP = 'ncdump'

class BFFile(object):

    chg_attr_bin = None

    def __init__ ( self, path = None ):
        
        # Publics
        self.old_fsize = 0
        self.new_fsize = 0
        self.orbit = 0
        # Privates
        self._path = path
        
    def set_path(self, path):
        self._path = path

    def get_path(self):
        return self._path

    
def chg_attr_worker( bf_file ):

    bf_file.old_fsize = os.path.getsize( bf_file.get_path() )
    bf_file.orbit = orbit.bf_file_orbit( bf_file.get_path() )
    def gen_cdl( bf_path, out_path ):
        # Generate CDL
        args = [ NCDUMP, '-h', bf_file.get_path() ]
        cdl = bf_file.get_path() + '.cdl.old'
        try:
            with open( out_path, 'w' ) as f:
                subproc.check_call( args, stdout=f, stderr=f )
        except subproc.CalledProcessError:
            print('ERROR: {}'.format( ' '.join(args) ))
            raise

    # Generate old cdl
    gen_cdl( bf_file.get_path(), bf_file.get_path() + '.cdl.old' )

    # Create temporary file for input to chg_attr program
    tmp_file = str( uuid.uuid4() ) + '.txt.tmp'

    with open( tmp_file, 'w' ) as f:
        f.write('{}\n'.format( bf_file.get_path() ) )

    # Run the attribute change program
    try:
        args = [ bf_file.chg_attr_bin, tmp_file ]
        subproc.check_call( args, stderr=subproc.STDOUT )
    except subproc.CalledProcessError as e:
        print( 'ERROR: {}'.format(' '.join(args)) )
        raise
    finally:
        os.remove( tmp_file )
       
    # Get new file size
    bf_file.new_fsize = os.path.getsize( bf_file.get_path() )

    # Generate new CDL
    gen_cdl( bf_file.get_path(), bf_file.get_path() + '.cdl' )

    return bf_file

def main():
    parser = argparse.ArgumentParser( formatter_class = argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument('in_dir', help='Directory where BF files are stored.', 
        type = str )
    parser.add_argument('attr_bin', help='Path to the change_attribute binary.',
        type = str )
    parser.add_argument('fsize_txt', help='Output file that contains before/after \
        size of BF file during attribute change.', 
        default = os.path.join('.', 'bf_sizes.txt'), nargs='?' )

    args = parser.parse_args()

    #
    # ENABLE LOGGING
    #

    logFormatter = logging.Formatter( bfutils.constants.LOG_FMT, \
        bfutils.constants.LOG_DATE_FMT )
    rootLogger = logging.getLogger()

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel( logging.DEBUG )
 
    logger = logging.getLogger(__name__)
    

    bf_files = []

    BFFile.chg_attr_bin = args.attr_bin

    logger.info('Parsing directories...')
    # Recurse through directory to find all BF files    
    for root, dirs, files in os.walk( args.in_dir ):
       for file in files:
            path = os.path.abspath( os.path.join( root, file ) )
            
            f_type = orbit.isBFfile( [path] )
            if orbit.is_bf_file( file ):
                bf_files.append( BFFile( path ) )

    logger.info('Done.')

    logger.info('Performing parallel work...')
    # Execute worker threads
    p = Pool( MAX_WORKERS )
    ret_bf_files = p.map( chg_attr_worker, bf_files )
    p.close()
    p.join()
    logger.info('Done.')

    # Sort the bf_files by orbit, then print result to output file
    bf_files_sort = sorted( ret_bf_files, key = lambda k: k.orbit )

    with open( args.fsize_txt, 'w') as f:
        for file in bf_files_sort:
            f.write( '{}: {} {}\n'.format( os.path.basename( file.get_path()), file.old_fsize, file.new_fsize ) )

if __name__ == '__main__':
    main()
