import argparse
import bfutils
from bfutils import orbit
from mpi4py import MPI
#from concurrent.futures import ProcessPoolExecutor
#from mpi4py.futures import ProcessPoolExecutor
import os
import hashlib
import logging
import sys
from mpi4py import futures
import errno

#MPI Variables
mpi_comm = MPI.COMM_WORLD
mpi_rank = MPI.COMM_WORLD.Get_rank()
mpi_size = MPI.COMM_WORLD.Get_size()
rank_name = "MPI_Rank {}".format(mpi_rank)

MAX_WORKERS = 64

#
# ENABLE LOGGING
#

logFormatter = logging.Formatter( bfutils.constants.LOG_FMT, bfutils.constants.LOG_DATE_FMT )
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger = logging.getLogger()
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel( logging.DEBUG )
logger = logging.getLogger( rank_name )

# MPI control flags
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
MPI_SLAVE_RETURN        = 10

class BFFile(object):

    chg_attr_bin = None

    def __init__ ( self, path = None, cdl_dir = None, dry_run = None ):
        
        # Publics
        self.old_fsize = 0
        self.new_fsize = 0
        self.orbit = 0
        self.dry_run = dry_run
        # Privates
        self._path = path
        self._cdl_dir = cdl_dir     
        self._cdl_path = None
        self._hash_val = ''
        self._file_bn = None

    def set_hash( self, hash ):
        self._hash_val = hash

    def get_hash( self ):
        return self._hash_val

    def set_cdl_dir( self, dir ):
        self._cdl_dir = dir

    def get_cdl_dir( self ):
        return self._cdl_dir

    def set_path(self, path):
        self._path = path

    def get_path(self):
        return self._path

    def get_file_bn(self):
        if self._file_bn is None:
            self._file_bn = os.path.basename( self._path )
        return self._file_bn

def do_hash( data ):
    hashReadSize = 100000000
    
    hasher = hashlib.sha256()
    with open( data.get_path(), 'rb' ) as f:
        buf = f.read( hashReadSize )
        while buf:
            hasher.update(buf)
            buf = f.read( hashReadSize )

    # Get the actual hash of the tar file
    data.set_hash( hasher.hexdigest() )
    data.orbit = orbit.bf_file_orbit( data.get_path() )

    return data

def mpi_rank_terminate( data ):
    logger.debug("Terminating.")
    sys.exit(0)

def worker():

    while True:
        logger.debug("Waiting to receive job")
        stat = MPI.Status()
        mpi_comm.send( 0, dest = MPI_MASTER, tag = MPI_SLAVE_IDLE )
        data = mpi_comm.recv( source = MPI_MASTER, status=stat )
        tag = stat.Get_tag()
        if tag == MPI_SLAVE_TERMINATE:
            return
        func = data[0]
        data = data[1]
        

        logger.info("Received job {}.".format( func ) )

        ret_val = func( data )

        logger.info('Sending back data.')

        mpi_comm.send( ret_val, dest = MPI_MASTER, tag = MPI_SLAVE_RETURN )
    

def main():
    
    parser = argparse.ArgumentParser(description='This script generates an \
        MD5 hash of all Basic Fusion files in the given directory.',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument('dir', help='Directory containing BF files.', 
        type = str, nargs='+' )
    parser.add_argument('--out-dir', help='Output directory that will contain \
        hash text files.', type = str, default=os.path.join('.', 'hash_dir' ))
    args = parser.parse_args()
    
    bf_files = []
    orbits = {}
    for dir in args.dir:
        logger.info('Recursing through directory {}...'.format(dir))
        # Recurse through directory to find all BF files 
        for root, dirs, files in os.walk( dir ):
           for file in files:
                path = os.path.abspath( os.path.join( root, file ) )
                
                if orbit.is_bf_file( file ):
                    bf_files.append( BFFile( path ) )
                    o = orbit.bf_file_orbit( file )
                    if o in orbits:
                        raise RuntimeError('Found two instances of orbit {}:\n{}\n{}'.format( 
                            o, orbits[o], path ) )
                    orbits[o] = path

    logger.info('Done.')

    bf_len_orig = len(bf_files)

    bf_files_hashed = []

#    logger.info('Calling MPI map on BF files...')
#    with ProcessPoolExecutor( max_workers = MAX_WORKERS ) as executor:
#        future = executor.map( do_hash, bf_files )
#    logger.info('Done.')
#
#    for i in future:
#        print(i.get_hash())

    #print(future.result())

    #s_list = sorted( bf_list, key = lambda x: x.orbit )
    #with open(args.out_file, 'w') as f:
    #    for i in future:
    #        f.write('{}: {}\n'.format( os.path.basename( i.get_path(), i.get_hash() ) ) )
        
    # Perform a map operation
    while len( bf_files_hashed ) != bf_len_orig:
        
        # Check for slaves that want to communicate
        stat = MPI.Status()                 # Class that keeps track of MPI information    
        
        if len(bf_files) != 0:
            mpi_comm.probe( status = stat )
        else:
            mpi_comm.probe( status = stat, tag = MPI_SLAVE_RETURN )
            

        if stat.Get_tag() == MPI_SLAVE_RETURN:
            logger.info('Gathering return value from slave {}'.format( stat.source) )
            ret_val = mpi_comm.recv( source = stat.source )
            bf_files_hashed.append( ret_val )
       
            i = ret_val
            s_time = orbit.orbit_start( i.orbit )
            dir = s_time[0:4] + '.' + s_time[4:6]
            month_dir = os.path.join( args.out_dir, dir )
            hash_file = os.path.join( month_dir, '{}.sha256'.format( i.get_file_bn() ) )
            try:
                os.makedirs( month_dir )
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            with open( hash_file, 'w' ) as f:
                f.write('{}\n'.format(i.get_hash()))

        elif stat.Get_tag() == MPI_SLAVE_IDLE and len(bf_files) != 0:

            data = bf_files.pop()
            logger.info("Sending slave {} hash job: {}.".format( stat.source, os.path.basename( data.get_path()) ) )

            mpi_comm.recv( source=stat.source, tag = MPI_SLAVE_IDLE )
            mpi_comm.send( (do_hash, data ), dest = stat.source )


   
#    try:
#        os.makedirs( args.out_dir )
#    except OSError as e:
#        if e.errno != errno.EEXIST:
#            raise
#
#    for i in bf_files_hashed:
#        s_time = orbit.orbit_start( i.orbit )
#        dir = s_time[0:4] + '.' + s_time[4:6]
#        month_dir = os.path.join( args.out_dir, dir )
#        hash_file = os.path.join( month_dir, '{}.sha256'.format( i.get_file_bn() ) )
#        try:
#            os.makedirs( month_dir )
#        except OSError as e:
#            if e.errno != errno.EEXIST:
#                raise
#        with open( hash_file, 'w' ) as f:
#            f.write('{}\n'.format(i.get_hash()))

    logger.info('Done processing')

    assert len( bf_files_hashed ) == bf_len_orig


if __name__ == '__main__':
    if mpi_size == 1:
        raise RuntimeError('Need more than 1 MPI process!')

    try:
        if mpi_rank == 0:
            main()
        else:
            worker()
    except SystemExit:
        pass
    except:
        logger.exception('Encountered exception.')
        mpi_comm.Abort()

    if mpi_rank == 0:
        # We need to terminate all slaves, or else the entire process might hang!
        # mpi_comm.Abort()
        for i in xrange(1, mpi_size ):
            logger.debug("Terminating slave {}".format(i))
            mpi_comm.recv( source = i, tag = MPI_SLAVE_IDLE )
            mpi_comm.send( (mpi_rank_terminate, 0), dest = i )
