from bfutils.globus import GlobusTransfer
from bfutils.orbit import orbit_start
import argparse
import os

def main():
    parser = argparse.ArgumentParser(description="This script downloads the archive tar files from Nearline onto Blue Waters.")

    parser.add_argument('ORBIT_START', help='Starting orbit.', type=int )
    parser.add_argument('ORBIT_END', help='Ending orbit', type=int )
    parser.add_argument('-s', '--src', dest='SRC_DIR', help='Source tar directory.', required=True )
    parser.add_argument('-d', '--dest', dest='DEST_DIR', help='Destination tar directory', required=True )
    parser.add_argument('-p', dest='globus_split', help='Number of Globus transfers to use.', action='store_true', default=1)
    args = parser.parse_args()


    if args.SRC_DIR:
        tar_dir = args.SRC_DIR
    if args.DEST_DIR:
        dst_dir = args.DEST_DIR

    NL = 'd599008e-6d04-11e5-ba46-22000b92c6ec'
    BW = 'd59900ef-6d04-11e5-ba46-22000b92c6ec'

    transfer = GlobusTransfer( NL, BW )

    for orbit in xrange( args.ORBIT_START, args.ORBIT_END +1 ):
        o_start = orbit_start( orbit )
        year = o_start[0:4]
        src_file = os.path.join( tar_dir, str(year), '{}archive.tar'.format(orbit) )
        dst_file = os.path.join( dst_dir, str(year), '{}archive.tar'.format(orbit) )

        transfer.add_file( src_file, dst_file )

    transfer.transfer( parallel=args.globus_split, batch_dir='.', \
        label='{}_{}_tar'.format( args.ORBIT_START, args.ORBIT_END ) )
        
    

if __name__ == '__main__':
    main()
