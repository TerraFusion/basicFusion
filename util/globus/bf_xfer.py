import argparse
from bfutils import globus
import re
import datetime as dt
import os 
import sys

NL='d599008e-6d04-11e5-ba46-22000b92c6ec'
# NOTE: 'ROGER' and 'Condo' are used interchangeably
ROGER='da262cbf-6d04-11e5-ba46-22000b92c6ec'
bf_re = 'TERRA_BF_L1B_O[0-9]+_[0-9]+_F000_V001.h5$'

def main():
    parser = argparse.ArgumentParser( description='Initiates transfers between \
        ROGER and Nearline endpoints.')
    parser.add_argument('src_id', help='Source Globus ID', type=str)
    parser.add_argument('src_dir', help='Source directories to recursively \
        transfer Basic Fusion files.', nargs='+', type=str)
    parser.add_argument('dest_id', help='Destination Globus ID.', type=str)
    parser.add_argument('dest_dir', help='Destination directory to transfer to.')
    parser.add_argument('-g', '--globus-split', help='Globus parallelism. \
        Specifies how many globus transfers to use. Defaults to 1.', 
        type=int, default=1, dest='globus_split' )
    parser.add_argument('--sync-level', help='Globus sync level.',
       choices=['exists', 'size', 'mtime', 'checksum'], type=str ) 
    parser.add_argument('--local-src', help='src_dir is a local directory. \
        Bypass the globus ls command in favor for a direct recursive search of the \
        directory.', action='store_true')
    parser.add_argument('-r', '--range', dest='range', help='Select only the \
        specified range of orbits to transfer.', type=int, nargs=2 )
    parser.add_argument('--orbit-label', '-o', help='Mark Globus label \
        with range of orbits being transferred.', action='store_true', 
        dest='orbit_label')

    args = parser.parse_args()

    transfer = globus.GlobusTransfer( args.src_id, args.dest_id )

    files = []
    if args.local_src:
        for dir in args.src_dir:
            for root, directories, fnames in os.walk( dir ):
                for fname in fnames:
                    path = os.path.join( root, fname )
                    if re.search( bf_re, path ):
                        files.append( path )
    else:
        for dir in args.src_dir:
            files += globus.ls( args.src_id, dir, True )
    
    omin=sys.maxint
    omax=0

    for file in files:
        if re.search( bf_re, file ):
            fname = file.split( os.path.sep )[-1]
            
            # Extract time
            ftime = fname.split('_')[4]
            year = ftime[0:4]
            month = ftime[4:6]
            day = ftime[6:8]
            
            match = re.search( '_O[0-9]+_', fname )
            orbit = int( fname[ match.start() + 2 : match.end() - 1 ])
            # If doing only a range, check if orbit is in range
            if args.range:
                if orbit < args.range[0] or orbit > args.range[1]:
                    continue

            dest_path = os.path.join( args.dest_dir, "{}.{}".format(year, month) , fname )
            transfer.add_file( file, dest_path )

            if orbit < omin:
                omin = orbit
            if orbit > omax:
                omax=orbit

    delta = dt.timedelta( days=30 )
    now = dt.datetime.utcnow()
    deadline = delta + now

    label = None
    if args.orbit_label:
        label = '{}_{}'.format(omin, omax)
        
    transfer.transfer( parallel = args.globus_split, sync_level = args.sync_level, 
        deadline = deadline.strftime("%Y-%m-%d %H:%M:%S"), label = label ) 

if __name__ == '__main__':
    main()
