import argparse
from bfutils import globus
import bfutils
import re
import datetime as dt
import os 
import sys

NL='d599008e-6d04-11e5-ba46-22000b92c6ec'
# NOTE: 'ROGER' and 'Condo' are used interchangeably
ROGER='da262cbf-6d04-11e5-ba46-22000b92c6ec'
bf_re = 'TERRA_BF_L1B_O[0-9]+_[0-9]+_F000_V001.h5$'

def main():
    parser = argparse.ArgumentParser( description='This script transfers \
        BasicFusion files between two Globus endpoints. The main function \
        is to transfer the files while retaining the yyyy.mm directory \
        structure. By default, src_dir is recursively searched for all files \
        via a Globus listing. The manner in which it performs a listing can \
        be modified via the provided arguments.')
    parser.add_argument('src_id', help='Source Globus ID', type=str)
    parser.add_argument('src_dir', help='Source directories to recursively \
        transfer Basic Fusion files.', nargs='+', type=str)
    parser.add_argument('dest_id', help='Destination Globus ID.', type=str)
    parser.add_argument('dest_dir', help='Destination directory to transfer to.')
    parser.add_argument('-g', '--globus-split', help='Globus parallelism. \
        Specifies how many globus transfers to use. e.g. if set to 2, the \
        transfer task will be partitioned amongst 2 Globus transfers. \
        Defaults to 1.', 
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
    parser.add_argument('--infer', help='Infer the location of the BF \
        in src_dir instead of performing a directory search. Can only be used \
        when passing the --range flag. WARNING: Beta feature!',
        action='store_true')
    args = parser.parse_args()

    if args.infer and not args.range:
        raise ValueError('--infer cannot be passed without --range.')
    if args.infer and args.local_src:
        raise ValueError('--infer cannot be passed with --local-src.')
    if args.infer and len(args.src_dir) > 1:
        raise ValueError('Cannot pass multiple src_dir if passing --infer.')

    transfer = globus.GlobusTransfer( args.src_id, args.dest_id )

    files = []
    if args.local_src:
        for dir in args.src_dir:
            for root, directories, fnames in os.walk( dir ):
                for fname in fnames:
                    path = os.path.join( root, fname )
                    if re.search( bf_re, path ):
                        files.append( path )
    elif not args.infer:
        for dir in args.src_dir:
            files += globus.ls( args.src_id, dir, True )
    else:
        for i in xrange( args.range[0], args.range[1] + 1 ):
            o_start = bfutils.orbit.orbit_start( i )
            subdir = '{}.{}'.format( o_start[0:4], o_start[4:6] )
            files.append( os.path.join( args.src_dir[0], subdir, 
                'TERRA_BF_L1B_O{}_{}_F000_V001.h5'.format(i, o_start)) )

    omin=sys.maxint
    omax=0

    orbits = {}

    for file in files:
        if bfutils.orbit.is_bf_file( file ):
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
            orbit = bfutils.orbit.bf_file_orbit( file )
            orbits[orbit] = None

            if orbit < omin:
                omin = orbit
            if orbit > omax:
                omax=orbit

    if args.range:
        for i in xrange( args.range[0], args.range[1] + 1 ):
            if i not in orbits:
                raise RuntimeError('Was not able to find orbit {}'.format(i))

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
