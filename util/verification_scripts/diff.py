import argparse
import os
import re
from bfutils.orbit import orbit_start
import errno
import subprocess as sp
from multiprocessing import Pool
CDL_re='.cdl$'

# TERRA_BF_L1B_O1000_20000225002507_F000_V001.h5.cdl


def diff_worker( arg ):
    
    orbit = int(arg['orbit'])
    f1 = arg['f1']
    f2 = arg['f2']

    print(orbit)

    o_start = orbit_start( orbit )
    o_path = os.path.join('.', 'diff_dir', o_start[0:4], \
        o_start[4:6], o_start[6:8])
    try:
        os.makedirs( o_path )
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open( f2, 'r' ) as f:
        k_lines = f.readlines()
    with open( f1, 'r') as f:
        l_lines = f.readlines()

    diff_log = os.path.join( o_path, '{}_cdl.diff'.format(orbit))
    args = ['diff', f1, f2 ]
    with open( diff_log, 'w' ) as f:
        sp.call( args, stdout=f, stderr=sp.STDOUT )
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir_one', help='Directory where Landon\'s\
        log files are stored.', type=str )
    parser.add_argument('dir_two', help='Directory where Dr. Yang\'s\
        log files are stored.', type=str )
    args = parser.parse_args()

    one_files = {}
    print('Recursing through dir_one')
    for root, directories, fnames in os.walk( args.dir_one ):
        for fname in fnames:
            if re.search( CDL_re, fname ):
                # Extract the orbit
                orbit = fname.split('_')[3]
                orbit = orbit.replace('O', '')
                one_files[orbit] = os.path.join( root, fname )
    print('Done')
    two_files = {}
    print('Recursing through dir_two')
    for root, directories, fnames in os.walk( args.dir_two ):
        for fname in fnames:
            if re.search( CDL_re, fname):
                # Extract the orbit
                orbit = fname.split('_')[3].replace('O', '')
                two_files[orbit] = os.path.join( root, fname )

    print('Done')

    arg_list = []
    for orbit in one_files:
        if orbit in two_files:
            arg = { 'f1' : one_files[orbit], \
                    'f2' : two_files[orbit], \
                    'orbit' : orbit \
                  }

            arg_list.append(arg)


    p = Pool(15)
    
    diff_list = p.map( diff_worker, arg_list )
    p.close()
    p.join()

if __name__ == '__main__':
    main()
