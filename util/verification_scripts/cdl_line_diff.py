import argparse
import re
from multiprocessing import Pool, Process, Queue
import os
import logging
import bfutils as bf
import csv
import sys
import errno

cdl_re='\.cdl$'
input_re='^input[0-9]+\.txt$'
input_re2='^[0-9]+input\.txt$'
csv_re='^[0-9]+\.csv$'
proc_log_re1='[0-9]+process_log\.txt$'
proc_log_re2='errors[0-9]+\.txt$'
duplicate_file='./duplicates.txt'

done='DONE'

# Generates the CDL line number diff
def gen_diff( arg ):
    f_1 = arg['f_1']
    f_2 = arg['f_2']
    orbit = arg['orbit']
    one_count = 0
    two_count = 0

    i = 0
    with open( f_1, 'r' ) as f:
        for i, l in enumerate(f, 1):
            pass
    one_count = i

    i = 0    
    with open( f_2, 'r' ) as f:
        for i, l in enumerate(f, 1):
            pass
    two_count = i
    delta = abs( one_count - two_count )
    ret_str = '{} {} {}'.format(delta, f_1, f_2)
    return ret_str

#files = {
#    orbit : 
#    {
#        cdl : 'string'
#        input : 'string'
#        proc_log : 'string'
#        bf_size : int
#    }
#
#    csv : 
#    {
#        o_range : 'file path'
#    }

def parse_dir( arg ):
    files = {}
    files['csv'] = {}

    subdirs = ['']
    if arg['num'] == 2:
        subdirs = ['cdl', 'errors', 'file-size', 'finputlist' ]

    for subdir in subdirs:
        for root, directories, fnames in os.walk( os.path.join( arg['dir'], subdir) ):
            if 'cdl2' in root or 'ddl2' in root or 'ddl' in root or \
               'errors2' in root or 'file-size2' in root or 'samples' in root:
                continue

            for fname in fnames:
                if re.search( cdl_re, fname ):
                    # Extract the orbit
                    orbit = fname.split('_')[3]
                    orbit = orbit.replace('O', '')
                    if orbit not in files:
                        files[orbit] = {}
                    path = os.path.join( root, fname)
                    files[orbit]['cdl'] = path
                    # Find line count
                    with open( path, 'r' ) as f:
                        num_lines = sum( 1 for line in f)
                    files[orbit]['cdl_num_line'] = num_lines

                elif re.search( csv_re, fname ):
                    # Extract the orbit range. They are grouped by thousand
                    o_range = fname.split('.')[0]
                    if 'csv' not in files:
                        files['csv'] = {}
                    files['csv'][o_range] = os.path.join( root, fname ) 
                elif re.search( input_re, fname ) or re.search(input_re2, fname):
                    # Extract the orbit
                    orbit = re.findall('\d+', fname)[0]
                    if orbit not in files:
                        files[orbit] = {}
                    
                    path = os.path.join( root, fname)
                    files[orbit]['input'] = path

                    # Find line count
                    with open( path, 'r' ) as f:
                        num_lines = sum( 1 for line in f )
                    files[orbit]['input_num_line'] = num_lines
                  
                elif re.search( proc_log_re1, fname ) or re.search( proc_log_re2, fname):
                    orbit = re.findall('\d+', fname)[0]
                    path = os.path.join( root, fname)
                    if orbit not in files:
                        files[orbit] = {}

                    # proc_log shouldn't be populated yet. If it is, then
                    # there must be a duplicate file
                    if 'proc_log' in files[orbit]:
                        with open(duplicate_file, 'a') as f:
                            f.write( '{}\n{}\n\n'.format( \
                                files[orbit]['proc_log'], path))
                        continue

                    files[orbit]['proc_log'] = path 
                    # Find line count
                    with open( path, 'r' ) as f:
                        num_lines = sum(1 for line in f)
                    files[orbit]['log_num_line'] = num_lines
         
    #if arg['num'] == 1:
    #    print(files)
    return ( files, arg['num'] )


class Work(object):
    def __init__( self, func, **kwargs ):
        self.func = func
        self.args = kwargs

def get_csv_size( arg ):
    with open( arg, 'rb') as f:
        reader = csv.reader(f)
        ret_list = []
        for row in reader:
            if row[0] == 'orbit':
                continue
            ret_list.append( row )
    return ret_list

def proc_log_size( arg ):
    dict = arg[0]
    orbit = arg[1]

    if 'bf_size' in dict:
        return

    with open( dict['proc_log'], 'r' ) as f:
        lines = f.readlines()

    try:
        #LTC Feb 28, 2018
        # Decided to use stat instead of ls for finding BF file size. Unfortunately,
        # this change was not reflected on all versions of scripts so we need to explicitly
        # handle the two different outputs of stat and ls.

        # using stat
        if lines[-1].split()[0] == 'Birth:':
            ret_val = [ lines[-7].split()[1], orbit ]
        # using ls
        else:
            ret_val = [ lines[-1].split()[4], orbit ]
    except IndexError:
        with open( duplicate_file, 'a' ) as f:
            f.write('Anomaly found in input file: {}'.format(dict) )

        print("INDEX ERROR!!!")
        print(dict)
        print(lines)
        raise

    return ret_val

def main():
    parser = argparse.ArgumentParser( description='This script recursively finds \
        all CDL files in the two directories given to it, maps each CDL file \
        to a certain orbit, and prints out the line number difference for \
        each orbit pair that it found.')
    parser.add_argument('dir_one', help='First directory with CDL files.', \
        type=str )
    parser.add_argument('dir_two', help='Second directory with CDL files.', \
        type=str)
    parser.add_argument('--input-linenum', help='Find difference in input \
        file list line numbers.', dest='input_linenum', action='store_true' )
    parser.add_argument('-p', '--num-proc', help='Set the number of \
        subprocess. Default: 10.', type=int, dest='num_proc', default=10)
    args = parser.parse_args()

    # Enable logging
    logging.basicConfig( format=bf.constants.LOG_FMT, \
        datefmt=bf.constants.LOG_DATE_FMT, stream=sys.stderr, level=logging.DEBUG )
    logger = logging.getLogger('master')

    try:
        os.remove( duplicate_file )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

    arg_list = []
    type = 0
    if args.input_linenum:
        type = 1

    arg = { 'dir' : args.dir_one, \
            'num' : 1, \
            'type' : type \
          }
    arg_list.append( arg )
    arg = { 'dir' : args.dir_two, \
            'num' : 2, \
            'type' : type \
          }
    arg_list.append( arg )

    logging.info('Parsing directories...')
    p = Pool(2)        
    ret_vals = p.map( parse_dir, arg_list )
    p.close()
    p.join()

    for i in ret_vals:
        if i[1] == 1:
            one_files = i[0]
        else:
            two_files = i[0]

    logging.info('Done')
    logging.info('Parsing csv files...')
    # Directories have been fully parsed for relevant files. Now we
    # send the work of generating metadata info to subprocess pool
    p = Pool( args.num_proc )        
    arg_list = []

    # First, parse all the csv files for the orbit's file size
    for csv in one_files['csv']:
        arg_list.append( one_files['csv'][csv] )

    one_csv_size = p.map( get_csv_size, arg_list )

    arg_list = []
    for csv in two_files['csv']:
        arg_list.append( two_files['csv'][csv] )
    two_csv_size = p.map( get_csv_size, arg_list )
    logging.info('Inserting one_files csv orbit sizes') 
    for i in one_csv_size:
        for j in i:
            orbit = j[0]
            size = j[1]
            if orbit in one_files:
                if 'bf_size' in one_files[orbit]:
                    raise ValueError('Orbit {} has not-none bf_size'.format(orbit))
            else:
                one_files[orbit] = {}
            one_files[ orbit ]['bf_size'] = size

    logging.info('Inserting two_files csv orbit sizes') 
    for i in two_csv_size:
        for j in i:
            orbit = j[0]
            size = j[1]
            if orbit in two_files:
                if 'bf_size' in two_files[orbit]:
                    raise ValueError('Orbit {} has not-none bf_size'.format(orbit))
            else:
                two_files[orbit] = {}    

            two_files[ orbit ]['bf_size'] = size
     
    logging.info('Done.')
   
    arg_list = []

    logging.info('Finding BF sizes according to error log...')
    # Now we need to find sizes according to the bf_proc_log
    for orbit in one_files:
        if orbit != 'csv' and orbit in two_files:
            if re.search( proc_log_re1, one_files[orbit]['proc_log'] ) or \
               re.search( proc_log_re2, one_files[orbit]['proc_log'] ):
                arg_list.append( [ one_files[orbit], orbit ] )

    sizes = p.map( proc_log_size, arg_list )
    for i in sizes:
        if 'bf_size' in one_files[i[1]]:
            raise ValueError('bf_size already in one_files for orbit {}'.format(i[1]))
    
        one_files[i[1]]['bf_size'] = i[0]

    arg_list = []
#    for orbit in two_files:
#        if orbit != 'csv' and orbit in two_files:
#            try:
#                if re.search( proc_log_re1, two_files[orbit]['proc_log'] ) or \
#                   re.search( proc_log_re2, two_files[orbit]['proc_log'] ):
#                    arg_list.append( [ two_files[orbit], orbit ] )
#            except KeyError:
#                print('ERROR: orbit: {}'.format(orbit))
#                print( two_files[orbit])
#                raise
# 
#    sizes = p.map( proc_log_size, arg_list )
#    for i in sizes:
#        if i:
#            if 'bf_size' in two_files[i[1]]:
#                raise ValueError('bf_size already in two_files for orbit {}'.format(two_files[i[1]]))
#        
#            two_files[i[1]]['bf_size'] = i[0]
    

#    logging.info('Generating metadata info...')
#    arg_list = []
#    for orbit in one_files:
#        if orbit != 'csv' and orbit in two_files:
#            cur_arg = { 'f_1' : one_files[orbit], \
#                        'f_2' : two_files[orbit], \
#                        'orbit' : orbit \
#                      }
#            arg_list.append( cur_arg )
#    
#    diff_list = p.map( gen_diff, arg_list )
#    logging.info('Done.')
    

    p.close()
    p.join()
    logging.info('Done.')

    logging.info('Printing result to stdout...')
    for orbit in one_files:
        if orbit == 'csv':
            continue

        try:
            if orbit in two_files:
                bf_diff = abs( int(one_files[orbit]['bf_size']) - int(two_files[orbit]['bf_size']))
                logdiff = abs( int(one_files[orbit]['log_num_line']) - int(two_files[orbit]['log_num_line']))
                input_diff = abs( int(one_files[orbit]['input_num_line'] ) - int(two_files[orbit]['input_num_line']))
                cdl_diff = abs( int(one_files[orbit]['cdl_num_line']) - int(two_files[orbit]['cdl_num_line']) )

                print('{} bf_size_diff: {} log_line_diff: {} input_line_diff: {} cdl_line_diff {}'.format( \
                    orbit, bf_diff, logdiff, input_diff, cdl_diff))
        except KeyError as e:
            logger.exception('ORBIT {}\none_files: {}\ntwo_files: {}'.format(orbit, one_files[orbit], two_files[orbit]))
 
    logging.info('Done')

if __name__ == '__main__':
    main()

