"""
@author : Landon Clipp
@email : clipp2@illinois.edu
"""

from __future__ import print_function
import os, sys
import argparse
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import re
import pickle
from BFfile import isBFfile
 
def main():
    
    saveFile=os.path.dirname( os.path.realpath(__file__) ) + "/inquireSave.txt"
    
    # Define the arguments this program can take
    parser = argparse.ArgumentParser(description="This script takes as input a directory containing Terra files to be used \
in the Basic Fusion program. For all of the files that match a certain regular expression, this script inquires their file \
sizes and outputs 8 histogram plots where the x-axis is file size and the y-axis is file count.")
    

    parser.add_argument("TERRA_DIR", help="The directory containing Terra files.", type=str)
    parser.add_argument("OUT_DIR", help="Directory to save the histogram plots. Defaults to current directory.", type=str, \
        default='./')
    parser.add_argument("--useSaved", help="Use the saved values of the data size query. This file is found at {}. \
    The presence of this flag makes the TERRA_DIR argument obsolete (can pass in junk).".format(saveFile), \
        dest='useSaved', action='store_true')
    args = parser.parse_args()

    if args.useSaved:
        with open( saveFile, 'rb' ) as f:
            instrSizes = pickle.load(f)
    else:
        # Make the path absolute
        terraDir = os.path.abspath( args.TERRA_DIR )

        instrSizes = [ [], [], [], [], [], [], [], [] ]
        
        # Recurse through all the files in terraDir
        for root, directories, filenames in os.walk( terraDir ):
            for filename in filenames:
                # Determine if the file is proper
                isProper = isBFfile( [ filename ] )
                if isProper[0] != -1:
                   # If it is, add the file size (in MB) to its list
                   instrSizes[ isProper[0] ].append( os.path.getsize( os.path.join(root, filename) ) / 1000000 )

        with open( saveFile, 'wb' ) as f:
            pickle.dump( instrSizes, f )

    # We now have the sizes of all the proper files. We can make a histogram of this now...
    figNames = [ ['mop.png', 'MOPITT'], ['cer.png', 'CERES'], ['mod.png', 'MODIS'], ['ast.png', 'ASTER'], \
                 ['mis_grp.png', 'MISR GRP'], ['mis_agp.png', 'MISR AGP'], ['mis_gmp.png', 'MISR GMP'], \
                 ['mis_hrll.png', 'MISR HRLL'] ]
    
    bins=range(0,300,20)
    for i in xrange(0, 8):
        plt.hist( instrSizes[i], bins=bins)
        plt.xticks( bins )
        plt.title("File size frequency (" + figNames[i][1] + ')')
        plt.xlabel("Size (MB)")
        plt.ylabel("Frequency")
        plt.savefig( os.path.join( args.OUT_DIR, figNames[i][0]), bbox_inches='tight' )
        plt.close()
                

if __name__ == '__main__':
    main()
