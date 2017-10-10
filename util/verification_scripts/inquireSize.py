"""
@author : Landon Clipp
@email : clipp2@illinois.edu
"""

from __future__ import print_function
import os, sys
import argparse
from mpi4py import MPI
import matplotlib.pyplot as plt
import re

def isBFfile( file_list ):
    """isBFfile
    DESCRIPTION:
        This function takes as input a list of strings containing a file path and determines, based on a regex pattern, if the file is or is not a \"proper\" Basic Fusion file. What constitutes a proper BF file is documented on this project's GitHub.

    ARGUMENTS:
        file_list (list)  -- A filename or a filepath list of strings
    
    EFFECTS:
        None

    RETURN:
        A list of integers of size len( file_list ), where each element directly corresponds to the elements in file_list.
        For each element:
        -1 indicates that the file is not proper.
        0 indicates a proper MOPITT file match.
        1 indicates a proper CERES file match.
        2 indicates a proper MODIS file match.
        3 indicates a proper ASTER file match.
        4 indicates a proper MISR GRP file match.
        5 indicates a proper MISR AGP file match.
        6 indicates a proper MISR GMP file match.
        7 indicates a proper MISR HRLL file match.
    """

    isProper = []

    MOP_re='^MOP01-[0-9]{8}-L[0-9]V[0-9].[0-9]{2}.[0-9].he5'
    CER_re='^CER_SSF_Terra-FM[0-9]-MODIS_Edition[0-9]A_[0-9]{6}.[0-9]{10}'
    MOD_re='^MOD0((21KM)|(2HKM)|(2QKM)|(3)).A[0-9]{7}.[0-9]{4}.[0-9]{3}.[0-9]{13}.hdf'
    AST_re='^AST_L1T_[0-9]{17}_[0-9]+_[0-9]+.hdf'
    MIS_re1='^MISR_AM1_GRP_ELLIPSOID_GM_P[0-9]{3}_O[0-9]+_(AA|AF|AN|BA|BF|CA|CF|DA|DF)_F[0-9]+_[0-9]+.hdf' 
    MIS_re2='^MISR_AM1_AGP_P[0-9]{3}_F[0-9]+_[0-9]+.hdf'
    MIS_re3='^MISR_AM1_GP_GMP_P[0-9]{3}_O[0-9]+_F[0-9]+_[0-9]+.hdf'
    MIS_re4='^MISR_HRLL_P[0-9]{3}.hdf'

    # It's more efficient to match one large regular expression than many smaller ones. 
    # Combine all of the above regex into one.
    hugeRE='(' + ')|('.join([ MOP_re, CER_re, MOD_re, AST_re, MIS_re1, MIS_re2, MIS_re3, MIS_re4]) + ')'

    for f in file_list:
        # Strip the path
        fname = os.path.basename( f )
        
        if re.match( MOP_re, fname ):
            isProper.append(0)
        elif re.match( CER_re, fname ):
            isProper.append(1)
        elif re.match( MOD_re, fname ): 
            isProper.append(2)
        elif re.match( AST_re, fname ):
            isProper.append(3)
        elif re.match( MIS_re1, fname ):
            isProper.append(4)
        elif re.match( MIS_re2, fname ):
            isProper.append(5)
        elif re.match( MIS_re3, fname ):
            isProper.append(6)
        elif re.match( MIS_re4, fname ):
            isProper.append(7)
        else:
            isProper.append(-1)

    return isProper
    
def main():
    
    # Define the arguments this program can take
    parser = argparse.ArgumentParser(description="This script takes as input a directory containing Terra files to be used \
in the Basic Fusion program. For all of the files that match a certain regular expression, this script inquires their file \
sizes and outputs 5 histogram plots (one for each instrument) where x-axis is file size and y-axis is file count.")
    

    parser.add_argument("TERRA_DIR", help="The directory containing Terra files.", type=str)
    parser.add_argument("OUT_DIR", help="Directory to save the histogram plots. Defaults to current directory.", type=str, \
        default='./')

    args = parser.parse_args()

    # Make the path absolute
    terraDir = os.path.abspath( args.TERRA_DIR )

    fList = []
    instrSizes = [ [], [], [], [], [], [], [], [] ]

    isBFfile( fList )

    # Recurse through all the files in terraDir
    for root, directories, filenames in os.walk( terraDir ):
        for filename in filenames:
            # Determine if the file is proper
            isProper = isBFfile( [ filename ] )
            if isProper[0] != -1:
               # If it is, add the file size (in MB) to its list
               instrSizes[ isProper[0] ].append( os.path.getsize( os.path.join(root, filename) ) / 1000000 )

    # We now have the sizes of all the proper files. We can make a histogram of this now...
    figNames = [ 'mop.png', 'cer.png', 'mod.png', 'ast.png', 'mis_grp.png', 'mis_agp.png', 'mis_gmp.png', 'mis_hrll.png' ]
    for i in xrange(0, 8):
        plt.hist( instrSizes[i] )
        plt.title("File size frequency")
        plt.xlabel("Size (MB)")
        plt.ylabel("Frequency")
        plt.savefig( os.path.join( args.OUT_DIR, figNames[i]) )

                

if __name__ == '__main__':
    main()
