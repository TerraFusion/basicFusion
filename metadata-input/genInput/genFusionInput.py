"""
@author Landon Clipp
@email clipp2@illinois.edu

This script is ultimately intended to replace the atrocious genFusionInput.sh script. In the process of development, it will only replace certain functionality until all functionality has a Python implementation.
"""

import BFfile
import os, sys
import argparse
import re

#===================================================================================================
def MISR_miss( inFile ):
    
    grp_miss = "MISR_AM1_GRP_MISS"
    gp_miss  = "MISR_AM1_GP_MISS"
    agp_miss = "# MISR_AM1_AGP MISS"
    hrll_miss = "# MISR_AM1_HRLL MISS"

    with open( inFile, 'r' ) as f:
        f_list = f.readlines()
        i = 0
        fname = os.path.basename(f_list[i]).strip()

        # Find the first occurance of ASTER file
        while ( not re.match( BFfile.AST_re, fname ) \
        and 'AST N/A' not in f_list[i] \
        or '#' == f_list[i].lstrip()[0] \
        or f_list[i].isspace() ) \
        and i < len(f_list) - 2:
            i = i + 1
            fname = os.path.basename(f_list[i]).strip()

        # We found the first occurance of ASTER. Now need to find the last occurance of ASTER
        while (re.match( BFfile.AST_re, fname ) \
        or 'AST N/A' in f_list[i] \
        or '#' == f_list[i].lstrip()[0] \
        or f_list[i].isspace() ) \
        and i < len(f_list) - 2:
            i = i + 1
            fname = os.path.basename(f_list[i]).strip()
    
    
        curLine=''
        reIdx=0
        pseudo_re = [ '_AA_', '_AF_', '_AN_', '_BA_', '_BF_', '_CA_', '_CF_', '_DA_', '_DF_', \
                           'MISR_AM1_AGP', 'MISR_AM1_GP_GMP_', 'MISR_HRLL_' ]
        while reIdx < len(pseudo_re):
            
            curLine = f_list[i]
            
            # If line is all spaces or line is comment and it's not the end of the list
            if (f_list[i].isspace() or f_list[i].lstrip()[0] == '#') and i < len(f_list)-1:
                i = i + 1 
                continue
           
            if f_list[i].lstrip()[0] == '#':
                curLine=''

            if pseudo_re[reIdx] not in os.path.basename(curLine):
                if reIdx >= 0 and reIdx <= 8:
                    f_list.insert( i, grp_miss)
                elif reIdx == 9:
                    f_list.insert( i, agp_miss)
                elif reIdx == 10:
                    f_list.insert( i, gp_miss)
                elif reIdx == 11:
                    f_list.insert( i, hrll_miss )
           
            if i < len(f_list) - 1: 
                i = i + 1
            reIdx = reIdx + 1

 
    with open( inFile, 'w' ) as f:
        for item in f_list:
            f.write("{}\n".format(item.strip()) )

#==========================================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("IN_FILE", help="The BasicFusion input file list.", type=str)
    args = parser.parse_args()

    MISR_miss( args.IN_FILE )

if __name__ == '__main__':
    main()
