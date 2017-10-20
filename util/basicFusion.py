import sys, os
import re

MOP_re='^MOP01-[0-9]+-L[0-9]+V[0-9]+.[0-9]+.[0-9]+.he5$'
CER_re='^CER_SSF_Terra-FM[0-9]-MODIS_Edition[0-9]+A_[0-9]+.[0-9]+$'
MOD_re='^MOD0((21KM)|(2HKM)|(2QKM)|(3)).A[0-9]+.[0-9]+.[0-9]+.[0-9]+.hdf$'
AST_re='^AST_L1T_[0-9]+_[0-9]+_[0-9]+.hdf$'
MIS_re_GRP='^MISR_AM1_GRP_ELLIPSOID_GM_P[0-9]{3}_O[0-9]+_(AA|AF|AN|BA|BF|CA|CF|DA|DF)_F[0-9]+_[0-9]+.hdf$' 
MIS_re_AGP='^MISR_AM1_AGP_P[0-9]{3}_F[0-9]+_[0-9]+.hdf$'
MIS_re_GP='^MISR_AM1_GP_GMP_P[0-9]{3}_O[0-9]+_F[0-9]+_[0-9]+.hdf$'
MIS_re_HRLL='^MISR_HRLL_P[0-9]{3}.hdf$'

def findProcPartition( numProcess, numTasks):
    """DESCRIPTION:
        This function determines which MPI processes will be responsible for how many orbits. It loops through the
        list "orbits" and increments each element in the "processBin" list, up to len(orbits) number of orbits.
    
    ARGUMENTS:
        numProcess (int)        -- The number of MPI processes (i.e. the number of bins to fill with orbits)
        numTasks                -- Number of individual tasks to perform

    EFFECTS:
        None

    RETURN:
        Returns a list of size numProcess. Each element in the list tells how many orbits each MPI process will
        handle.
    """

    processBin = [ 0 ] * numProcess     # Each element tells which process how many orbits it should handle
    
    pIndex = 0

    # Loop through all the orbits to fill up the processBin
    for i in xrange(0, numTasks ):
        if ( pIndex == numProcess ):
            pIndex = 0

        processBin[pIndex] = processBin[pIndex] + 1
        pIndex = pIndex + 1

    return processBin

def isBFfile( file_list ):
    """DESCRIPTION:
        This function takes as input a list of strings containing a file path and determines, based on a regex pattern, if the file is or is not a \"proper\" Basic Fusion file. What constitutes a proper BF file is documented on this project's GitHub.
    ARGUMENTS:
        file_list (list)  -- A list of filenames or filepaths.
    
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


    # It's more efficient to match one large regular expression than many smaller ones. 
    # Combine all of the above regex into one.
    #hugeRE='(' + ')|('.join([ MOP_re, CER_re, MOD_re, AST_re, MIS_re1, MIS_re2, MIS_re3, MIS_re4]) + ')'

    for f in file_list:
        # Strip the path
        fname = os.path.basename( f ).strip()
        
        if re.match( MOP_re, fname ):
            isProper.append(0)
        elif re.match( CER_re, fname ):
            isProper.append(1)
        elif re.match( MOD_re, fname ): 
            isProper.append(2)
        elif re.match( AST_re, fname ):
            isProper.append(3)
        elif re.match( MIS_re_GRP, fname ):
            isProper.append(4)
        elif re.match( MIS_re_AGP, fname ):
            isProper.append(5)
        elif re.match( MIS_re_GP, fname ):
            isProper.append(6)
        elif re.match( MIS_re_HRLL, fname ):
            isProper.append(7)
        else:
            isProper.append(-1)

    return isProper
