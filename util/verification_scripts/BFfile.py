import sys, os
import re

def isBFfile( file_list ):
    """isBFfile
    DESCRIPTION:
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

    MOP_re='^MOP01-[0-9]+-L[0-9]+V[0-9]+.[0-9]+.[0-9]+.he5$'
    CER_re='^CER_SSF_Terra-FM[0-9]-MODIS_Edition[0-9]+A_[0-9]+.[0-9]+$'
    MOD_re='^MOD0((21KM)|(2HKM)|(2QKM)|(3)).A[0-9]+.[0-9]+.[0-9]+.[0-9]+.hdf$'
    AST_re='^AST_L1T_[0-9]+_[0-9]+_[0-9]+.hdf$'
    MIS_re1='^MISR_AM1_GRP_ELLIPSOID_GM_P[0-9]{3}_O[0-9]+_(AA|AF|AN|BA|BF|CA|CF|DA|DF)_F[0-9]+_[0-9]+.hdf$' 
    MIS_re2='^MISR_AM1_AGP_P[0-9]{3}_F[0-9]+_[0-9]+.hdf$'
    MIS_re3='^MISR_AM1_GP_GMP_P[0-9]{3}_O[0-9]+_F[0-9]+_[0-9]+.hdf$'
    MIS_re4='^MISR_HRLL_P[0-9]{3}.hdf$'

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
