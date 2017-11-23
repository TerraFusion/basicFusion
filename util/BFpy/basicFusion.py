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

# ==========================================
# = Define what orbits belong to what year =
# ==========================================

_orbitYear = { 2000 : { 'start' : 1000, 'end' : 5528 }, \
              2001 : { 'start' : 5529, 'end' : 10844 }, \
              2002 : { 'start' : 10845, 'end' : 16159 }, \
              2003 : { 'start' : 16160, 'end' : 21474 }, \
              2004 : { 'start' : 21475, 'end' : 26804 }, \
              2005 : { 'start' : 26805, 'end' : 32119 }, \
              2006 : { 'start' : 32120, 'end' : 37435 }, \
              2007 : { 'start' : 37436, 'end' : 42750 }, \
              2008 : { 'start' : 42751, 'end' : 48080 }, \
              2009 : { 'start' : 48081, 'end' : 53395 }, \
              2010 : { 'start' : 53396, 'end' : 58711 }, \
              2011 : { 'start' : 58712, 'end' : 64026 }, \
              2012 : { 'start' : 64027, 'end' : 69365 }, \
              2013 : { 'start' : 69357, 'end' : 74671 }, \
              2014 : { 'start' : 74672, 'end' : 79986 }, \
              2015 : { 'start' : 79987, 'end' : 85302 }, \
              'orbitLimits' : { 'start' : 1000, 'end' : 85302 }, \
              'yearLimits'  : { 'start' : 2000, 'end' : 2015 } \
            }
def orbitYear( orbit ):
    '''
    DESCRIPTION:
        Returns the year that orbit belongs to.
    ARGUMENTS:
        orbit (int) -- Terra orbit
    EFFECTS:
        None
    RETURN:
        Year that orbit belongs to.
        -1 on failure.
    '''

    for i in xrange( _orbitYear['yearLimits']['start'], _orbitYear['yearLimits']['end']):
        if orbit >= _orbitYear[i]['start'] and orbit <= _orbitYear[i]['end']:
            return i

    raise ValueError("Orbit {} outside the range of accepted values!".format(orbit))

def makeRunDir( dir ):
    '''
    DESCRIPTION:
        This function makes a directory called run# where # is the first integer greater than any other run# in
        the directory given by the argument dir.
    ARGUMENTS:
        dir (str)   -- Directory to create the run#.
    EFFECTS:
        Creates a directory.
    RETURN:
        Returns the full path of the newly created directory.
    '''

    if not isinstance(dir, basestring):
        raise ValueError("Passed argument \'dir\' is not a string!")

    greatestSeen=-1
    runRE='^run[0-9]+$'

    # Walk through every entry in dir. Find the greatest run#.
    for i in os.listdir( dir ):
        if not re.match( runRE, i ) :
            continue
        tempRunNum = int( i.replace( 'run', '') )
        if tempRunNum > greatestSeen:
            greatestSeen = tempRunNum

    newNum = greatestSeen + 1

    # We can make our new run directory
    newDir = os.path.join( dir, 'run' + str(newNum) )
    os.mkdir( newDir )

    return newDir
         
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
