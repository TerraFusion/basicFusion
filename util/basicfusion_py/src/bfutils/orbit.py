import sys, os
import re
import bfutils.constants as constants
import cPickle as pickle
import json

__author__ = 'Landon T. Clipp'
__email__  = 'clipp2@illinois.edu'

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

_orbit_limits = { 2000 : { 'start' : 1000, 'end' : 5528 }, \
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


def _check_o_info_pickle( orbit_txt = constants.ORBIT_INFO_TXT, \
        orbit_p = constants.ORBIT_INFO_DICT ):
    # TODO
    # LTC: Not sure if I want to implement this function. Originally thought that dumping
    # the Orbit_Path_Time.txt into json, then loading the json into a python dictionary
    # would decrease lookup times for orbit_start, but the very act of loading the json
    # file every time orbit_start is called may take longer than parsing the Orbit.txt file
    # directly.

    o_info = {}

    # Check if the python pickle containing the orbit info dictionary exists
    if not os.path.isfile( orbit_p ): 
        # If it doesn't exist, we need to generate it.
        with open( orbit_info, 'r') as file:
            for line in file:
                line = line.strip()
                
                # Split the line into each field
                fields = line.split(' ')
                orbit = fields[0].strip()
                path = fields[1].strip()
                stime = fields[2].strip()
                etime = fields[3].strip()

                # Get each field into an appropriate format
                orbit = int(orbit)
                path = int(path)
    
                # Remove all non-integer characters
                stime = re.sub(r"\D", '', stime)
                etime = re.sub(r"\D", '', stime)

                # Create new 

def orbit_start( orbit, orbit_info=constants.ORBIT_INFO_TXT ):
    '''
**DESCRIPTION:**  
    This function finds the starting time of the orbit according to the Orbit_Path_Info.txt file. Please
    see the GitHub documentation on how to generate this file.
    
**ARGUMENTS:**  
    *orbit (int)* -- Orbit to find starting time of  
    *orbit_info (str)*    -- Path to the orbit_info.txt file
    
**EFFECTS:**  
    None
    
**RETURN:**   
    String containing the starting time in the format yyyymmddHHMMSS.
    
'''

    if orbit < _orbit_limits['orbitLimits']['start'] or \
       orbit > _orbit_limits['orbitLimits']['end']:
        raise ValueError("Argument 'orbit' is outside the supported bounds.")

    with open( orbit_info, 'r') as file:
        for line in file:
            if re.search( "^{}".format(orbit), line ):
                match=line
                break

    if match == None:
        return None
    
    match = match.split(' ')[2].strip()
    match = match.replace('-','')
    match = match.replace('T','')
    match = match.replace(':','')
    match = match.replace('Z','')

    return match

def orbitYear( orbit ):
    '''
**DESCRIPTION:**  
    Returns the year that orbit belongs to.
    
**ARGUMENTS:**  
    *orbit (int)* -- Terra orbit
    
**EFFECTS:**  
    None
    
**RETURN:**  
    Year that orbit belongs to.      
    -1 on failure.
    '''

    for i in xrange( _orbit_limits['yearLimits']['start'], _orbit_limits['yearLimits']['end']):
        if orbit >= _orbit_limits[i]['start'] and orbit <= _orbit_limits[i]['end']:
            return i

    raise ValueError("Orbit {} outside the range of accepted values!".format(orbit))

def makeRunDir( dir ):
    '''
**DESCRIPTION:**  
    This function makes a directory called run# where # is the first integer greater than any other run# in
    the directory given by the argument dir.
    
**ARGUMENTS:**  
    *dir (str)*   -- Directory to create the run#.
    
**EFFECTS:**  
    Creates a directory.

**RETURN:**  
    Returns the full path of the newly created directory.
    '''

    if not isinstance(dir, basestring):
        raise TypeError("Passed argument \'dir\' is not a string!")

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
    '''
**DESCRIPTION:**  
    This function implements a bin-packing algorithm. It attempts
    to fill numProcess number of bins with numTasks number of elements
    as evenly as possible. This can be used for non-dynamic load balancing
    MPI programs.
    
**ARGUMENTS:**   
    *numProcess (int)* -- The number of MPI processes (i.e. the number of 
                        bins to fill)  
    *numTasks   (int)* -- Number of individual tasks to perform

**EFFECTS:**    
    None

**RETURN:**    
    Returns a list of size numProcess. Each element in the list tells how 
    many orbits each MPI process will handle.
    '''

    processBin = [ 0 ] * numProcess     # Each element tells which process how many orbits it should handle
    
    pIndex = 0

    # Loop through all the orbits to fill up the processBin
    for i in xrange(0, numTasks ):
        if ( pIndex == numProcess ):
            pIndex = 0

        processBin[pIndex] = processBin[pIndex] + 1
        pIndex += 1

    return processBin

def isBFfile( file_list ):
    '''
**DESCRIPTION:**  
    This function takes as input a list of strings containing a file path 
    and determines, based on a regex pattern, if the file is or is not a 
    'proper' Basic Fusion file. What constitutes a proper BF file is 
    documented on this project's GitHub.

**ARGUMENTS:**  
    *file_list (list)*  -- A list of filenames or file paths.

**EFFECTS:**  
    None

**RETURN:**  
    A list of integers of size len( file_list ), where each element 
    directly corresponds to the elements in file_list.  
    For each element:  
    -1 indicates that the file is not proper.  
    0 indicates a proper MOPITT file match.  
    1 indicates a proper CERES file matchh.  
    2 indicates a proper MODIS file match.  
    3 indicates a proper ASTER file match.  
    4 indicates a proper MISR GRP file match.  
    5 indicates a proper MISR AGP file match.  
    6 indicates a proper MISR GMP file match.  
    7 indicates a proper MISR HRLL file match.  
    '''

    if type( file_list ) is not list:
        raise TypeError("Argument 'file_list' is not a list.")

    isProper = []

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
