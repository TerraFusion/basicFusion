"""
@author : Landon Clipp
@email : clipp2@illinois.edu
"""

from __future__ import print_function
import os, sys, errno
import argparse
import tarfile
import subprocess
import hashlib
from mpi4py import MPI

def eprint( *args, **kwargs):
    print( *args, file=sys.stderr, **kwargs )

def queryFiles( orbitNum, SQLqueries, SQL_DB, queryLoc, instr ):
    """         queryFiles
    DESCRIPTION:
        This function handles the task of querying the SQLite database for orbit orbitNum and for instrument instr.
        It only queries for a singular instrument/orbit pair and appends the result to a text file. This text file is
        located at the path: [queryLoc]/[orbitNum].txt.

    ARGUMENTS:
        orbitNum (int)          -- The orbit to query for
        SQLqueries (string)     -- A path to the queries.bash file. This file contains a series of SQLite calls
                                   wrapped inside of bash functions.
        SQL_DB (string)         -- The path to the SQLite database on which the queries within SQLqueries will be
                                   performed.
        queryLoc (string)         -- This is a directory path to where the database query results will be stored.
        instr (string)          -- This is a 3 character string denoting which instrument to query for. The valid
                                   values are:
                                   "MOP", "CER", "AST", "MIS", "MOD".

    EFFECTS:
        Appends to the [queryLoc]/[orbitNum].txt file.

    RETURN:
        None
    """

    procList = []
    
    if instr == "MOP":
        # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
        processCall = [ "bash", SQLqueries, "instrumentOverlappingOrbit", SQL_DB, str(orbitNum), "MOP" ]

        with open("{}/{}.txt".format( queryLoc, orbitNum), "a") as out:
            procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "CER":
         
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentOverlappingOrbit", SQL_DB, str(orbitNum), "CER" ]

            with open("{}/{}.txt".format(queryLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "AST":
         
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentStartingInOrbit", SQL_DB, str(orbitNum), "AST" ]

            with open("{}/{}.txt".format(queryLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "MIS":
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentStartingInOrbit", SQL_DB, str(orbitNum), "MIS" ]

            with open("{}/{}.txt".format(queryLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "MOD":

            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentInOrbit", SQL_DB, str(orbitNum), "MOD" ]

            with open("{}/{}.txt".format(queryLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    else:
        raise ValueError("The argument 'instr' should be either MOP, CER, AST, MIS, or MOD.")
    
    exit_codes = [ p.wait() for p in procList ]
   
def createTar( orbitNum, orbitFileList, outputDir, hashDir ):
    """         createTar
    DESCRIPTION:
        This function takes as input a specific Terra orbit number, a list of input Terra HDF files for that orbit,
        and generates one single tar file containing all of the files listed in orbitFileList.

        There is a special case for the MISR HRLL and AGP files. Since these two file sets are reused many times over
        in the BasicFusion processing, instead of re-tarring them over and over, we will simply create a text file 
        indicating which HRLL and AGP file needs to be included in this orbit. THIS IMPLIES that the entire AGP and HRLL
        file sets will be tarred/untarred separately, and that whatever script untars the orbit-level tar files
        generated by this script will be aware of this subtlety.

        FOR INSTANCE:
        Say that the database returns these AGP and HRLL files for orbit 40110:

        /projects/TDataFus/oneorbit/MISR/AGP_HR/MISR_HRLL_P022.hdf
        /projects/TDataFus/oneorbit/MISR/AGP/MISR_AM1_AGP_P022_F01_24.hdf

        Instead of tarring those actual files, this function will create a text file named MISR_PATH_FILES.txt containing the lines:

        MISR_HRLL_P022.hdf
        MISR_AM1_AGP_P022_F01_24.hdf

        The MISR_PATH_FILES.txt line is what will be tarred along with the rest of the files returned by the database.

    ARGUMENTS:
        orbitNum (int)          -- The Terra orbit number. This number is used to name the output tar file.
        orbitFileList (string)  -- A path to the text file containing all of the input HDF Terra files for the orbit
                                   specified by orbitNum. This list of files is generated by querying the SQLite database
                                   passed to this script.
        outputDir (string)      -- A directory path to where the resultant tar file will reside.

    EFFECTS:
        Generates a new tar file at outputDir

    RETURN:
        None
    """

    tarName="{}archive.tar".format(orbitNum)
    # Create a tar file
    tar = tarfile.open("{}/{}".format(outputDir, tarName), "w" )
        
    extraMISR=0
    MISR_PATH_FILES="MISR_PATH_FILES_{}.txt".format(orbitNum)

    with open( orbitFileList, "r" ) as f:
        for line in f:
            if "MISR_HRLL" in line or "MISR_AM1_AGP" in line:
                extraMISR=1
                # Grab just the file name
                MISRFile=line.split('/', -1)[-1]
                with open("/dev/shm/{}".format(MISR_PATH_FILES), "a") as MIS_f:
                    MIS_f.write(MISRFile)
        # TODO: Finish this functionality
            else:
                tar.add( line.strip(), recursive=False )

    # Add the extra MISR files
    if extraMISR == 1:
        tar.add( "/dev/shm/{}".format(MISR_PATH_FILES), recursive=False ) 

    tar.close()

    # I'm not sure if you have to close the tar before you remove a file it will tar...
    if extraMISR == 1: 
        # Delete the MISR_PATH_FILES file
        os.remove("/dev/shm/{}".format(MISR_PATH_FILES ))

    # Create the md5 hash
    hasher = hashlib.md5()
    with open("{}/{}".format(outputDir, tarName), 'rb') as tarFile:
        buf = tarFile.read()
        hasher.update(buf)



    with open( os.path.join(hashDir, "{}hash.md5".format(orbitNum)), 'w' ) as md5F:
        md5F.write(hasher.hexdigest())

def findJobPartition( numProcess, orbits ):
    """         findJobPartition()
    
    DESCRIPTION:
        This function determines which MPI jobs will be responsible for how many orbits. It loops through the
        list "orbits" and increments each element in the "processBin" list, up to len(orbits) number of orbits.
    
    ARGUMENTS:
        numProcess (int)        -- The number of MPI processes (i.e. the number of bins to fill with orbits)
        orbits (list: int)      -- A list containing all of the Terra orbits that need to be processed.

    EFFECTS:
        None

    RETURN:
        Returns a list of size numProcess. Each element in the list tells how many orbits each MPI process will
        handle.
    """

    processBin = [ 0 ] * numProcess     # Each element tells which process how many orbits it should handle
    
    pIndex = 0

    # Loop through all the orbits to fill up the processBin
    for i in xrange(0, len(orbits) ):
        if ( pIndex == numProcess ):
            pIndex = 0

        processBin[pIndex] = processBin[pIndex] + 1
        pIndex = pIndex + 1

    return processBin

def main():
    """
        main()

    DESCRIPTION:
        This function handles the high level details of parsing the SQLite database given to it and packing the
        results of the database query into tar files. The database contains all of the input HDF Terra files for
        the Basic Fusion project and is made by using the utilities in basicFusion/metadata-input/build. When given
        a list of orbits to tar (by means of parameters to this script), this script will query the database once
        for each orbit (and also for each instrument in the orbit) and then pack all of the files returned by that query 
        into a tar file. The resultant tar file represents a complete set of input to one Basic Fusion granule.

    ARGUMENTS:
        This function takes no arguments besides that of the command line arguments.

    EFFECTS:
        When querying the database, all queries are done simultaneously by means of multithreading.
        All tar file operations are performed by means of multiprocessing.

    RETURN:
        0 for success
        Non-zero for failure
    """

    # Define the arguments this program can take
    parser = argparse.ArgumentParser()

    # --orbit-file is not implemented yet
    parser.add_argument("--orbit-file", "-o", dest='orbitFile', help="Path to the text file containing the orbits to process. \
                        NOT IMPLEMENTED.")
    parser.add_argument("SQLite", help="The SQLite database containing input HDF Terra files.")
    parser.add_argument("OUT_DIR", help="Where the output tar files will reside.")
    parser.add_argument("HASH_DIR", help="Where the MD5 hashes will be stored.")
    parser.add_argument("--O_START", dest='oStart', \
                        help="The starting orbit to process. Only include this if the -o flag is not given.", \
                        type=int )
    parser.add_argument("--O_END", dest='oEnd', \
                        help="The ending orbit to process. Only include this if the -o flag is not given.", \
                        type=int )
    parser.add_argument("--query-loc", dest='queryLoc', help="Where the SQLite query results will be temporarily stored.\
                        This will default to /dev/shm if no value is provided.",\
                        type=str, default="/dev/shm/" )
    args = parser.parse_args()

    # Make paths absolute
    SQLite = os.path.abspath(args.SQLite)
    queryLoc = os.path.abspath(args.queryLoc)
    OUT_DIR = os.path.abspath(args.OUT_DIR)


    # TODO: Uncomment the below code when the --orbit-file argument is finally implemented
    # Check that the arguments passed to script are valid
    if args.orbitFile:
        raise NotImplementedError('--orbit-file parameter not implemented!')

        if args.oStart or args.oEnd:
            eprint("ERROR: --O_START and --O_END cannot be present if --orbit-file is given!")
            return 1
    
    if not args.orbitFile:
        if not args.oStart or not args.oEnd:
            eprint("ERROR: If --orbit-file isn't given, both --O_START and --O_END must be given!")
            return 1

        # Check that oEnd >= oStart
        if not args.oEnd >= args.oStart:
            eprint("ERROR: --O_END must be >= --O_START!")
            return 1
 
    #______________VARIABLES_______________#
    
    # Get the absolute path of this script
    SCRIPT_PATH = os.path.realpath(__file__)

    # Get the basic fusion project path
    BF_PATH = ""
    if 'basicFusion/' in SCRIPT_PATH:
        head, sep, _ = SCRIPT_PATH.partition('basicFusion/')
        BF_PATH = head + sep
    else:
        eprint("ERROR: This script is not in a basicFusion repository!")
        return 1

    # Get the queries.bash path
    SQLqueries = BF_PATH + "/metadata-input/queries.bash"

    # MPI VARIABLES
    comm = MPI.COMM_WORLD
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()


    #__________END VARIABLES__________#
    
    # Prepare the orbit number list to process
    orbits = []

    if args.orbitFile:
        with open(args.orbitFile, 'r' ) as f:
            for line in f:
                # Convert the string to an integer type
                line.strip()
                try:
                    integer = int(line)
                except ValueError:
                    eprint("The line '{}' in the orbit file is not an integer!".format(line))
                    return 1

                orbits.append(integer)

    # The orbit file was not passed, so use the orbit start and orbit end parameters
    else:
        for x in range( args.oStart, args.oEnd + 1 ):
            orbits.append(x)


    # Define which portions of the orbits list to work on using the MPI rank
    processBin = findJobPartition( size, orbits )
    # processBin is a list of size "size" (the MPI size) that tells each rank how many orbits it should handle.
    # Each process knows how many every other process will handle. Using this information, we can figure out the
    # start and end index of the orbits list that each rank will handle
    startIndex = 0
    endIndex = 0
    for i in xrange( 0, rank ):
        startIndex = startIndex + processBin[i]

    endIndex = startIndex + processBin[rank] - 1

    # For every orbit in the orbits list, query the SQLite database and save the result into a temporary file
    pList = {}
    
    for i in xrange( startIndex, endIndex + 1 ): 
        # Delete any temporary files if they exists
        try:
            os.remove( "{}/{}.txt".format(queryLoc, orbits[i] ) )
        except:
            pass

        # Query for each instrument
        for instr in ("MOP", "CER", "AST", "MIS", "MOD"):
            queryFiles( orbits[i], SQLqueries, SQLite, queryLoc, instr )

        # We are ready to create the tar file for orbit orbits[i]
        createTar( orbits[i], "{}/{}.txt".format(queryLoc, orbits[i]), args.OUT_DIR , args.HASH_DIR)

        # Delete the temporary file created by queryFiles
        
        try:
            os.remove( "{}/{}.txt".format(queryLoc, orbits[i] ) )
        except:
            pass

    return 0

if __name__ == '__main__':
   
    main() 
