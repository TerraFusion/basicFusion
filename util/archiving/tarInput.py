"""
@author : Landon Clipp
@email : clipp2@illinois.edu
"""

from __future__ import print_function
import os, sys
import argparse
import tarfile
import subprocess
from multiprocessing import Process

def eprint( *args, **kwargs):
    print( *args, file=sys.stderr, **kwargs )

def queryFiles( orbits, SQLqueries, args, tmpLoc, instr ):
    """         queryFiles
    DESCRIPTION:
        This function handles the task of querying the SQLite database based on the list of orbits passed to it.
        It only queries for a singular instrument and appends the result to a text file. This text file is
        located at the path: [tmpLoc]/[orbitNum].txt. This function will also create n number of independent threads
        for querying the database where n = number of orbits in the orbits list.

    ARGUMENTS:
        orbits (list)           -- A Python list of integers denoting which orbits to query for.
        SQLqueries (string)     -- A path to the queries.bash file. This file contains a series of SQLite calls
                                   wrapped inside of bash functions.
        args (Namespace)        -- This object is what is returned by the ArgumentParser.parse_args() call. Each
                                   member variable of this object is simply an argument passed to the whole script.
                                   This function accesses the args.SQLite variable.
        tmpLoc (string)         -- This is a directory path to where the database query results will be stored.
        instr (string)          -- This is a 3 character string denoting which instrument to query for. The valid
                                   values are:
                                   "MOP", "CER", "AST", "MIS", "MOD".

    EFFECTS:
        Appends to the [tmpLoc]/[orbitNum].txt file.

    RETURN:
        None
    """

    procList = []
    
    if instr == "MOP":
        for orbitNum in orbits:
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentOverlappingOrbit", args.SQLite, str(orbitNum), "MOP" ]

            with open("{}/{}.txt".format( tmpLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "CER":
         
        procList = []
        for orbitNum in orbits:
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentOverlappingOrbit", args.SQLite, str(orbitNum), "CER" ]

            with open("{}/{}.txt".format(tmpLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "AST":
         
        procList = []
        for orbitNum in orbits:
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentStartingInOrbit", args.SQLite, str(orbitNum), "AST" ]

            with open("{}/{}.txt".format(tmpLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "MIS":
        procList = []
        for orbitNum in orbits:
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentStartingInOrbit", args.SQLite, str(orbitNum), "MIS" ]

            with open("{}/{}.txt".format(tmpLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    elif instr == "MOD":

        procList = []
        for orbitNum in orbits:
            # SQLcall is the bash call to the queries.bash script. queries.bash is a bash wrapper for SQLite calls
            processCall = [ "bash", SQLqueries, "instrumentInOrbit", args.SQLite, str(orbitNum), "MOD" ]

            with open("{}/{}.txt".format(tmpLoc, orbitNum), "a") as out:
                procList.append( subprocess.Popen( processCall, stdout=out, stderr=out ) )
            
    else:
        raise ValueError("The argument 'instr' should be either MOP, CER, AST, MIS, or MOD.")
    
    exit_codes = [ p.wait() for p in procList ]
   
def createTar( orbitNum, orbitFileList, outputDir ):
    """         createTar
    DESCRIPTION:
        This function takes as input a specific Terra orbit number, a list of input Terra HDF files for that orbit,
        and generates one single tar file containing all of the files listed in orbitFileList.

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

    # Create a tar file
    tar = tarfile.open("{}/{}archive.tar".format(outputDir, orbitNum), "w" )

    with open( orbitFileList, "r" ) as f:
        for line in f:
            tar.add( line.strip(), recursive=False )

    tar.close()
 
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

    parser.add_argument("--orbit-file", dest='orbitFile', help="Path to the text file containing the orbits to process.")
    parser.add_argument("SQLite", help="The SQLite database containing input HDF Terra files.")
    parser.add_argument("OUT_DIR", help="Where the output tar files will reside.")
    parser.add_argument("--O_START", dest='oStart', \
                        help="The starting orbit to process. Only include this if the -o flag is not given.", \
                        type=int )
    parser.add_argument("--O_END", dest='oEnd', \
                        help="The ending orbit to process. Only include this if the -o flag is not given.", \
                        type=int )
    parser.add_argument("--query-loc", dest='queryLoc', help="Where the SQLite query results will be temporarily stored.\
                        This will default to /dev/shm if no value is provided.",\
                        type=str )
    args = parser.parse_args()

    # Check that some orbit information has been provided
    if args.orbitFile:
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
    SQLqueries = BF_PATH + "metadata-input/queries.bash"
    # Where to store the temporary SQL queries. The default is "/dev/shm", a RAM filesystem mount.
    if not args.queryLoc:
        tmpLoc = "/dev/shm"
    else:
        tmpLoc = args.queryLoc

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

    # Delete any temporary files if they exists
    for orbit in orbits:
        try:
            os.remove( "{}/{}.txt".format(tmpLoc, orbit) )
        except:
            pass

    # For every orbit in the orbits list, query the SQLite database and save the result into a temporary file
    queryFiles( orbits, SQLqueries, args, tmpLoc, "MOP" ) 
    queryFiles( orbits, SQLqueries, args, tmpLoc, "CER" ) 
    queryFiles( orbits, SQLqueries, args, tmpLoc, "AST" ) 
    queryFiles( orbits, SQLqueries, args, tmpLoc, "MIS" ) 
    queryFiles( orbits, SQLqueries, args, tmpLoc, "MOD" ) 

    # We are ready to create the tar files
    pList = {}
    for orbit in orbits:
        pList[orbit] = Process( target = createTar, args=(orbit, "{}/{}.txt".format(tmpLoc, orbit), args.OUT_DIR ) ) 
        pList[orbit].start()

    # Wait for all of the processes to finish
    for orbit in orbits:
        pList[orbit].join()
        
    # Delete any temporary files if they exists
    for orbit in orbits:
        try:
            os.remove( "{}/{}.txt".format(tmpLoc, orbit) )
        except:
            pass

    return 0

if __name__ == '__main__':
    
    p = Process(target=main)
    p.start()
    p.join()
