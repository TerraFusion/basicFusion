#!/bin/bash

# @author: Landon Clipp
# @email: clipp2@illinois.edu
usage(){

    description="Usage: $0 [SQLite database] [Out dir] [SrcID] [DestID] [DestDir] [Ostart] [Oend]

DESCRIPTION:
\tThis script handles tarring all of the input Basic Fusion files and then transferring them to NCSA Nearline.
\tThis script requires the following:
\t\ttarInput.py         -- Found in basicFusion/util/archiving. This tars the files by orbit.
\t\tglobusTransfer.py   -- Found in basicFusion/util/globus. This transfers the tar files to Nearline.

\tThe tar files will be processed in batches of a year. This script requires the basicFusion/metadata-input/data/Orbit_Path_Time.txt
\tfile to determine the boundaries of each year (from 2000 to 2016). It also requires the basicFusion/metadata-input/queries.bash
\tfile to perform SQLite queries. These files only need to exist at these paths, they don't need to be passed explicitly.

ARGUMENTS:
\t[SQLite database]     -- The path to the SQLite database containing records of all the input Terra HDF files.
\t[Out dir]             -- Where the intermediary tar files will reside on this machine.
\t[SrcID]               -- The source Globus endpoint ID (the machine running this script)
\t[DestID]              -- The destination Globus endpoint ID (where the tar files will be written to)
\t[DestDir]             -- The destination directory where tar files will finally reside on the DestID endpoint
\t[Ostart]              -- The starting orbit to process
\t[Oend]                -- The ending orbit to process
"
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ $# != 7 ]; then
    usage
    exit 1
fi

#
#       ARGUMENT PARSING
#

SQL_DB="$1"
if [ ! -e "$SQL_DB" ]; then
    echo "Error: The SQL Database $SQL_DB does not exist!" >&2
    exit 1
fi
shift

tempDir="$1"
if [ ! -d "$tempDir" ]; then
    echo "Error: The directory $tempDir does not exist!" >&2
    exit 1
fi
shift

srcID="$1"
shift
destID="$1"
shift
destDir="$1"
shift
oStart="$1"
shift
oEnd="$1"

SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BF_PATH="${SCRIPT_PATH%/basicFusion/*}/basicFusion"
QUERIES="$BF_PATH/metadata-input/queries.bash"
ORBIT_TIMES="$BF_PATH/metadata-input/data/Orbit_Path_Time.txt"
PY_ENV="$BF_PATH/externLib/BFpyEnv"

TAR_JOB_NPJ=5                   # Tar job nodes per job
TAR_JOB_PPN=20                  # Tar job processors per node
TAR_JOB_WALLTIME="12:00:00"     # Walltime of tar jobs
TAR_JOB_NAME="TerraTar"         # Name of the PBS job
GLOBUS_WALLTIME="24:00:00"      # The Globus transfer walltime
GLOBUS_NAME="TerraGlobus"       # The Globus transfer job name

# Check that the queries.bash and Orbit_Path_Time.txt files exists
if [ ! -e "$QUERIES" ]; then
    echo "Error: The $QUERIES file was not found!" >&2
    exit 1
fi

if [ ! -e "$ORBIT_TIMES" ]; then
    echo "Error: The $ORBIT_TIMES file was not found!" >&2
    exit 1
fi

# Check that the Python virtual environment exists
if [ ! -d "$PY_ENV" ]; then
    echo "Error: The $PY_ENV virtual environment does not exist! Use the" >&2
    echo "basicFusion/util/configureEnv.sh script to create this"
    exit 1
fi

# Check that the Globus CLI is logged in
# source the BFpyEnv
source "$PY_ENV/bin/activate"
echo "Logging into the Globus CLI..."
python "$BF_PATH"/util/globus/globusTransfer.py --check-login $srcID $destID / /

echo "Activating source Globus endpoint..."
globus endpoint activate $srcID
echo "Activating destination globus endpoint..."
globus endpoint activate $destID

#
#       Tar file job creation       
#
# We are ready to create the jobs for tarring the files. Batches will be processed by the
# year.

# Make a directory to hold all of the job files

jobFiles="$BF_PATH/jobFiles/archive/"
mkdir -p "$jobFiles"
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the $jobFiles directory!" >&2
    exit 1
fi

# Find what the highest numbered "run" directory is and grab the number from it.
# You can run this "bashism" nonsense in the terminal to see what it's doing.
lastNum=$(ls "$jobFiles" | grep run | sort --version-sort | tail -1 | tr -dc '0-9')

if [ ${#lastNum} -eq 0 ]; then
    lastNum=-1
fi

let "lastNum++"

# Make the "run" directory
runDir="$jobFiles/run$lastNum"
echo "Generating files for parallel execution at: $runDir"

mkdir "$runDir"
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the run directory!" >&2
    exit 1
fi
# Clean up the path for runDir (remove unnecessary '/' characters)
runDir="$(cd "$runDir" && pwd)"

mkdir "$runDir/tar"
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the $runDir/tar directory!" >&2
    exit 1
fi

mkdir "$runDir/globus"
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the $runDir/globus directory!" >&2
    exit 1
fi

#
#       Find how the jobs will be partitioned
#

declare -a jobNumOrbits
declare -a jobYear
job=-1
jobNumOrbits[$i]=0
curOrbit=$oStart
prevYear=0

# WARNING: This loop takes a long time!!! Possibile TODO is to make this loop faster somehow.
# Step through each line in the $ORBIT_TIMES file
while read -r line; do

    # Keep stepping through the file until we've found the first line that matches $curOrbit
    lineOrbitNum=$(echo "$line" | cut -d' ' -f1)
    if [ $curOrbit -gt $lineOrbitNum ]; then
        continue
    elif [ $curOrbit -lt $lineOrbitNum ]; then
        echo "ERROR: The variable lineOrbitNum outpaced the variable curOrbit! curOrbit should always" >&2
        echo "be greater than or equal to lineOrbitNum!" >&2
        exit 1
    fi

    # The previous if/elif lines assert that curOrbit equals lineOrbitNum.
    # lineOrbitNum is simply the orbit found at the beginning of $line. lineOrbitNum and
    # curOrbit should always equal each other.

    # As long as we haven't reached the end orbit...
    if [ $curOrbit -le $oEnd ]; then

        # Get the current year from the start time of the orbit
        curYear=$(echo "$line" | cut -d' ' -f3 | cut -d'-' -f1)
        
        # We are still on the same year
        if [ $curYear == $prevYear ]; then
            jobNumOrbits[$job]=$((jobNumOrbits[$job] + 1))
            jobYear[$job]=$curYear
            let "curOrbit++"
            prevYear=$curYear

        # We have hit a new year
        else
            echo "Determining the orbits for year $curYear."
            let "job++"
            jobNumOrbits[$job]=1 
            jobYear[$job]=$curYear
            let "curOrbit++"
            prevYear=$curYear
        fi
        
    # Else break from this entire loop
    else
        break
    fi

done < "$ORBIT_TIMES"


# We now have the number of orbits that each job will process, up to $job number of jobs.
# Each job handles only one specific year of data.

#
#       Find orbit start and end for each job
#

declare -a jobStart
declare -a jobEnd

# The first job orbit calculation needs to be outside the loop since jobStart is dependent
# on the previous job's jobEnd value (and obviously, the first job has no "previous" job to
# depend on )

jobStart[0]=$oStart
jobEnd[0]=$(( ${jobStart[0]} + ${jobNumOrbits[0]} - 1 ))

for i in $(seq 1 $job); do

    # jobStart[i] = jobEnd[i-1] + 1
    jobStart[$i]=$(( ${jobEnd[ $((i - 1)) ]} + 1))
    # jobEnd[i] = jobStart[i] + jobNumOrbits[i] - 1
    jobEnd[$i]=$(( ${jobStart[$i]} + ${jobNumOrbits[$i]} - 1 ))

done

# Print the job information
for i in $(seq 0 $job); do
    echo "jobYear: ${jobYear[$i]} jobNumOrbits: ${jobNumOrbits[$i]} jobStart: ${jobStart[$i]} jobEnd: ${jobEnd[$i]}"
done

# We now have all the parameters we need for each job!
# source the BFpyEnv
source "$PY_ENV/bin/activate"


    ######################################################################
    #       Make PBS scripts for the tarring and transfer jobs           #
    ######################################################################

cd $runDir
mkdir -p PBSscripts
mkdir -p logs
mkdir -p logs/tar
mkdir -p logs/transfer

cd PBSscripts
mkdir -p tarJobs
mkdir -p transferJobs

prevJobID="0"

echo "Generating PBS scripts in: $(pwd)"

for i in $(seq 0 $job ); do
    destYearDir="$destDir/${jobYear[$i]}"

    # Make a directory at destID for the current year
    printf "\n\tNOTE: Ignore Globus errors that indicate that the directory already exists!!!\n"
    globus mkdir $destID:"$destYearDir"

    # Find the number of MPI processes
    MPI_numProc=$(( ${TAR_JOB_NPJ} * ${TAR_JOB_PPN} ))
    # Make the tar job PBS script
    pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=${TAR_JOB_NPJ}:ppn=${TAR_JOB_PPN}
#PBS -l walltime=$TAR_JOB_WALLTIME
#PBS -N ${TAR_JOB_NAME}_${jobYear[$i]}
export PMI_NO_PREINITIALIZE=1
export PMI_NO_FORK=1
cd $runDir
source \"$PY_ENV/bin/activate\"

# The following line removes any files that may exist in tempDir
rm -rf \"$tempDir\"
mkdir \"$tempDir\"
module load mpich
mpirun -np $MPI_numProc python \"$BF_PATH/util/archiving/tarInput.py\" --O_START ${jobStart[$i]} --O_END ${jobEnd[$i]} $SQL_DB $tempDir 1> \"$runDir/logs/tar/stdout$i.txt\" 2> \"$runDir/logs/tar/stderr$i.txt\"
exit \$?
"
    echo "$pbsFile" > tarJobs/job$i.pbs

    # Make the transfer job PBS script
pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=1:ppn=1
#PBS -l walltime=$GLOBUS_WALLTIME
#PBS -N ${GLOBUS_NAME}_${jobYear[$i]}
export PMI_NO_PREINITIALIZE=1
export PMI_NO_FORK=1
cd $runDir
source \"$PY_ENV/bin/activate\"
python \"$BF_PATH\"/util/globus/globusTransfer.py --wait $srcID $destID \"$tempDir\" \"$destDir\" 1> \"$runDir/logs/transfer/stdout$i.txt\" 2> \"$runDir/logs/transfer/stderr$i.txt\"
exit \$?
"

    echo "$pbsFile" > transferJobs/job$i.pbs

    # Submit the jobs to the queue!!!
    # Notice that the "depend" line makes the jobs serially dependent. For instance, if we have 3 years of data to transfer, the jobs will
    # look like this:
    # 2000 tar -> 2000 transfer -> 2001 tar -> 2001 transfer -> 2002 tar -> 2002 transfer

    if [ "$prevJobID" == "0" ]; then
        prevJobID=`qsub tarJobs/job$i.pbs`
        retVal=$?
    else
        prevJobID=`qsub -W depend=afterok:$prevJobID tarJobs/job$i.pbs`
        retVal=$?
    fi

    if [ $retVal -ne 0 ]; then
        echo "Error: Failed to submit job to the queue."
        exit 1
    fi

    prevJobID=`qsub -W depend=afterok:$prevJobID transferJobs/job$i.pbs`
    retVal=$?
    if [ $retVal -ne 0 ]; then
        echo "Error: Failed to submit job to the queue."
        exit 1
    fi

done

