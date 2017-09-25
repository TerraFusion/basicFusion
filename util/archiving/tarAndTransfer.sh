#!/bin/bash

# @author: Landon Clipp
# @email: clipp2@illinois.edu
usage(){

    description="Usage: $0 [SQLite database] [Out dir] [SrcID] [DestID] [DestDir] [Ostart] [Oend] [--nodes NODES]

DESCRIPTION:
\tThis script handles tarring all of the input Basic Fusion files and then transferring them to NCSA Nearline.
\tThis script requires the following:
\t\ttarInput.py         -- Found in basicFusion/util/archiving. This tars the files by orbit.
\t\tglobusTransfer.py   -- Found in basicFusion/util/globus. This transfers the tar files to Nearline.

\tThe tar files will be processed in batches of a year. This script requires the basicFusion/metadata-input/data/Orbit_Path_Time.txt
\tfile to determine the boundaries of each year (from 2000 to 2016). It also requires the basicFusion/metadata-input/queries.bash
\tfile to perform SQLite queries. These files only need to exist at these paths, they don't need to be passed explicitly.

ARGUMENTS:

\tPOSITIONAL

\t[SQLite database]     -- The path to the SQLite database containing records of all the input Terra HDF files.
\t[Out dir]             -- Where the intermediary tar files will reside on this machine.
\t[SrcID]               -- The source Globus endpoint ID (the machine running this script)
\t[DestID]              -- The destination Globus endpoint ID (where the tar files will be written to)
\t[DestDir]             -- The destination directory where tar files will finally reside on the DestID endpoint
\t[Ostart]              -- The starting orbit to process
\t[Oend]                -- The ending orbit to process

\tOPTIONAL

\t--nodes NODES         -- Specify the number of nodes to use for the tarring jobs. Defaults to 5.
"
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ $# -ne 7 -a $# -ne 9 ]; then
    usage
    exit 1
fi

intReg='^[0-9]+$'       # Integer regular expression
programCall="$0 $@"        # Save the command line arguments for this script's call

#
#       ARGUMENT PARSING
#

cmdLineArgs="$@"

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
shift


SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BF_PATH="${SCRIPT_PATH%/basicFusion/*}/basicFusion"
QUERIES="$BF_PATH/metadata-input/queries.bash"
ORBIT_TIMES="$BF_PATH/metadata-input/data/Orbit_Path_Time.txt"
PY_ENV="$BF_PATH/externLib/BFpyEnv"

TAR_JOB_NPJ=5                   # Tar job nodes per job. This specifies the default value.
                                # Will be overwritten if --nodes is provided.
TAR_JOB_PPN=20                  # Tar job processors per node
TAR_JOB_WALLTIME="12:00:00"     # Walltime of tar jobs
TAR_JOB_NAME="TerraTar"         # Name of the PBS job
GLOBUS_WALLTIME="24:00:00"      # The Globus transfer walltime
GLOBUS_NAME="TerraGlobus"       # The Globus transfer job name

    
if [ "$1" == "--nodes" ]; then
    shift
    numNodes="$1"

    if ! [[ "$numNodes" =~ $intReg ]]; then
        echo "Error: A positive integer should follow the --nodes parameter!" >&2
        exit 1
    fi

    TAR_JOB_NPJ="$numNodes"

elif [ "$1" != "" ]; then
    echo "Error: Illegal parameter: $1" >&2
    exit 1
fi

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
    echo "basicFusion/util/configureEnv.sh script to create this" >&2
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

# Save the command line arguments of this script's call into a file
echo "$programCall" > "$runDir"/cmdLineArgs.txt



#
#       Find how the jobs will be partitioned
#

declare -a jobNumOrbits
declare -a jobYear
job=-1
jobNumOrbits[0]=0
curOrbit=$oStart
prevYear=0

# WARNING: This loop takes a long time!!! Possibile TODO is to make this loop faster somehow.
# Step through each line in the $ORBIT_TIMES file

while true; do

    linenum=$(( curOrbit - 999 ))               # The line number we want to read is simply curOrbit - 999. There is a
                                                # one-to-one mapping of orbit to line number.

    line="$(sed "${linenum}q;d" "$ORBIT_TIMES")"

    #line="$(grep -w "^$curOrbit" "$ORBIT_TIMES")"
    # Keep stepping through the file until we've found the first line that matches $curOrbit
    lineOrbitNum=$(echo "$line" | cut -d' ' -f1)
    #if [ $curOrbit -gt $lineOrbitNum ]; then
    #    continue
    #elif [ $curOrbit -lt $lineOrbitNum ]; then
    #    echo "ERROR: The variable lineOrbitNum outpaced the variable curOrbit! curOrbit should always" >&2
    #    echo "be greater than or equal to lineOrbitNum!" >&2
    #    exit 1
    #fi

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

done


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

cd $runDir
mkdir -p PBSscripts
mkdir -p logs
mkdir -p logs/tar
mkdir -p logs/transfer

# Save some information about this job, like the date and the command line args
echo "$cmdLineArgs" > cmdLineArgs.txt
date > date.txt

# Print the job information, append to the cmdLineArgs.txt file
for i in $(seq 0 $job); do
    echo "jobYear: ${jobYear[$i]} jobNumOrbits: ${jobNumOrbits[$i]} jobStart: ${jobStart[$i]} jobEnd: ${jobEnd[$i]} NPJ: ${TAR_JOB_NPJ}" | tee -a cmdLineArgs.txt
done

# We now have all the parameters we need for each job!
# source the BFpyEnv
source "$PY_ENV/bin/activate"


    ######################################################################
    #       Make PBS scripts for the tarring and transfer jobs           #
    ######################################################################


cd PBSscripts
mkdir -p tarJobs
mkdir -p transferJobs

declare -a tarJobID
declare -a globusJobID

echo "Generating PBS scripts in: $(pwd)"

for i in $(seq 0 $job ); do
    destYearDir="$destDir/${jobYear[$i]}"

    # Make a directory at destID for the current year
    printf "\n\tNOTE: Ignore Globus errors that indicate that the directory already exists!!!\n"
    globus mkdir $destID:"$destYearDir"

    # Find the number of MPI processes
    MPI_numProc=$(( ${TAR_JOB_NPJ} * ${TAR_JOB_PPN} ))

    # Each job gets its own directory within tempDir that is just the year of the job
    curStageArea="$tempDir/${jobYear[$i]}"

    # Make the tar job PBS script
    pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=${TAR_JOB_NPJ}:ppn=${TAR_JOB_PPN}
#PBS -l walltime=$TAR_JOB_WALLTIME
#PBS -N ${TAR_JOB_NAME}_${jobYear[$i]}
cd $runDir
source \"$PY_ENV/bin/activate\"

# The following line removes any files that are no longer needed
rm -rf \"$curStageArea\"
mkdir \"$curStageArea\"

module load mpich
# Both stdout/stderr of time and python call will be redirected to stdout_err$i.txt
{ time mpirun -np $MPI_numProc python \"$BF_PATH/util/archiving/tarInput.py\" --O_START ${jobStart[$i]} --O_END ${jobEnd[$i]} $SQL_DB \"$curStageArea\" ; } &> \"$runDir/logs/tar/stdout_err$i.txt\"

"
    echo "$pbsFile" > tarJobs/job$i.pbs

    # Make the transfer job PBS script
pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=1:ppn=1
#PBS -l walltime=$GLOBUS_WALLTIME
#PBS -N ${GLOBUS_NAME}_${jobYear[$i]}
cd $runDir
source \"$PY_ENV/bin/activate\"
globus endpoint activate $srcID
globus endpoint activate $destID
# Both stdout/stderr of time and python call will be redirected to stdout_err$i.txt
{ time python \"$BF_PATH\"/util/globus/globusTransfer.py --wait $srcID $destID \"${curStageArea}\" \"${destYearDir}\"} ; } &> \"$runDir/logs/transfer/stdout_err$i.txt\" 

rm -rf \"$curStageArea\"
"

    echo "$pbsFile" > transferJobs/job$i.pbs

    # Submit the jobs to the queue!!!
    # Notice that the "depend" line makes the jobs serially dependent.
    # This operation is piplined. The two main instructions are a Tar job followed by a Globus transfer job.
    # A Tar job can be executed at the same time of the previous Globus transfer job. As an example, the pipeline
    # looks like this:
    #
    #           TAR0              0
    #          /   
    #       GLOB0    TAR1         1
    #             /
    #            /
    #           /    
    #       GLOB1    TAR2         2
    #             /
    #            /
    #           /
    #       GLOB2    TAR3         3
    #             /
    #            /
    #           /
    #       GLOB3                 4

    #                  #
    #   TAR JOB SUBMIT #
    #                  #
    if [ $i -eq 0 ]; then
        prevTarID=""
        prevGlobID=""
    else
        prevTarID=${tarJobID[ $(( i - 1)) ]}
        prevGlobID=${globusJobID[$((i-1))]}
    fi

    # This is the first Tar job to submit ( $prevTarID is not available )
    if [ ${#prevTarID} -eq 0 ]; then
        tarJobID[$i]=`qsub tarJobs/job$i.pbs`
        retVal=$?

    # This is not the first tar job to submit
    else
        # The tar job needs to depend on the Globus job two iterations before
        if [ $i -le 1 ]; then
            tarJobID[$i]=`qsub -W depend=afterany:$prevTarID tarJobs/job$i.pbs`
            retVal=$?
        else
            twoPrevGlobID=${globusJobID[ $(( i - 2 )) ]}
            tarJobID[$i]=`qsub -W depend=afterany:$prevTarID:$twoPrevGlobID tarJobs/job$i.pbs`
            retVal=$?
        fi
        
    fi

    if [ $retVal -ne 0 ]; then
        echo "Error: Failed to submit Tar job to the queue."
        exit 1
    fi


    #
    #   GLOBUS JOB SUBMIT
    #

    # The Globus job should depend on its respective tar job (the one that was just submitted in the previous code
    # and also the Globus job submitted during the previous loop iteration.
    
    if [ ${#prevGlobID} -eq 0 ]; then
        # There is no previous globus job to be dependent on
        globusJobID[$i]=`qsub -W depend=afterany:${tarJobID[$i]} transferJobs/job$i.pbs`
        retVal=$?
    else
        # Depend on the previous globus job ID
        globusJobID[$i]=`qsub -W depend=afterany:${tarJobID[$i]}:$prevGlobID transferJobs/job$i.pbs`
        retVal=$?
    fi

    if [ $retVal -ne 0 ]; then
        echo "Error: Failed to submit Globus job to the queue."
        exit 1
    fi

done

