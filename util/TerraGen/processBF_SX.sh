#!/bin/bash

usage(){

    printf "\nUSAGE: $0 [input granule listing path] [orbit start] [orbit end] [output path] [OPTS]\n" >&2
    description="NAME: process\"BasicFusion\"_\"SchedulerX\".sh
DESCRIPTION:
 
\tThis script automatically generates all of the files necessary to submit multiple instantiations of the basicFusion code
\ton the compute nodes of Blue Waters. One call to the basicFusion program will generate one single granule of output.
\tBased on the limits provided by the JOB PARAMETERS variables, this script determines how many resources to request from
\tthe Blue Waters resource manager, and it will also automatically submit all of the jobs to the queue.

\tAll of the variables in the JOB PARAMETERS section are configurable. These values must be within acceptable ranges as
\tdetermined by the Blue Waters user guide. This script assumes that the granule file listings for the input HDF granules
\thave already been generated.

\tThis script uses the Scheduler.x program to submit the task to the Blue Waters job scheduler.
\tNCSA Scheduler can be found at: https://github.com/ncsa/Scheduler

\tLog files for the job are stored in the basicFusion/jobFiles/BFprocess directory.

ARGUMENTS:
\t[input granule listing path] -- The path where all the input granule listings reside. These files cannot be within any
\t                                subdirectory of this path.
\t[orbit start]                -- The starting orbit to process
\t[orbit end]                  -- The ending orbit to process
\t[output path]                -- This is where all the output Basic Fusion files will go.
\t[OPTS]                       -- Valid options are:
\t                                --GZIP [1-9]      Sets the HDF compression level
\t                                --CHUNK           This enables HDF chunking
"   
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ $# -lt 4 -o $# -gt 7 ]; then
    usage
    exit 1
fi

LISTING_PATH=$1
if [ ! -d "$LISTING_PATH" ]; then
    echo "Illegal parameter:" >&2
    printf "\t$LISTING_PATH\n" >&2
    printf "\tIs not a valid path to a directory.\n" >&2
    exit 1
fi
shift
OSTART=$1
shift
OEND=$1
shift
OUT_PATH=$1                                             # Where the resultant output HDF files will go
OUT_PATH=$(cd "$OUT_PATH" && pwd)
shift
USE_GZIP=0
USE_CHUNK=0

intREG='^[0-9]+$'       # An integer regular expression

# Do the OPTS argument parsing
for i in $(seq 1 "$#"); do
    if [ "$1" == "--GZIP" ]; then
        shift
        USE_GZIP=$1

        # Assert that USE_GZIP is an integer
        if ! [[ $USE_GZIP =~ $intREG ]]; then
            echo "An integer between 1-9 must follow the --GZIP option!" > &2
            exit 1
        fi

        # Assert that USE_GZIP is between 1 through 9
        if [ $USE_GZIP -lt 1 -o $USE_GZIP -gt 9 ]; then

            echo "An integer between 1-9 must follow the --GZIP option!" > &2
            exit 1
        fi

        echo "_____GZIP ENABLED______"
    else
        echo "_____GZIP DISABLED_____"
    fi

    if [ "$1" == "--CHUNK" ]; then
        USE_CHUNK=1
        echo "_____HDF CHUNKING ENABLED______"
    else
        echo "_____HDF CHUNKING DISABLED_____"
    fi

    # If neither of the valid options are set, then this parameter must be illegal
    if [ $USE_CHUNK -eq 0 -a $USE_GZIP -eq 0 ]; then
        echo "Illegal parameter: $1"
        exit 1
    fi

    shift
done

#____________JOB PARAMETERS____________#
MAX_NPJ=32                                              # Maximum number of nodes per job
MAX_PPN=16                                              # Maximum number of processors (cores) per node.
MAX_NUMJOB=30                                           # Maximum number of jobs that can be submitted simultaneously
WALLTIME="06:00:00"                                     # Requested wall clock time for the jobs
QUEUE="normal"                                          # Which queue to put the jobs in
#--------------------------------------#

#____________FILE NAMING_______________#

FILENAME='TERRA_BF_L1B_O${orbit}_${orbit_start}_F000_V000.h5'          # The variables "orbit" and "orbit_start" will be expanded later on.

#--------------------------------------#



ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"    # Get the absolute path of this script
LISTING_PATH="$(cd $(dirname $LISTING_PATH) && pwd)/$(basename $LISTING_PATH)"  # Make the LISTING_PATH an absolute path
BIN_DIR="$ABS_PATH/../../bin"                                   # Get the absolute path of the basic fusion binary directory
BIN_DIR="$(cd $(dirname $BIN_DIR) && pwd)/$(basename $BIN_DIR)"
BF_PROG="$BIN_DIR/basicFusion"                                  # Get the absolute path of the basicFusion program
BF_PATH="$(cd "$BIN_DIR/../" && pwd)"                           # Get the path of BF repository
SCHED_PATH="$BF_PATH/externLib/Scheduler"                       # Get the path of the Scheduler repository
ORBIT_INFO="$BF_PATH/metadata-input/data/Orbit_Path_Time.txt"   # Get the path of the Orbit_info file
hn="$(hostname)"                                                # host name of this machine


# Check that Scheduler has already been downloaded and compiled
if [ ! -x "$SCHED_PATH" ]; then
    echo "Fatal error: Scheduler has not been downloaded/compiled or the Fortran executable has not been flagged for execution. Please run the configureEnv.sh script in the basicFusion/util directory." >&2
    exit 1
fi

# Check that the ORBIT_INFO file exists
if [ ! -e "$ORBIT_INFO" ]; then
    echo "Fatal error: The $ORBIT_INFO file does not exist. Please refer to the GitHub wiki on how to generate this file." >&2
    exit 1
fi


numOrbits=$(($OEND - $OSTART + 1))                      # Total number of orbits to generate
numProcessPerJob=$((numOrbits / $MAX_NUMJOB)) 


# Find the remainder of this division
let "numExtraProcess= $numOrbits % $MAX_NUMJOB"

# Find how many jobs need to be submitted assuming each job is completely filled (MAX_NPJ * MAX_PPN - 1)
let "numJobs= $numOrbits / $(($MAX_NPJ * $MAX_PPN - 1))"

# If the number of jobs is greater than MAX_NUMJOB, then we will need to overfill
if [ $numJobs -ge $MAX_NUMJOB ]; then 
    
    for i in $(seq 0 $((MAX_NUMJOB - 1)) ); do
        PPN[$i]=$MAX_PPN
        NPJ[$i]=$MAX_NPJ

        numProcess[$i]=$numProcessPerJob
        let "numProcess[$i] += $numExtraProcess / $MAX_NUMJOB"

        if [ $i -eq $((MAX_NUMJOB-1)) ]; then
            let "numProcess[$i] += $numExtraProcess % $MAX_NUMJOB"
        fi
    done

    numJobs=$((MAX_NUMJOB-1))

# Else if we do not need to overfill
else
    for i in $(seq 0 $((numJobs - 1)) ); do         # Assign the first few jobs to have maximum number of PPN and NPJ
        PPN[$i]=$MAX_PPN
        NPJ[$i]=$MAX_NPJ
        numProcess[$i]=$((MAX_PPN * MAX_NPJ - 1))       # Each job will handle MAX_PPN * MAX_NPJ - 1 processes.
                                                        # It's -1 because Scheduler needs one core to itself.
    done

    let "numExtraProcess = $numOrbits - ((MAX_NPJ * MAX_PPN - 1) * numJobs)"  # Find the remaining number of jobs to process
    numProcess[$numJobs]=$numExtraProcess
    
    if [ $numExtraProcess -gt 0 ]; then             # If the remaning number of processes is greater than 0.
        numNodes=$((numExtraProcess / MAX_PPN))     # Find the number of nodes the last job needs to have.

        if [ $numNodes -eq 0 ]; then                # If it needs less than one node (assuming MAX_PPN), only request
            PPN[$numJobs]=$((numExtraProcess+1))    # exact number of cores that we need + 1, and only one node.
            NPJ[$numJobs]=1                         # We add +1 to it because NCSA Scheduler requires 1 core for itself.
                                                    # We don't add +1 in the case where there are many processes to be run
                                                    # because the time difference would be negligible. This is just an
                                                    # edge case consideration (what if only 1 granule is being processed?)
        else
            PPN[$numJobs]=$MAX_PPN                  # Else, assign the MAX_PPN and find number of nodes needed to process remaining orbits
            
            if [ $((numExtraProcess % MAX_PPN)) -eq 0 ]; then
                NPJ[$numJobs]=$numNodes             # An edge case where the numExtraProcesses is a multiple of MAX_PPN
            else
                NPJ[$numJobs]=$((numNodes + 1))
            fi
        fi
    fi
fi

# For each job, figure out the range of orbits it will be responsible for
accumulator=$OSTART

if [ $((numOrbits % $((MAX_NPJ * MAX_PPN)) )) -ne 0 ]; then
    let "numJobs++"
fi

# Set the starting and ending orbits for each job
for i in $(seq 0 $((numJobs-1)) ); do
    jobOSTART[$i]=$accumulator
    # Orbit end = orbit start + numOrbitsForJob - 1
    jobOEND[$i]=$((${jobOSTART[$i]} + ${numProcess[$i]} - 1))
    accumulator=$((${jobOEND[$i]} + 1))
done

# =================================== #
# Print the node and job information  #
# =================================== #
for i in $(seq 0 $((numJobs-1)) ); do
    echo "JOB PARAMETERS"
    printf '%d PPN: %-5s NPJ: %-5s numProcess: %-5s start: %-5s end: %-5s\n' $i ${PPN[$i]} ${NPJ[$i]} ${numProcess[$i]} ${jobOSTART[$i]} ${jobOEND[$i]}
done


# Time to create the files necessary to submit the jobs to the queue
BF_PROCESS="$BF_PATH/jobFiles/BFprocess"
mkdir -p "$BF_PROCESS"

# Find what the highest numbered "run" directory is and grab the number from it.
# If you want, you can run this "bashism" nonsense in the terminal yourself to see what it's doing.
lastNum=$(ls "$BF_PROCESS" | grep run | sort --version-sort | tail -1 | tr -dc '0-9')

# Just keep it zero-indexed... used to offset the let "lastNum++" line
if [ ${#lastNum} -eq 0 ]; then
    lastNum=-1
fi

let "lastNum++"

# Make the "run" directory
runDir="$BF_PROCESS/run$lastNum"
echo "Generating files for parallel execution at: $runDir"

mkdir "$runDir"
if [ $? -ne 0 ]; then
    echo "Failed to make the run directory!"
    exit 1
fi


# Save the current date into the run directory
date > "$runDir/date.txt"

# Make the job directories inside each run directory
mkdir -p "${runDir}/programCalls"
for i in $(seq 0 $((numJobs-1)) ); do
    jobDir[$i]="${runDir}/programCalls/job$i"
    mkdir -p "${jobDir[$i]}"                        # Makes a directory for each job
done

# The MPI program is different between ROGER and Blue Waters
mpiProg=""
loadMPICH=""
if [ "$hn" == "cg-gpu01" ]; then
    loadMPICH="module load mpich"
    mpiProg="mpirun"
# Else assume that we are on Blue Waters
else
    mpiProg="aprun"
fi
logDir="$runDir/logs"
mkdir "$logDir"
mkdir "$logDir"/$mpiProg
mkdir "$logDir"/$mpiProg/errors
mkdir "$logDir"/$mpiProg/logs

########################
#   MAKE PBS SCRIPTS   #
########################
 
cd $runDir
mkdir -p PBSscripts
cd PBSscripts
echo "Generating PBS scripts in: $(pwd)"

nodeType=""
# If we're not on ROGER, assume that we are on Blue Waters and set the
# node type to xe
if ! [ "$hn" == "cg-gpu01" ]; then
    nodeType=":xe"
fi

for i in $(seq 0 $((numJobs-1)) ); do

    # littleN (-n) tells us how many total processes in the job.
    # 
    littleN=$(( ${NPJ[$i]} * ${PPN[$i]} ))
    
    
    pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=${NPJ[$i]}:ppn=${PPN[$i]}${nodeType}
#PBS -l walltime=$WALLTIME
#PBS -q $QUEUE
#PBS -N TerraProcess_${jobOSTART[$i]}_${jobOEND[$i]}
export PMI_NO_PREINITIALIZE=1
export USE_GZIP=${USE_GZIP}
export USE_CHUNK=${USE_CHUNK}
export PMI_NO_FORK=1
cd $runDir
source /opt/modules/default/init/bash
module load hdf4 hdf5
$loadMPICH
$mpiProg -n $littleN -N ${PPN[$i]} -R 0 time $SCHED_PATH/scheduler.x $runDir/processLists/job$i.list /bin/bash -noexit 1> $logDir/aprun/logs/job$i.log 2> $logDir/aprun/errors/$job$i.err
exit 0
"

    echo "$pbsFile" > job$i.pbs

done

############################
#    MAKE PROCESS LISTS    #
############################

# The process lists tell each independent job, up to MAX_NUMJOB number of jobs, what bash scripts to execute.
# Each of these bash scripts contain a single function call to the program. They will be created later on.
 
cd ..
mkdir -p processLists
cd processLists
echo "Generating process lists in: $(pwd)"

for i in $(seq 0 $((numJobs-1)) ); do
    for orbit in $(seq ${jobOSTART[$i]} ${jobOEND[$i]} ); do
        echo "$runDir ${jobDir[$i]}/orbit$orbit.sh" >> job$i.list
    done
done

############################
#    MAKE PROGRAM CALLS    #
############################
mkdir "$logDir"/basicFusion
mkdir "$logDir"/basicFusion/errors
mkdir "$logDir"/basicFusion/logs
BFfullPath="$runDir/$(basename $BF_PROG)"
logPath="$runDir/logs/basicFusion"
# Copy the basicFusion binary to the job directory. Use this copy of the bianry.
cp $BF_PROG $runDir 

for job in $(seq 0 $((numJobs-1)) ); do
    echo "Writing program calls in: ${jobDir[$job]}"

    for orbit in $(seq ${jobOSTART[$job]} ${jobOEND[$job]} ); do
        # Read Orbit start time from file
        
        filter="$orbit "        # The filter for grep
        # Grab the orbit start time from ORBIT_INFO
        orbit_start=$(grep $filter $ORBIT_INFO | cut -f3 -d" ") 
        # Get rid of the '-' characters
        orbit_start=${orbit_start//-/}
        # Get rid of the 'T' characters
        orbit_start=${orbit_start//T/}
        # Get rid of the ':' characters
        orbit_start=${orbit_start//:/}
        # Get rid of the 'Z'
        orbit_start=${orbit_start//Z/}
        evalFileName=$(eval echo "$FILENAME")

        outGranule="$OUT_PATH/$evalFileName"

        # BF program call looks like this:
        # ./basicFusion [output HDF5 filename] [input file list] [orbit_info.bin]
        programCalls="\
\"$BFfullPath\" \"$outGranule\" \"$LISTING_PATH/input$orbit.txt\" \"$BIN_DIR/orbit_info.bin\" 2> \"$logPath/errors/$orbit.err\" 1> \"$logPath/logs/$orbit.log\"

# We will now save the MD5 checksum.
mkdir -p \"$OUT_PATH/checksum\"
md5sum \"$OUT_PATH/$evalFileName\" > \"$OUT_PATH/checksum/$orbit.md5\"
"

        echo "$programCalls" > "${jobDir[$job]}/orbit$orbit.sh"

        chmod +x "${jobDir[$job]}/orbit$orbit.sh"
 
    done # DONE with for orbit in....
    
    ###################
    #    CALL QSUB    #
    ###################

    echo "Submitting $runDir/PBSscripts/job$job.pbs to queue."
    curDir="$(pwd)"
    cd $runDir/PBSscripts
    qsub job$job.pbs 
    # cd back into the curDir directory
    cd "$curDir"
    echo

done # DONE with for job in....

echo
exit 0
