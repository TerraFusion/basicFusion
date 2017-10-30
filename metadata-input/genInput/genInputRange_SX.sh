#!/bin/bash
usage(){

    printf "\nUSAGE: $0 [SQLite database filepath] [orbit start] [orbit end] [output path] [OPTS]\n" >&2
    description="NAME: genInputRange_\"SchedulerX\".sh
DESCRIPTION:
 
\tThis script generates a range of BasicFusion input file listings. One call to the "genFusionInput.sh" script will create one input file, but generating a large number of input files is computationally expensive and thus necessitates submitting the task to compute nodes. This script assumes the following conditions are met:

\t1. It is being submitted on the Blue Waters super computer
\t2. The Scheduler fortran program is installed and compiled. This can be downloaded from https://github.com/ncsa/Scheduler.
\t   The fortran executable must be located inside the root Scheduler project directory, and be named "scheduler.x".
\t3. The input HDF files exist on the file system
\t4. The sqlite database containing the file paths to the input HDF files has been generated

\tAll of the variables in the JOB PARAMETERS section are configurable. These values must be within acceptable ranges as determined by the Blue Waters user guide. This script assumes that the granule file listings for the input HDF granules have already been generated.

\tThis script uses the Scheduler.x program to submit the task to the Blue Waters job scheduler.
\tNCSA Scheduler can be found at: https://github.com/ncsa/Scheduler

\tLog files for the job are stored in the basicFusion/jobFiles/BFinputGen directory.

ARGUMENTS:
\t[SQLite database filepath]    -- The path to the SQLite database generated using metadata-input/build scripts.
\t[orbit start]                 -- The starting orbit to process
\t[orbit end]                   -- The path where the resulting input files will reside
\t[out path]                    -- Path where the files will be saved
\t[OPTS]        The available options are:
\t                              --npj [NODES_PER_JOB]        Default of 4
\t                              --cpn [CORES_PER_NODE]       Default of 16
\t                              --numJob [NUM_JOBS]          Default of 1
\t                              --walltime \"[WALLTIME]\"      Default of \"02:00:00\"

\t              NOTE that these options (except for walltime) only provide maximum values to bound what this script requests of the resource manager!
\t              These values are not necessarily what is requested.                
"   
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ $# -lt 4 -o $# -gt 12 ]; then
    usage
    exit 1
fi

int_re='^[0-9]+$'

#____________DEFAULT JOB PARAMETERS____________#
MAX_NPJ=4                                               # Maximum number of nodes per job
MAX_CPN=16                                              # Maximum number of cores per node.
MAX_NUMJOB=1                                            # Maximum number of jobs that can be submitted simultaneously
WALLTIME="02:00:00"                                     # Requested wall clock time for the jobs
QUEUE=""                                                # Which queue to put the jobs in. By default, no queue is specified.
#--------------------------------------#

DB_PATH="$1"; shift
OSTART=$1; shift
OEND=$1; shift
OUT_PATH="$1"; shift                                             # Where the resultant output HDF files will go
OUT_PATH="$(cd "$OUT_PATH" && pwd)"
# Parse the OPTS
# TODO Finish this opt parsing
while [ $# -ne 0 ]; do
    if [ "$1" == "--npj" ]; then
        shift
        if ! [[ $1 =~ $re ]] ; then
            echo "Error: An integer must follow --npj!" >&2
            exit 1
        fi
        MAX_NPJ=$1
        if [ "$MAX_NPJ" -le 0 ]; then
            echo "Error: --npj can't be less than 0!" >&2
            exit 1
        fi
        shift
    elif [ "$1" == "--cpn" ]; then
        shift
        if ! [[ $1 =~ $re ]] ; then
            echo "Error: An integer must follow --cpn!" >&2
            exit 1
        fi
        MAX_CPN="$1"
        shift
    elif [ "$1" == "--numJob" ]; then
        shift
        MAX_NUMJOB="$1"
         if ! [[ $1 =~ $re ]] ; then
            echo "Error: An integer must follow --numJob!" >&2
            exit 1
         fi
        shift
    elif [ "$1" == "--walltime" ]; then
        shift
        WALLTIME="$1"
        shift
    else
        echo "Error: Unrecognized argument: $1" >&2
        exit 1
    fi
done

echo "Making jobs with the following maximum parameters:"
printf "\tMax Nodes Per Job: ${MAX_NPJ}\n"
printf "\tMax Cores Per Node: ${MAX_CPN}\n"
printf "\tMax Number of Jobs: ${MAX_NUMJOB}\n"
printf "\tWalltime: ${WALLTIME}\n"
echo


# Make the DB path absolute
DB_PATH="$(cd $(dirname $DB_PATH) && pwd)/$(basename $DB_PATH)"
hn=$(hostname)  # Hostname of the machine running this script

if [ ! -f "$DB_PATH" ]; then
    echo "The database at:"
    echo "$DB_PATH"
    echo "does not exist."
    exit 1
fi

# Get the absolute path of this script
ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the absolute path of the basic fusion binary directory
BIN_DIR="$ABS_PATH/../../bin"
BIN_DIR="$(cd $(dirname "$BIN_DIR") && pwd)/$(basename $BIN_DIR)"

# Get the path of BF repository
BF_PATH="$(cd "$BIN_DIR/../" && pwd)"

# Get the path of the Scheduler repository
SCHED_PATH="$BF_PATH/externLib/Scheduler"

source "$ABS_PATH"/../../util/findJobPartition

# Check that Scheduler has already been downloaded
if [ ! -d "$SCHED_PATH" ]; then
    echo "Fatal error: Scheduler has not been downloaded. Please run the configureEnv.sh script in the basicFusion/util directory." >&2
    exit 1
fi

echo "Finding the job partitioning. Please be patient..."
declare -a jobOSTART    # Outputs of findJobPartition
declare -a jobOEND
declare -a CPN
declare -a NPJ
declare -a numProcess
declare numJobs
findJobPartition $OSTART $OEND $MAX_NPJ $MAX_CPN $MAX_NUMJOB
echo "Done."
echo


# =================================== #
# Print the node and job information  #
# =================================== #
for i in $(seq 0 $((numJobs-1)) ); do
    printf '%d CPN: %-5s NPJ: %-5s start: %-5s end: %-5s\n' $i ${CPN[$i]} ${NPJ[$i]} ${jobOSTART[$i]} ${jobOEND[$i]}
done


# Time to create the files necessary to submit the jobs to the queue
BF_PROCESS="$BF_PATH/jobFiles/genFusionInput"
mkdir -p "$BF_PROCESS"

# Find what the highest numbered "run" directory is and grab the number from it.
# You can run this "bashism" nonsense in the terminal to see what it's doing.
lastNum=$(ls "$BF_PROCESS" | grep run | sort --version-sort | tail -1 | tr -dc '0-9')

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
date > "${runDir}/date.txt"

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

for i in $(seq 0 $((numJobs-1)) ); do
    echo "#!/bin/bash"                                          >> job$i.pbs
    echo "#PBS -j oe"                                           >> job$i.pbs
    # Special case for ROGER. ROGER has no notion of node type (xe)
    if [ "$hn" == "cg-gpu01" ]; then
        echo "#PBS -l nodes=${NPJ[$i]}:ppn=${CPN[$i]}"           >> job$i.pbs
    # Else assume that we are on Blue Waters
    else
        echo "#PBS -l nodes=${NPJ[$i]}:ppn=${CPN[$i]}:xe"           >> job$i.pbs
    fi
    echo "#PBS -l walltime=$WALLTIME"                           >> job$i.pbs
    if [ ${#QUEUE} -ne 0 ]; then
        echo "#PBS -q $QUEUE"                                       >> job$i.pbs
    fi
    echo "#PBS -N TerraInput_${jobOSTART[$i]}_${jobOEND[$i]}"   >> job$i.pbs
    # The PMI environment variables are added as a workaround to a known Application Level Placement
    # Scheduler (ALPS) bug on Blue Waters. Not entirely sure what they do but BW staff told me
    # that it works, and it does!
    echo "export PMI_NO_FORK=1"                                 >> job$i.pbs
    echo "export PMI_NO_PREINITIALIZE=1"                        >> job$i.pbs
    echo "cd $runDir"                                           >> job$i.pbs
    

    echo "source /opt/modules/default/init/bash &> /dev/null"   >> job$i.pbs
    echo "module load hdf4 hdf5"                                >> job$i.pbs
    echo "$loadMPICH"                                           >> job$i.pbs
    littleN=$(( ${NPJ[$i]} * ${CPN[$i]} ))
    bigR=0

    mpiCall=""
    if [ "$mpiProg" == "mpirun" ]; then
        numRanks=$(( ${NPJ[$i]} * ${CPN[$i]} - 1 ))
        mpiCall="$mpiProg -np $littleN"
    elif [ "$mpiProg" == "aprun" ]; then
        mpiCall="$mpiProg -n $littleN -N ${CPN[$i]} -R $bigR"
    else
        echo "The MPI program should either be mpirun or aprun!" >&2
        exit 1
    fi

    echo "$mpiCall $SCHED_PATH/scheduler.x $runDir/processLists/job$i.list /bin/bash -noexit 1> $logDir/$mpiProg/logs/job$i.log 2> $logDir/$mpiProg/errors/$job$i.err" >> job$i.pbs
    echo "exit 0"                                               >> job$i.pbs

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
mkdir "$logDir"/genFusionInput
mkdir "$logDir"/genFusionInput/errors
mkdir "$logDir"/genFusionInput/logs

for job in $(seq 0 $((numJobs-1)) ); do
    echo "Writing program calls in: ${jobDir[$job]}"

    for orbit in $(seq ${jobOSTART[$job]} ${jobOEND[$job]} ); do
        echo "$ABS_PATH/genFusionInput.sh $DB_PATH $orbit $OUT_PATH/input$orbit.txt 2> $runDir/logs/genFusionInput/errors/$orbit.err 1> $runDir/logs/genFusionInput/logs/$orbit.log

# Remove log/error files if they're empty
if ! [ -s \"$runDir/logs/genFusionInput/errors/${orbit}.err\" ]; then
    rm -f \"$runDir/logs/genFusionInput/errors/${orbit}.err\"
fi

if ! [ -s \"$runDir/logs/genFusionInput/logs/${orbit}.log\" ]; then
    rm -f \"$runDir/logs/genFusionInput/logs/${orbit}.log\"
fi" > "${jobDir[$job]}/orbit$orbit.sh"

    chmod +x "${jobDir[$job]}/orbit$orbit.sh"
    done
done

echo

###################
#    CALL QSUB    #
###################
cd $runDir/PBSscripts
for job in $(seq 0 $((numJobs-1)) ); do
    echo "Submitting $runDir/PBSscripts/job$job.pbs to queue."
    qsub job$job.pbs
done

exit 0
