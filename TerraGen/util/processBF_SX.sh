#!/bin/bash

if [ $# -ne 5 ]; then
    printf "\nUSAGE: $0 [Scheduler directory path] [input granule listing path] [orbit start] [orbit end] [output path]\n" >&2
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

\tLog files for the job are stored in the Scheduler project path."   
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
    exit 1
fi

SCHED_PATH=$1
LISTING_PATH=$2
OSTART=$3
OEND=$4
OUT_PATH=$5                                             # Where the resultant input[orbitNum].txt files will go


#____________JOB PARAMETERS____________#
MAX_NPJ=32                                              # Maximum number of nodes per job
MAX_PPN=16                                              # Maximum number of processors (cores) per node.
MAX_NUMJOB=5                                           # Maximum number of jobs that can be submitted simultaneously
WALLTIME="22:34:49"                                     # Requested wall clock time for the jobs
QUEUE="high"                                          # Which queue to put the jobs in
#--------------------------------------#

#____________FILE NAMING_______________#

FILENAME='TERRA_BF_L1B_O${orbit}_F000_V000.h5'          # The variable "orbit" will be expanded later on.

#--------------------------------------#

# Get the absolute path of this script
ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Make the LISTING_PATH an absolute path
LISTING_PATH="$(cd $(dirname $LISTING_PATH) && pwd)/$(basename $LISTING_PATH)"

# Get the absolute path of the basic fusion binary directory
BIN_DIR="$ABS_PATH/../../bin"
BIN_DIR="$(cd $(dirname $BIN_DIR) && pwd)/$(basename $BIN_DIR)"

# Get the absolute path of the basicFusion program
BF_PROG="$BIN_DIR/basicFusion"

numOrbits=$(($OEND - $OSTART + 1))                      # Total number of orbits to generate
numProcessPerJob=$((numOrbits / $MAX_NUMJOB)) 


# Find the remainder of this division
let "numExtraProcess= $numOrbits % $MAX_NUMJOB"

# Find how many jobs need to be submitted assuming each job is completely filled (MAX_NPJ and MAX_PPN)
let "numJobs= $numOrbits / $(($MAX_NPJ * $MAX_PPN))"

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
        numProcess[$i]=$((MAX_PPN * MAX_NPJ))       # Each job will handle exactly MAX_PPN * MAX_NPJ processes
    done

    let "numExtraProcess = $numOrbits - (MAX_NPJ * MAX_PPN * numJobs)"  # Find the remaining number of jobs to process
    numProcess[$numJobs]=$numExtraProcess
    
    if [ $numExtraProcess -gt 0 ]; then             # If the remaning number of processes is greater than 0.
        numNodes=$((numExtraProcess / MAX_PPN))     # Find the number of nodes the last job needs to have.

        if [ $numNodes -eq 0 ]; then                # If it needs less than one node (assuming MAX_PPN), only request
            PPN[$numJobs]=$numExtraProcess          # exact number of cores that we need, and only one node.
            NPJ[$numJobs]=1
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

for i in $(seq 0 $((numJobs-1)) ); do
    printf '%d PPN: %-5s NPJ: %-5s numProcess: %-5s start: %-5s end: %-5s\n' $i ${PPN[$i]} ${NPJ[$i]} ${numProcess[$i]} ${jobOSTART[$i]} ${jobOEND[$i]}
done


# Time to create the files necessary to submit the jobs to the queue
mkdir -p "$SCHED_PATH/BFprocess"

# Find what the highest numbered "run" directory is and grab the number from it.
# You can run this "bashism" nonsense in the terminal to see what it's doing.
lastNum=$(ls "$SCHED_PATH/BFprocess" | grep run | sort --version-sort | tail -1 | tr -dc '0-9')

if [ ${#lastNum} -eq 0 ]; then
    lastNum=-1
fi

let "lastNum++"

# Make the "run" directory
runDir="$SCHED_PATH/BFprocess/run$lastNum"
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

logDir="$runDir/logs"
mkdir "$logDir"
mkdir "$logDir"/aprun
mkdir "$logDir"/aprun/errors
mkdir "$logDir"/aprun/logs

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
    echo "#PBS -l nodes=${NPJ[$i]}:ppn=${PPN[$i]}:xe"           >> job$i.pbs
    echo "#PBS -l walltime=$WALLTIME"                           >> job$i.pbs
    echo "#PBS -q $QUEUE"                                       >> job$i.pbs
    echo "#PBS -N TerraProcess_${jobOSTART[$i]}_${jobOEND[$i]}" >> job$i.pbs
    echo "cd $runDir"                                           >> job$i.pbs
    echo "source /opt/modules/default/init/bash"                >> job$i.pbs
    echo "module load hdf4 hdf5"                                >> job$i.pbs

    littleN=$(( ${NPJ[$i]} * ${PPN[$i]} ))
    echo "aprun -n $littleN -N ${PPN[$i]} -R 4 $SCHED_PATH/scheduler.x $runDir/processLists/job$i.list /bin/bash -noexit 1> $logDir/aprun/logs/job$i.log 2> $logDir/aprun/errors/$job$i.err" >> job$i.pbs
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
mkdir "$logDir"/basicFusion
mkdir "$logDir"/basicFusion/errors
mkdir "$logDir"/basicFusion/logs

for job in $(seq 0 $((numJobs-1)) ); do
    echo "Writing program calls in: ${jobDir[$job]}"

    for orbit in $(seq ${jobOSTART[$job]} ${jobOEND[$job]} ); do

        # Expand the FILENAME variable so that the $ORBIT value is inserted into the name
        evalFileName=$(eval echo "$FILENAME")

        # BF program call looks like this:
        # ./basicFusion [output HDF5 filename] [input file list] [orbit_info.bin]
        echo "$BF_PROG \"$OUT_PATH/$evalFileName\" \"$LISTING_PATH/input$orbit.txt\" \"$BIN_DIR/orbit_info.bin\" 2> $runDir/logs/basicFusion/errors/$orbit.err 1> $runDir/logs/basicFusion/logs/$orbit.log" > "${jobDir[$job]}/orbit$orbit.sh"

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
