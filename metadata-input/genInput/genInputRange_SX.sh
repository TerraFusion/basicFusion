#!/bin/bash

if [ $# -ne 4 ]; then
    printf "\nUSAGE: $0 [SQLite database filepath] [orbit start] [orbit end] [output path]\n" >&2
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

\tLog files for the job are stored in the basicFusion/jobFiles/BFinputGen directory."   
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
    exit 1
fi

DB_PATH=$1
OSTART=$2
OEND=$3
OUT_PATH=$4                                             # Where the resultant output HDF files will go
OUT_PATH=$(cd "$OUT_PATH" && pwd)

#____________JOB PARAMETERS____________#
MAX_NPJ=32                                              # Maximum number of nodes per job
MAX_PPN=16                                              # Maximum number of processors (cores) per node.
MAX_NUMJOB=30                                           # Maximum number of jobs that can be submitted simultaneously
WALLTIME="06:00:00"                                     # Requested wall clock time for the jobs
QUEUE="normal"                                            # Which queue to put the jobs in
#--------------------------------------#

# Make the DB path absolute
DB_PATH="$(cd $(dirname $DB_PATH) && pwd)/$(basename $DB_PATH)"

if [ ! -f "$DB_PATH" ]; then
    echo "The database at:"
    echo "$DB_PATH"
    echo "does not exist."
    exit 1
fi

# Get the absolute path of this script
ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Make the LISTING_PATH an absolute path
LISTING_PATH="$(cd $(dirname $LISTING_PATH) && pwd)/$(basename $LISTING_PATH)"

# Get the absolute path of the basic fusion binary directory
BIN_DIR="$ABS_PATH/../../bin"
BIN_DIR="$(cd $(dirname $BIN_DIR) && pwd)/$(basename $BIN_DIR)"

# Get the path of BF repository
BF_PATH="$(cd "$BIN_DIR/../" && pwd)"

# Get the path of the Scheduler repository
SCHED_PATH="$BF_PATH/externLib/Scheduler"
echo "$SCHED_PATH"
# Check that Scheduler has already been downloaded
if [ ! -d "$SCHED_PATH" ]; then
    echo "Fatal error: Scheduler has not been downloaded. Please run the configureEnv.sh script in the basicFusion/util directory." >&2
    exit 1
fi

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
    printf '%d PPN: %-5s NPJ: %-5s numProcess: %-5s start: %-5s end: %-5s\n' $i ${PPN[$i]} ${NPJ[$i]} ${numProcess[$i]} ${jobOSTART[$i]} ${jobOEND[$i]}
done


# Time to create the files necessary to submit the jobs to the queue
BF_PROCESS="$BF_PATH/jobFiles/BFinputGen"
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
    echo "#PBS -N TerraInput_${jobOSTART[$i]}_${jobOEND[$i]}"   >> job$i.pbs
    # The PMI environment variables are added as a workaround to a known Application Level Placement
    # Scheduler (ALPS) bug on Blue Waters. Not entirely sure what they do but BW staff told me
    # that it works, and it does!
    echo "export PMI_NO_FORK=1"                                 >> job$i.pbs
    echo "export PMI_NO_PREINITIALIZE=1"                        >> job$i.pbs
    echo "cd $runDir"                                           >> job$i.pbs
    echo "source /opt/modules/default/init/bash"                >> job$i.pbs
    echo "module load hdf4 hdf5"                                >> job$i.pbs

    littleN=$(( ${NPJ[$i]} * ${PPN[$i]} ))
    bigR=4
    if [ $bigR -ge ${PPN[$i]} ]; then
        bigR=0      # Done in the cases where small number of nodes used. -R tells how many node failures are
                    # acceptable
    fi
    echo "aprun -n $littleN -N ${PPN[$i]} -R $bigR $SCHED_PATH/scheduler.x $runDir/processLists/job$i.list /bin/bash -noexit 1> $logDir/aprun/logs/job$i.log 2> $logDir/aprun/errors/$job$i.err" >> job$i.pbs
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
        echo "$ABS_PATH/genFusionInput.sh $DB_PATH $orbit $OUT_PATH/input$orbit.txt 2> $runDir/logs/genFusionInput/errors/$orbit.err 1> $runDir/logs/genFusionInput/logs/$orbit.log" > "${jobDir[$job]}/orbit$orbit.sh"

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
