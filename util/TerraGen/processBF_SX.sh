#!/bin/bash
# Author: Landon Clipp
# Email: clipp2@illinois.edu
# Preface:
#       I totally apologize for writing this whole thing in Bash. What began as a simple, small script
#       turned into an unweildy beast. Kids, always avoid writing in Bash if you can. The only thing it's
#       good for is if you need an easy interface for external program calling. I sometimes like to tell
#       myself that I'm justified in using Bash because it's so good at program calling. It's easily
#       one of the most dirty, unelegant, and obnoxious languages you'll ever have the displeasure of
#       dealing with.
#

#============================================================================================
#       findJobPartition
#
# DESCRIPTION:
#       This function takes as input a range of orbits and various maximum job parameters
#       and determines how the orbits will be partitioned among the jobs. The parameters it
#       determines include the number of cores per node, number of nodes per job, and the
#       number of jobs.
#
# ARGUMENTS:
#       Command Line:
#           [Orbit start] [Orbit end] [Max Nodes Per Job] [Max Cores Per Node] [Max num Jobs]
#       Implicit (global variables):
#               jobOSTART       -- Array that will contain starting orbits for each job, where
#                                  each element is a specific job.
#               jobOEND         -- Array that will contain the ending orbits for each job.
#               CPN             -- Array that will contain the cores per node of each job.
#               NPJ             -- Array that will contain the nodes per job of each job.
#               numJobs         -- Integer variable that will contain the amount of jobs
#                                  allocated in the partition.
# EFFECTS:
#       Modifies the Implicit arguments to contain the proper parameters.
#
# RETURN:
#       Implicit arguments.
#
#==============================================================================================

findJobPartition(){

    local OSTART_l=$1; shift
    local OEND_l=$1; shift
    local MAX_NPJ_l=$1; shift
    local MAX_CPN_l=$1; shift
    local MAX_NUMJOB_l=$1; shift

    declare -a numProcess_l 
    local numProcessPerJob_l
    local numOrbits_l
    local numExtraProcess_l
    local numNodes_l
    local accumulator_l
    local jobBrim_l
    local numJobs_l
    numOrbits_l=$(($OEND_l - $OSTART_l + 1))                      # Total number of orbits to generate
    jobBrim_l=$(( MAX_NPJ_l * MAX_CPN_l ))

    numJobs=0

    # First attempt to fill up all jobs to the brim
    for i in $(seq 0 $((MAX_NUMJOB_l-1))); do
        numProcess_l[$i]=0              # Initialize to zero
        CPN[$i]=0
        NPJ[$i]=0
        let "numJobs++"

        # Loop over every node in the job       
        for j in $(seq 0 $((MAX_NPJ_l-1))); do

            # Loop over every core in the job
            for k in $(seq 0 $((MAX_CPN_l-1))); do
                numProcess_l[$i]=$(( ${numProcess_l[$i]} + 1 ))           # Increment this job's number of orbits to process
                let "numOrbits_l--"                                     # Decrement the remaining orbits left

                # Clip the CPN to the maximum value
                if [ ${CPN[$i]} -lt $MAX_CPN_l ]; then
                    CPN[$i]=$(( ${CPN[$i]} + 1 ))
                fi

                if [ $numOrbits_l -le 0 ]; then
                    break 3
                fi
            done
 
        done

        NPJ[$i]=$((j+1))

    done

    NPJ[$i]=$((j+1))

    # Currently, all jobs are set to hold MAX_NPJ * MAX_CPN number of processes.
    # Check if there are more orbits to process. If so, distribute the remaining
    # processes evenly
    if [ $numOrbits_l -gt 0 ]; then
        while true; do
            for i in $(seq 0 $numJobs); do
                if [ $numOrbits_l -le 0 ]; then
                    break 2
                fi

                numProcess_l[$i]=$(( ${numProcess_l[$i]} + 1 ))
                let "numOrbits_l--"
            done
        done
    fi 

    # For each job, figure out the range of orbits it will be responsible for
    accumulator_l=$OSTART_l


    # Set the starting and ending orbits for each job
    for i in $(seq 0 $((numJobs-1)) ); do
        jobOSTART[$i]=$accumulator_l
        # Orbit end = orbit start + numOrbitsForJob - 1
        jobOEND[$i]=$((${jobOSTART[$i]} + ${numProcess_l[$i]} - 1))
        accumulator_l=$((${jobOEND[$i]} + 1))
    done
}

#=======================================================================================
#       findOrbitStart()
# DESCRIPTION:
#       This function finds the starting date and time of orbit $1 as determined by the
#       Orbit_Info.txt file.
#
# ARGUMENTS:
#       $1      -- The orbit to find the starting time of
#       $2      -- The Orbit_Info.txt file.
#
# EFFECTS:
#       None
#
# RETURN:
#       Modifies the orbit_start global variable.
#=========================================================================================

findOrbitStart(){

    local orbit_l=$1; shift
    local ORBIT_INFO_l="$1"; shift

    # Grab the orbit start time from ORBIT_INFO
    orbit_start="$(grep -w "^$orbit_l" "$ORBIT_INFO_l" | cut -f3 -d" ")"
    # Get rid of the '-' characters
    orbit_start="${orbit_start//-/}"
    # Get rid of the 'T' characters
    orbit_start="${orbit_start//T/}"
    # Get rid of the ':' characters
    orbit_start="${orbit_start//:/}"
    # Get rid of the 'Z'
    orbit_start="${orbit_start//Z/}"
}

#========================================================================================================
#       initAndSubmit_GNU()
#
# DESCRIPTION:
#       This function creates all of the files necessary to run the BasicFusion program in parallel for
#       every orbit between the starting and ending orbits. It uses the GNU parallel program to handle
#       the task distribution on the compute nodes.
#
#=========================================================================================================
initAndSubmit_GNU(){
    

    local orbit_start
    
    local logDir="$runDir/logs"
    mkdir "$logDir"
    mkdir "$logDir"/basicFusion
    mkdir "$logDir"/GNU_parallel

    # Copy the basicFusion binary to the job directory. Use this copy of the bianry.
    cp $BF_PROG $runDir 
    newBF_PROG="$runDir"/$(basename $BF_PROG)

    # Save the current date into the run directory
    date > "$runDir/date.txt"

    ########################
    #   MAKE PBS SCRIPTS   #
    ########################
     
    cd $runDir
    mkdir -p PBSscripts
    cd PBSscripts
    echo "Generating scripts for parallel execution in: $(pwd)"
    
    for i in $(seq 0 $((numJobs-1)) ); do

        echo "Making scripts for job ${i}..."

        local CMD_FILE="$runDir/parallel_BF_CMD_job_${i}.txt"
        rm -f "$CMD_FILE"

        # GNU parallel requires a text file where each line is a command-line call to process. It distributes each of these
        # command line calls amongst the nodes it governs. Generating this file is easy, just create the appropriate arguments
        # to the basicFusion program for each orbit, one orbit per line.

        
        evalFileName=$(eval echo $FILENAME)
        for orbit in $(seq ${jobOSTART[$i]} ${jobOEND[$i]} ); do
            
            orbit_start=""
            # Find the orbit start time
            findOrbitStart $orbit $ORBIT_INFO_TXT

            export orbit=$orbit

            # Evaluate the filename. Filename relies on the variables "orbit" and "orbit_start" being set to their appropriate values.
            evalFileName=$(eval echo "$FILENAME")
            echo "$newBF_PROG \"$OUT_PATH/$evalFileName\" $LISTING_PATH/input${orbit}.txt $ORBIT_INFO_BIN &> $logDir/basicFusion/log${orbit}.txt" >> "$CMD_FILE"
        
        done
 
        queueLine=""
        if [ ${#QUEUE} -ne 0 ]; then
            queueLine="#PBS -q $QUEUE"
        fi
        pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=${NPJ[$i]}:ppn=${CPN[$i]}${NODE_TYPE}
#PBS -l walltime=$WALLTIME
$queueLine
#PBS -N TerraProcess_${jobOSTART[$i]}_${jobOEND[$i]}
export USE_GZIP=${USE_GZIP}
export USE_CHUNK=${USE_CHUNK}
cd $runDir
source /opt/modules/default/init/bash
module load hdf4 hdf5 parallel
export PARALLEL=\"--env PATH --env LD_LIBRARY_PATH --env LOADEDMODULES --env _LMFILES_ --env MODULE_VERSION --env MODULEPATH --env MODULEVERSION_STACK --env MODULESHOME\"
parallel --jobs \$PBS_NUM_PPN --sshloginfile \$PBS_NODEFILE --workdir $runDir < \"$CMD_FILE\" &> $runDir/logs/GNU_parallel/log${i}.txt
exit 0
"

        echo "$pbsFile" > job$i.pbs


        echo "Done."
        echo

        ###################
        #    CALL QSUB    #
        ###################

        echo "Submitting $runDir/PBSscripts/job${i}.pbs to queue..."
        curDir="$(pwd)"
        cd $runDir/PBSscripts
        qsub job${i}.pbs 
        # cd back into the curDir directory
        cd "$curDir"
        echo "Done."
        echo

    done # DONE with for job in....
}

initAndSubmit_sched(){

    local logDir="$runDir/logs"
    mkdir "$logDir"
    mkdir "$runDir"/PBSscripts

    for i in $(seq 0 $((numJobs-1)) ); do

        # Make the job directories inside each run directory
        mkdir -p "${runDir}/programCalls"
        for i in $(seq 0 $((numJobs-1)) ); do
            jobDir[$i]="${runDir}/programCalls/job$i"
            mkdir -p "${jobDir[$i]}"                        # Makes a directory for each job
        done
        
        # littleN (-n) tells us how many total processes in the job.
        # 
        littleN=$(( ${NPJ[$i]} * ${CPN[$i]} ))

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

        queueLine=""
        if [ ${#QUEUE} -ne 0 ]; then
            queueLine="#PBS -q $QUEUE"
        fi
        pbsFile="\
#!/bin/bash
#PBS -j oe
#PBS -l nodes=${NPJ[$i]}:ppn=${CPN[$i]}${NODE_TYPE}
#PBS -l walltime=$WALLTIME
$queueLine
#PBS -N TerraProcess_${jobOSTART[$i]}_${jobOEND[$i]}
export PMI_NO_PREINITIALIZE=1
export USE_GZIP=${USE_GZIP}
export USE_CHUNK=${USE_CHUNK}
export PMI_NO_FORK=1
cd $runDir
source /opt/modules/default/init/bash
module load hdf4 hdf5
$loadMPICH
$mpiCall time $SCHED_PATH/scheduler.x $runDir/processLists/job$i.list /bin/bash -noexit 1> $logDir/$mpiProg/logs/job$i.log 2> $logDir/$mpiProg/errors/$job$i.err
exit 0
"
        cd $runDir/PBSscripts
        echo "$pbsFile" > job${i}.pbs

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
           
            orbit_start=""
            findOrbitStart $orbit $ORBIT_INFO_TXT             
            evalFileName=$(eval echo $FILENAME)
            outGranule="$OUT_PATH/$evalFileName"

            # BF program call looks like this:
            # ./basicFusion [output HDF5 filename] [input file list] [orbit_info.bin]
            programCalls="\
\"$BFfullPath\" \"$outGranule\" \"$LISTING_PATH/input${orbit}.txt\" \"${ORBIT_INFO_BIN}\" 2> \"$logPath/errors/${orbit}.err\" 1> \"$logPath/logs/${orbit}.log\"

# We will now save the MD5 checksum.
mkdir -p \"$runDir/checksum\"
md5sum \"$OUT_PATH/$evalFileName\" > \"$runDir/checksum/${orbit}.md5\"
"

            echo "$programCalls" > "${jobDir[$job]}/orbit${orbit}.sh"

            chmod +x "${jobDir[$job]}/orbit${orbit}.sh"
     
        done # DONE with for orbit in....

        ###################
        #    CALL QSUB    #
        ###################

        echo "Submitting $runDir/PBSscripts/job${job}.pbs to queue."
        curDir="$(pwd)"
        cd $runDir/PBSscripts
        qsub job${job}.pbs 
        # cd back into the curDir directory
        cd "$curDir"
        echo

    done # DONE with for job in....
}

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
\t                                --GZIP [1-9]      Sets the HDF compression level. Default 0.
\t                                --CHUNK           This enables HDF chunking. Default OFF.
\t                                --GNU             Use GNU Parallel instead of NCSA Scheduler

"   
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ $# -lt 4 -o $# -gt 7 ]; then
    usage
    exit 1
fi

#____________JOB PARAMETERS____________#
MAX_NPJ=5                                               # Maximum number of nodes per job
MAX_CPN=16                                              # Maximum number of processors (cores) per node.
MAX_NUMJOB=5                                            # Maximum number of jobs that can be submitted simultaneously
WALLTIME="06:00:00"                                     # Requested wall clock time for the jobs
QUEUE=""                                                # Which queue to put the jobs in. Leave blank for system default.
NODE_TYPE=""
#--------------------------------------#

#____________FILE NAMING_______________#

FILENAME='TERRA_BF_L1B_O${orbit}_${orbit_start}_F000_V000.h5'          # The variables "orbit" and "orbit_start" will be expanded later on.

#--------------------------------------#

LISTING_PATH=$1

# ASSERT THAT LISTING_PATH IS A VALID DIRECTORY
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
USE_GNU=0
intREG='^[0-9]+$'       # An integer regular expression


    ########################
    # OPT ARGUMENT PARSING #
    ########################

for i in $(seq 1 "$#"); do
    if [ "$1" == "--GZIP" ]; then
        shift
        USE_GZIP=$1

        # Assert that USE_GZIP is an integer
        if ! [[ $USE_GZIP =~ $intREG ]]; then
            echo "An integer between 1-9 must follow the --GZIP option!" >&2
            exit 1
        fi

        # Assert that USE_GZIP is between 1 through 9
        if [ $USE_GZIP -lt 1 -o $USE_GZIP -gt 9 ]; then

            echo "An integer between 1-9 must follow the --GZIP option!" >&2
            exit 1
        fi

        echo "___HDF GZIP LEVEL ${USE_GZIP}___"

    elif [ "$1" == "--CHUNK" ]; then
        USE_CHUNK=1
        echo "___HDF CHUNKING ENABLED___"
    elif [ "$1" == "--GNU" ]; then
        USE_GNU=1
        echo "___USING GNU PARALLEL___"
    else
        echo "Error: Illegal parameter: $1" >&2
        exit 1
    fi

    shift
done

if [ $USE_GZIP -eq 0 ]; then
    echo "___HDF GZIP DISABLED___"
fi
if [ $USE_CHUNK -eq 0 ]; then
    echo "___HDF CHUNKING DISABLED___"
fi
if [ $USE_GNU -eq 0 ]; then
    echo "___USING NCSA SCHEDULER___"
fi


#####################################################################################################################################
#                               DIRECTORIES AND PATHS 


ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"    # Get the absolute path of this script
LISTING_PATH="$(cd $(dirname $LISTING_PATH) && pwd)/$(basename $LISTING_PATH)"  # Make the LISTING_PATH an absolute path
BIN_DIR="$ABS_PATH/../../bin"                                   # Get the absolute path of the basic fusion binary directory
BIN_DIR="$(cd $(dirname $BIN_DIR) && pwd)/$(basename $BIN_DIR)"
BF_PROG="$BIN_DIR/basicFusion"                                  # Get the absolute path of the basicFusion program
BF_PATH="$(cd "$BIN_DIR/../" && pwd)"                           # Get the path of BF repository
SCHED_PATH="$BF_PATH/externLib/Scheduler"                       # Get the path of the Scheduler repository
ORBIT_INFO_BIN="$BF_PATH/metadata-input/data/Orbit_Path_Time.bin"   # Get the path of the Orbit_info file
ORBIT_INFO_TXT="$BF_PATH/metadata-input/data/Orbit_Path_Time.txt"   # Get the .txt version of the file     
hn="$(hostname)"                                                # host name of this machine

#####################################################################################################################################


# Check that Scheduler has already been downloaded and compiled
if [ ! -x "$SCHED_PATH" ]; then
    echo "Fatal error: Scheduler has not been downloaded/compiled or the Fortran executable has not been flagged for execution. Please run the configureEnv.sh script in the basicFusion/util directory." >&2
    exit 1
fi

# Check that the ORBIT_INFO_BIN file exists
if [ ! -e "$ORBIT_INFO_BIN" ]; then
    echo "Fatal error: The $ORBIT_INFO_BIN file does not exist. Please refer to the GitHub wiki on how to generate this file." >&2
    exit 1
fi


# Check that the ORBIT_INFO_TXT file exists
if [ ! -e "$ORBIT_INFO_TXT" ]; then
    echo "Fatal error: The $ORBIT_INFO_TXT file does not exist. Please refer to the GitHub wiki on how to generate this file." >&2
    exit 1
fi

# Check that the basicFusion program has been compiled
if [ ! -e "$BF_PROG" ]; then
    echo "Fatal error: The Basic Fusion program was not found at ${BF_PROG}. Please compile and try again." >&2
    exit 1
fi

###########################
# FIND THE JOB PARTITIONS #
###########################

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

echo "JOB PARAMETERS"

for i in $(seq 0 $((numJobs-1)) ); do
    printf '%d CPN: %-5s NPJ: %-5s start: %-5s end: %-5s\n' $i ${CPN[$i]} ${NPJ[$i]} ${jobOSTART[$i]} ${jobOEND[$i]}
done
echo

# ================================================ #
# Create the directories to hold all the job files #
# ================================================ #

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
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the run directory!"
    exit 1
fi

mkdir "$runDir"/checksum
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed to make the checksum directory!"
    exit 1
fi


# Save the current date into the run directory
date > "$runDir/date.txt"


# ========================================================== #
# Create all of the necessary files for parallel computation #
# ========================================================== #

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

# If we're not on ROGER, assume that we are on Blue Waters and set the
# node type to xe
if ! [ "$hn" == "cg-gpu01" ]; then
    NODE_TYPE=":xe"
fi

if [ $USE_GNU -eq 0 ]; then
    initAndSubmit_sched
else
    initAndSubmit_GNU
fi
    
echo
exit 0
