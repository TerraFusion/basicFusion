#!/bin/bash


OSTART=$1
OEND=$2
NUMJOBS=$3
OUTPUTDIR="/projects/TDataFus/fusionTesting/output/2013"
INPUTDIR="/projects/TDataFus/fusionTesting/input/2013"
CMD_PATH="/projects/TDataFus/TerraGen/commandFiles/BF"
BF_PROJ_PATH="/home/clipp/basicFusion"
PBSpath="../pbsScripts/BF"
cmdFileName='parallel_BF_CMD_${jobStart}_${jobEnd}.txt' # cmdFileName must be enclosed in single quotes
WALLTIME="48:00:00"
CURDIR=$(pwd)
if [ $# -ne 5 ]; then
    echo "Usage: $0 [orbit start] [orbit end] [num jobs to execute] [nodes/Job] [processors/node]"
    exit 1
fi

NPJ=$4
PPN=$5

# Check that the arguments are all integers, and that OEND > OSTART

intRegex='^[0-9]+$'
if ! [[ $1 =~ $intRegex ]]; then
    echo "Invalid arguments: All arguments must be integers!"
    exit 1
fi
if ! [[ $2 =~ $intRegex ]]; then
    echo "Invalid arguments: All arguments must be integers!"
    exit 1
fi
if ! [[ $3 =~ $intRegex ]]; then
    echo "Invalid arguments: All arguments must be integers!"
    exit 1
fi
if [ $2 -lt $1 ]; then
    echo "Invalid arguments: Orbit end must be greater than or equal to orbit start!"
    exit 1
fi

numOrbits=$(($OEND - $OSTART + 1))

# Find how many processes each job needs to handle
let "numProcessPerJob= $numOrbits / $NUMJOBS"

if [ $numProcessPerJob -eq 0 ]; then
    echo "Fatal error: you have more jobs requested than you have orbits!"
    exit 1
fi

# Find the quotient remainder (the modulus) of this number.
# This number will be added onto the last job.
numExtraProcess=$((numOrbits % NUMJOBS))
for i in $(seq 0 $((NUMJOBS-1)) ); do
    # Assign the number of processes to each job
    numProcess[$i]=$numProcessPerJob
done

# Add the remainder number of processes onto the last job
let "numProcess[$((NUMJOBS-1))] += $numExtraProcess"

# As of June 15, 2017, we are assuming that the input.txt files are already generated. This may change.
# Create the commands file for each job

jobStart=$OSTART
for i in $(seq 0 $((NUMJOBS-1)) ); do
    let "jobEnd = $jobStart + ( ${numProcess[$i]} - 1 )"

    # Save the name of the command file in an array
    cmdName[$i]=$(eval echo "$cmdFileName")

    ./genBFcommands.sh $jobStart $jobEnd "$INPUTDIR" "$CMD_PATH" "$BF_PROJ_PATH" "$OUTPUTDIR"
    if [ $? -ne 0 ]; then
        echo "Failed to generate basic fusion commands.\n"
        exit 1
    fi

    # Write the corresponding PBS script
    PBSname[$i]="BF_Exec_${jobStart}_${jobEnd}"
    rm -f "$PBSpath"/"${PBSname[$i]}.pbs"

    # Echo PBS name into pbs file
    echo "#PBS -N ${PBSname[$i]}" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    # Echo the resource request
    echo "#PBS -l nodes=$NPJ:ppn=$PPN" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    # Echo the wall time
    echo "#PBS -l walltime=$WALLTIME" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    echo "#PBS -j oe" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    
    # Insert the rest of the comands
    echo 'cd $PBS_O_WORKDIR' >> "$PBSpath"/"${PBSname[$i]}.pbs"
    echo "module load parallel" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    echo "export PARALLEL=\"--env PATH --env LD_LIBRARY_PATH --env LOADEDMODULES --env _LMFILES_ --env MODULE_VERSION --env MODULEPATH --env MODULEVERSION_STACK --env MODULESHOME\"" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    
    # Get the absolute path of the PBS scripts
    PBSabs=$(cd "$PBSpath" && pwd)
    
    # Execute GNU parallel
    echo "parallel --jobs \$PBS_NUM_PPN --sshloginfile \$PBS_NODEFILE --workdir \$PBS_O_WORKDIR < $CMD_PATH/${cmdName[$i]}" >> "$PBSpath"/"${PBSname[$i]}.pbs"
    
    echo "exit 0" >> "$PBSpath"/"${PBSname[$i]}.pbs"
 
    let "jobStart += ${numProcess[$i]}"

    # Submit the PBS script to the queue
    qsub "$PBSabs/${PBSname[$i]}.pbs"
done

