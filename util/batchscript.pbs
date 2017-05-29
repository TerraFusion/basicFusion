#!/bin/bash

# DANGER!!!!! the "FILELISTNAME" variable MUST remain on the 4th line for the jobSubmit script!!!
FILELISTNAME=inputFiles.txt

### set the number of nodes
### set the number of PEs per node
#PBS -l nodes=1:ppn=1:xe
### set the wallclock time
#PBS -l walltime=02:00:00
### set the job name
#PBS -N TERRAFusion
### set the job stdout and stderr
##PBS -e $PBS_JOBID.err
##PBS -o $PBS_JOBID.out
### set email notification
##PBS -m bea
##PBS -M username@host
### In case of multiple allocations, select which one to charge
##PBS -A xyz
### Set umask so users in my group can read job stdout and stderr files
##PBS -W umask=0027



PROJDIR=~/basicFusion/
EXENAME=basicFusion
OUTFILENAME=$PBS_JOBID.h5
STD_OUTFILENAME=$PBS_JOBID.out.txt

##HLIBPATH=/projects/sciteam/jq0/TerraFusion/basicFusion/hdflib/lib
##if [[ $LD_LIBRARY_PATH != *"hdflib/lib"* ]]; then
##    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH":"$HLIBPATH"
##fi
### COMMENT OUT THIS SETTING BEFORE FULL OPERATION OF PROGRAM!!!
### A COMMENT IS TWO POUND SYMBOLS
### Set the job to be submitted to the debug queue
#PBS -q high

# NOTE: lines that begin with "#PBS" are not interpreted by the shell but ARE
# used by the batch system, wheras lines that begin with multiple # signs,
# like "##PBS" are considered "commented out" by the batch system
# and have no effect.

# If you launched the job in a directory prepared for the job to run within,
# you'll want to cd to that directory
# [uncomment the following line to enable this]
cd $PBS_O_WORKDIR

# Alternatively, the job script can create its own job-ID-unique directory
# to run within.  In that case you'll need to create and populate that
# directory with executables and perhaps inputs
# [uncomment and customize the following lines to enable this behavior]
# mkdir -p /scratch/sciteam/$USER/$PBS_JOBID
# cd /scratch/sciteam/$USER/$PBS_JOBID
# cp /scratch/job/setup/directory/* .

# To add certain modules that you do not have added via ~/.modules
. /opt/modules/default/init/bash # NEEDED to add module commands to shell
module swap PrgEnv-cray PrgEnv-intel
module load szip/2.1
module load cray-hdf5/1.8.16 
module load hdf4/4.2.10
# export APRUN_XFER_LIMITS=1  # to transfer shell limits to the executable

### launch the application
### redirecting stdin and stdout if needed
### NOTE: (the "in" file must exist for input)

aprun -n 1 "$PROJDIR"/exe/"$EXENAME" "$PROJDIR"/jobOutput/"$OUTFILENAME" "$PROJDIR"/exe/"$FILELISTNAME" "$PROJDIR"/exe/orbit_info.bin &> "$PROJDIR"/jobOutput/"$STD_OUTFILENAME"


### For more information see the man page for aprun
#module load valgrind/3.12.0
#aprun -n 1 valgrind --leak-check=full --track-origins=yes --show-reachable=yes "$PROJDIR"/exe/"$EXENAME" "$PROJDIR"/jobOutput/"$OUTFILENAME" "$PROJDIR"/exe/"$FILELISTNAME" "$PROJDIR"/exe/orbit_info.bin &> "$PROJDIR"/jobOutput/"$STD_OUTFILENAME"
