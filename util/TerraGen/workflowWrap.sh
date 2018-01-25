#!/bin/bash

orbit_start=1000
orbit_end=42750

# Nearline Globus ID
remote_id=$GLOBUS_NL

# Nearline tar directory
remote_tar_dir='/projects/sciteam/bage/PerOrbitArchive/'

# Nearline output BasicFusion directory
remote_bf_dir='/projects/sciteam/bage/BFoutput/'

# BlueWaters Globus ID
host_id=$GLOBUS_BW

# User scratch space. Warning!!! It is recommended you:
# mkdir ${HOME}/scratch/bf_scratch
# and hand this directory to the workflow! Do NOT
# simply pass in ${HOME}/scratch/
scratch_space=

# Directory where the MISR path files (HRLL, AGP) have been untarred.
misr_path=

# Not used
#hash_dir="${HOME}/hash/"

# Submit this number of simultaneous Globus transfer requests.
# By default, Globus imposes a maximum of 3.
globus_parallelism=3

# Number of nodes to request
nodes=10
# Processors per node (default is 32)
ppn=32

# Specify how many orbits each PBS job will be reponsible for.
# e.g. for the case orbit_start=1000, orbit_end=1999:
# job_granularity=1000 will submit only one batch of jobs, whereas
# job_granularity=500 will submit two batches of jobs
job_granularity=50000

# Specify the location of the missing input granules. Each subdirectory
# in $modis_missing is numbered after the orbit. Each file within the orbit
# subdirectory will be included for processing the BasicFusion granule
modis_missing="/scratch/sciteam/clipp/missing"

# Specify the PBS queue to use. 
queue="--queue normal"

bwpy-environ -- python submitWorkflow.py -p $globus_parallelism -g $job_granularity --include-missing $modis_missing -l DEBUG $queue $orbit_start $orbit_end $remote_id $remote_tar_dir $remote_bf_dir $host_id $scratch_space $misr_path $nodes $ppn
