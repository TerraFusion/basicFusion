This directory contains various scripts that are useful for submitting the BasicFusion program in parallel. 

#=======================

processBF_SX.sh

    This script takes as input a starting and ending orbit (among other parameters) and automatically submits the job to the compute nodes of your host machine. The steps it takes to achieve this are:

    1. Using the starting/ending orbit, determine how to partition the entire task amongst all of the jobs, nodes, and cores available to us. The number of jobs, nodes, and cores are determined by internal variables set to default or user-determined values. These variables place an upper bound on how the amount of compute resources this script will request of the TORQUE resource manager.

    2. Once the task partitioning has been determined, begin writing all of the necessary scripts and files for submission to the queue. This includes writing the proper PBS scripts, writing all of the explicit calls to the Basic Fusion program (one for each orbit), and generally meeting the interface requirements for the "batch of jobs" program (this "batch of jobs" program is either NCSA Scheduler or GNU Parallel).

