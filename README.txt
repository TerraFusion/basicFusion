# basicFusion
Terra Data Fusion Project - University of Illinois  
Author: Landon Clipp  

This file outlines the structure of the combined TERRA Fusion code, how to compile it on a local machine, and how to add
additional code to the program. This file may not be up to date during development.

THE MASTER BRANCH OF THIS REPOSITORY REPRESENTS CURRENT WORKING CODE (COMPILABLE AND RUNNABLE ON BW)

The code is written in C using the following HDF libraries:  
1. hdf.h -- For the HDF4 functions  
2. hdf5.h -- For the HDF5 functions  
3. mfhdf.h -- For the HDF4 scientific dataset (SD) interface. Also acts as a wrapper for the hdf.h library (includes hdf by
  default).  

***********************
***PROGRAM STRUCTURE***
***********************

This program will contain 5 basic segments. Each segment will handle reading the datasets from one single instrument and
writing the data into the output HDF5 file. Currently, the MOPITT portion of the code handles creating the new HDF5 output
file and a program-wide global variable has been declared "hid_t outputFile" that contains the file identifier required by
HDF5 functions to handle writing information into the file. This is an HDF5 only identifier, it is not valid for HDF4.

Each segment of the code is to be written as if it is its own standalone program. It will have a main function (named by the
convention of MOPITT(), CERES(), etc ) where all of the appropriate function calling relevant for that instrument will
go. Any functions that could conceivably be reused across the instruments should be declared in the "libTERRA.h" and 
defined in the "libTERRA.c" files. This allows code reusability so that a function is available to any segment of the 
code if need be.

Every instrument's function should receive the normal main arguments, int argc and char* argv[] (aka char** argv). The
main function (main.c) will pass in the appropriate arguments to each instrument function.

*********************
* COMPILING/RUNNING *
*********************

A sample Makefile has been provided. Do NOT edit this Makefile on the Github repository. You can download it to your machine
and edit it for your own machine, but do not reupload the edited version.

---LOCAL MACHINE---

In order to compile this program, there are either one of two ways you can do so. First, you can either compile it using
the native gcc compiler using the appropriate include paths for your HDF libraries, or you can install the gcc wrappers
for HDF called h4cc (for HDF4) and h5cc (for HDF5). The latter is recommended because the Makefile assumes you've done 
so. The HDF wrappers simply call gcc using the appropriate include paths for your header files. These wrappers will
automatically know the path of your include file when you compile the HDF libraries. Instructions for installing the HDF
libraries (and by extension the h4cc/h5cc compilers) can be provided if desired.

Once you have installed h4cc and h5cc and confirmed that they are visible to your command line interpreter, you need to edit
your Makefile so that the compiler and linker are appropriately pointed to the correct inlcude and library files (using
the -I and -L arguments respectively). NOTE that h5cc only points to HDF5 libraries by default, and h4cc only points to 
HDF4 libraries.

The HDF5 (or HDF4) libraries might want access to JPEG libraries, so if you get 
errors about JPEG libraries, try updating or installing those.

---BLUE WATERS---

Steps to compile and run on Blue Waters:
Directory: /projects/sciteam/jq0/TerraFusion/basicFusion
    
                    BATCH SCRIPT
A script has been provided to make compiling and running the program easy. In the basicFusion directory, the script called
"jobSubmit" in the bin directory contains all of the necessary commands to run the program. All you need to do is to run
this script. Here are the steps the script takes (you can also do these steps manually):

Step 1: Load modules
  Loads the following modules:
    a. Intel programming environment
    b. szip library
    c. HDF5 library
    d. HDF4 library
Step 2: Compile the program (Makefile is located in the root project directory).
  Step 1 must be completed before step 2. The Makefile uses the default BW HDF4 and HDF5 libraries.
Step 3: Generates the inputFiles.txt file using the generateInput.sh script
Step 4: Submits the executable to the BW job queue (qsub)

Check the status of the job by executing the command "qstat | grep [your BW username]". A flag of "Q" means the job
is enqueued. A flag of "R" means the job is currently executing. A flag of "C" means the job has completed. 
     
     
                    INTERACTIVE
The steps for interactive mode are exactly the same as the batch script except for step 4, except you will need to enter
all the commands manually. To run the program in interactive mode, follow these steps:
Step 1: Load modules
  Enter into the bin directory and enter exactly: . ./loadModules
Step 2: Compile
  Enter into the root program directory and enter: make
Step 3:
  Generate the inputFiles.txt file using the generateInput.sh script located in bin.
  Give the script a path to the 5 instruments.
Step 4:
  Enter interactive mode (if not already):
    qsub -I -l nodes=1 -l walltime=03:00:00
  You may enter any value in for walltime. This is the time you will be alloted in interactive mode. It is given
  by HH:MM:SS.
Step 5:
  Run the following command:
  aprun -n 1 /projects/sciteam/jq0/TerraFusion/basicFusion/exe/TERRArepackage out.h5 /projects/sciteam/jq0/TerraFusion/basicFusion/exe/inputFiles.txt &> /projects/sciteam/jq0/TerraFusion/basicFusion/jobOutput/output.txt
  
  Explanation of the aprun command argument by argument:
  -n [number]: number of nodes requested
  /projects/sciteam...: path to executable program
  out.h5: argument to the executable program of what the output file will be called
  /projects/sciteam...: Argument to executable of the inputFiles.txt file
  &> /projects...: Send all program output to this text file
  
The program is now being executed. Use qstat to check the status.
  
 

*****************
***ADDING CODE***
*****************

I have yet to decide how I want to handle multiple collaborators on this project. We will discuss this more in the technical
meeting.

*****************
***FLOW CHARTS***
*****************

High level API flow chart

MOPITT--------------------------------------------------
                                                       |
MISR-------------->-                                   |
                   |                                   |
MODIS-----------hdf.h--->hdf5.h-->convrt to hdf5----->hdf5.h-----> output file
                   ^
CERES----------->---
                   ^
ASTER----------->---                 

PROGRAM STRUCTURE:
Note: Directionality indicates dependency. For instance, main.c is dependent on fileList.txt and outputFileName.
      MOPITTmain.c would be dependent on main.c, MOPITTfiles and libTERRA.c etc.

                                         --------->------MOPITTmain.c------->------|             
                                         |             ^^^MOPITTfiles^^^           | 
                                         |             ^^^libTERRA.c ^^^           |
                                         |-------->-------CERESmain.c------->------|   
                                         |             ^^^CERESfiles^^^            |   
                                         |             ^^^libTERRA.c^^^            |  
      fileList.txt-->main.c-------->-----|-------->-------MODISmain.c------->------|---->outputFile
    outputFileName----^                  |             ^^^MODISfiles^^^            |  
                                         |             ^^^libTERRA.c^^^            |   
                                         |-------->-------ASTERmain.c------->------|   
                                         |             ^^^ASTERfiles^^^            |   
                                         |             ^^^libTERRA.c^^^            |
                                         |-------->-------MISRmain.c-------->------|
                                                       ^^^MISRfiles^^^
                                                      ^^^libTERRA.c^^^
                                    
                                    libTERRA.c
                                     ^     ^
                                     |     |
                                     |     |
                                  hdf5.h  mfhdf.h
