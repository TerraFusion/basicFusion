# basicFusion
Terra Data Fusion Project - University of Illinois  
Author: Landon Clipp  
Email: clipp2@illinois.edu  

This file outlines the structure of the combined TERRA Fusion code, how to compile it on a local machine, and how to add
additional code to the program. This file may not be up to date during development.

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
convention of MOPITTmain(), CERESmain(), etc ) where all of the appropriate function calling relevant for that instrument will
go. Any and all helper functions should be declared in the "libTERRA.h" and defined in the "libTERRA.c" files. This allows
code reusability so that a function is available to any segment of the code if need be.

Every instrument's main function should receive the normal main arguments, int argc and char* argv[] (aka char** argv). The
main function should also have a prototype in the libTERRA.h file. Then, a master main function (main.c) will pass in the 
appropriate arguments to the instrument's main functions.

Remember, the MOPITT segment must be run before any other segment because it is responsible for creating the output file
and initializing the outputFile variable for the use of other segments.

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

Once you have installed h4cc and h5cc and confirmed that they are visible to your command line interpreter, the only thing
you need to do change the value of "INCLUDE2" and "LIB1" in the makefile to point to your HDF5 header files and your HDF4 
libraries respectively. For some reason, the HDF5 (or HDF4) libraries might want access to JPEG libraries, so if you get 
errors about JPEG libraries, try updating or installing those.

---BLUE WATERS---

Steps to compile and run on Blue Waters:
Directory: /projects/sciteam/jq0/TerraFusion/basicFusion/combinedCode
    
                    BATCH SCRIPT
Step 1: Compile.
  1. Manually enter the module lines in the file "combinedCode/loadModules" into your temrinal. Currently, this file is set up as a batch script but it does not work for some reason. So just enter the lines manually. Copy/pasting the lines works as well.
  2. Perform a "make clean" in the combinedCode directory.
  3. "make" the program.
Step 2: Run.
  1. Run the script "./jobSubmit". This executes the qsub command to place the batchscript.pbs script into the Blue Waters
     queue. qsub is a queueing command to request access to a compute node. Once the compute node has been granted,
     batchscript.pbs will be executed to run the program "TERRArepackage".
  2. Check the status of the job by executing the command "qstat | grep [your BW username]". A flag of "Q" means the job
     is enqueued. A flag of "C" means the job has completed.
     
     
                    INTERACTIVE
Coming soon...
     
NOTE:
  Currently there is no error output from the program when running a batch script. I am working on fixing this
  so that the useful diagnostic information the program provides can be accessible.

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
