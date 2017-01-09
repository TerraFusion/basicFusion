sicFusion
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

     DOWNLOADING AND SETTING UP PROGRAM
Download:
1. Change into the desired directory and type "git clone https://github.com/TerraFusion/basicFusion"

2. cd into the basicFusion directory

Setup:
3. cd into bin

4. Open jobSubmit with your favorite text editor (e.g. "vim jobSubmit")

5. Change the directory of the "inputFiles" variable at the top of the page to a directory containing
   the 5 TERRA instrment directories (which themselves contain valid HDF files). The directory you give
   MUST contain all of the subdirectories: MOPITT, CERES, MODIS, MISR, and ASTER. Also, it must be an absolute
   path.

6. Change the "PROJDIR" variable to point to your basicFusion directory. Must be an absolute path.

7. Save this file and close it.

8. Open up the generateInput.sh script

9. Change the OUTFILE variable to point to where you want the generated HDF file list to be stored.
   Recommended you put it in the basicFusion/exe directory.
   This must be a path to a NAMED .txt file, not a path to a directory (the txt file need not exist beforehand).

10. Save this file and close it.

11. Open up the batchscript.pbs script

12. Change the EXE variable to point to where you want the compiled, executable file to be stored.
    It's recommended you put it in the basicFusion/exe directory. This MUST be a path to a NAMED
    executable file (the executable need not exist beforehand).

13. Change the OUTFILEDIR variable to point to where you want the output HDF5 file to be stored. Recommended you
    put it in the basicFusion/jobOutput directory. This must be a path to a NAMED HDF5 file (with a valid HDF5
    suffix).

14. Change the FILELISTDIR variable to point to where the fileList.txt file (generated by generateInput.sh) will be
    stored. This MUST be the same value you entered for the OUTFILE variable in the generateInput.sh script.

15. Change the STD_STREAM_OUTFILE variable to point to where you want the program output to be stored. This must be
    a full path to a named .txt file (the file need not exist beforehand).
    
16. Change the value of the HLIBPATH variable to point to your hdf library folder (such a folder must contain both
    hdf4 and hdf5 libraries) if you plan on using your own compiled dynamic HDF libraries. If you do not, do not
    modify this variable. For the sake of simplicity, it is recommended you do not worry about this.

                    BATCH SCRIPT
A script has been provided to make compiling and running the program easy. In the basicFusion directory, the script called
"jobSubmit" in the bin directory contains all of the necessary commands to run the program. All you need to do is to run
this script. Here are the steps the script takes (you can also do these steps manually):


Step 1: Load modules
  a. cd into bin
  Execute the command
    . ./loadModules

    Be sure not to leave out the lone period before ./loadModules. This ensures that the modules are loaded
    onto your current BASH process. Leaving out the period would load them into a child process (which then
    immediately closes, effectively doing nothing to your current process).

    OR run the commands manually:

    a. module --verbose swap PrgEnv-cray PrgEnv-intel
    b. module --verbose load szip/2.1
    c. module --verbose load cray-hdf5/1.8.16
    d. module --verbose load hdf4/4.2.10

Step 2: Compile the program
  a. cd into root project directory
  b. make

Step 3: Generate the inputFiles.txt file using the generateInput.sh script
  a. cd into bin
  b. ./generateInput.sh [path to 5 instrument directories]

Setp 4: Set environment variables to tell program whether or not to unpack data
  a. export TERRA_DATA_UNPACK=1 (if you want unpacking)
  b. unset TERRA_DATA_UNPACK (if you don't want unpacking)

Step 4: Submit the executable to the BW job queue (qsub)
  a. qsub -v TERRA_DATA_UNPACK [path to batchscript.pbs]

Check the status of the job by executing the command "qstat | grep [your BW username]". A flag of "Q" means the job
is enqueued. A flag of "R" means the job is currently executing. A flag of "C" means the job has completed. 
     
     
                    INTERACTIVE

To run the program in interactive mode, follow these steps:

Step 1: Enter interactive mode
    qsub -I -l nodes=1 -l walltime=03:00:00
  You may enter any value in for walltime. This is the time you will be alloted in interactive mode. It is given
  by HH:MM:SS. Larger wall times means you will wait in the queue for a longer period.

Step 2: Load modules
  Execute the first step of the batch script instructions.
  Recall: cd into the bin directory and run (with the lone period): . ./loadModules

Step 3: Compile
  Enter into the root program directory and enter: make

Step 3:
  Generate the inputFiles.txt file using the generateInput.sh script located in bin.
  Give the script a path to the 5 instruments.

Step 4:
  Run the following command:
  aprun -n 1 "$EXE" "$OUTFILEDIR" "$FILELISTDIR" &> "$STD_STREAM_OUTFILE"

  Replacing all of the variables ($EXE, $OUTFILEDIR etc ) with the variables you set in your batchscript.pbs file
 
  Explanation of the aprun command argument by argument:
  -n [number]: number of nodes requested
  $EXE: path to executable program
  $OUTFILEDIR: argument to the executable program of what the output file will be called
  $FILELISTDIR: Argument to executable of the inputFiles.txt file
  &> $STD_STREAM_OUTFILE: Send all program output to this text file
  
The program is now being executed. Use:

qstat | grep [your BW username]

to check the status of your job.
  
 

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

