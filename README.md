# Terra Fusion Project
**University of Illinois - Urbana/Champaign**  
**Authors:**
  1. Landon Clipp <clipp2@illinois.edu>
  2. MuQun Yang
  
The Terra Fusion project was created to fuse the MOPITT, CERES, MISR, ASTER and MODIS data into one, unified HDF5 "Basic Fusion" file set. One Basic Fusion **(BF)** granule contains data from each instrument over a certain time period. The BF granularity is defined as one Terra orbit. 

This file outlines the structure of the combined TERRA Fusion code, how to compile it on a local machine, and how to add
additional code to the program. This file may not be up to date during development.

## Installation
Although this program can be installed and run on local machines, it was developed under the assumption that it would be run on either the Blue Waters or the ROGER super computers. The following instructions detail how to install the program on either of the super computers.

#### Required Libraries
The Basic Fusion program requires the following non-standard C libraries to function:

1. <hdf.h>  -- HDF4 library
1. <mfhdf.h> -- A netCDF library that is included with installation of the HDF4 library
1. <hdf5.h> -- The HDF5 library
1. <hdf5_hl.h> -- The HDF5 lite API

As can be inferred, users of this program must have both HDF4 and HDF5 libraries installed on their machine. Instructions on installing can be found at the HDF website: https://www.hdfgroup.org/. When installing the libraries on your machine, it is highly recommended that you configure the installation to include both static and dynamic libraries.

In addition to the HDF libraries, the Basic Fusion program is also dependent on the szip and jpeg libraries. These dependencies arise within the HDF libraries themselves. Please note that both Blue Waters and ROGER provide pre-compiled, pre-installed versions of all of these libraries through the use of the "module" construct. You can see a list of available modules by invoking "module avail" in the terminal and grepping the output to see the desired HDF, szip, or jpeg modules you need.


1. cd into the desired directory and type
    ```
    git clone https://YOUR_GITHUB_USERNAME@github.com/TerraFusion/basicFusion
    ```
1. cd into the `basicFusion/util` directory
1. Configure the external dependencies by running the configureEnv.sh script. This script handles downloading and setting up the Python dependencies used by the database generation scripts, as well as NCSA's Scheduler program used for parallel execution of the Basic Fusion code.  

    Run `./configureEnv.sh` to see what arguments it needs to run. Use the -a flag to download everything:
        ```
        ./configureEnv.sh -a
        ```
    This will download the dependencies to the basicFusion/externLib directory. Note that if at any point this script fails, please check its code to ensure that it is loading a proper MPICH module (the name of the module can change from site to site).
    
1. Check the output of configureEnv.sh. If it completed without any errors, the dependencies were successfully created.

## Database generation

A suite of scripts have been written under `basicFusion/metadataInput/build` to generate the SQLite database of all the input HDF granules. Run the findThenGenerate.sh script to:  
1. Discover all of the input HDF files available and  
2. Build the SQLite database from this list.

```
./findThenGenerate /path/to/inputfiles
```

The `/path/to/inputfiles` directory MUST contain the following subdirectories inside: ASTER, MISR, MOPITT, MODIS, CERES. This script will parse each of these subdirectories for all of the available files. This script can take some time to generate depending on the total size of the input data. The SQLite database will be saved as basicFusion/metadata-input/accesslist.sqlite

**NOTE!!!**: This shell script uses a Python script called **fusionBuildDB** to parse the listing of all available files and then generate the SQLite database. This script is run using the Python Virtual Environment created during the **Installation** section. IT IS REQUIRED that you follow the Installation section first to ensure that the virtual environment is installed.

## Input File Listing Generation

Once the database has been generated, we need to generate all of the input file listings as required by the basicFusion program itself. These files tell the BF program which input HDF files to process. 

#### Generate a single input file
Under `basicFusion/metadata-input/genInput`, the genFusionInput.sh script generates one single BasicFusion input file list. This script requires the SQLite database built in the Database Generation section for file querying. An example of how to run this script:

`./genFusionInput.sh /path/to/accesslist.sqlite 69400 /path/to/outputList.txt`

Your resulting input file list will be a properly formatted, canonical BasicFusion input file list.

#### Generate a range of input files
Under `basicFusion/metadata-input/genInput`, the genInputRange_SX.sh script can generate any range of input files. This range is specified by starting orbit and ending orbit. Because the process of querying the database is very slow, the execution of these queries on the compute nodes is necessary. This script will automatically handle submitting the database queries to the Blue Waters job queue.

An example of how to invoke this script:

```
./genInputRange_SX.sh /path/to/accesslist.sqlite 69400 70000 /path/to/output/dir
```

Use `qstat | grep [your username]` to see the status of your jobs.

Based on the orbit start and end times, as well as the parameters set inside of the script (max job size etc.), it determines how many resources to request of the Blue Waters TORQUE resource manager. It will then generate all of the necessary files for parallel computation in the `basicFusion/jobFiles/BFinputGen` directory and submit its jobs to the queue. The resultant output files will reside in your `/path/to/output` directory.

**NOTE!!!**: Inside the genInputRange_SX.sh script, there are variables that can be changed to tweak how the script requests resources from Blue Waters. These variables are under the **JOB PARAMETERS** section, and it is recommended you change these from their default values to suit your needs. For most purposes, the default values should work fine. **ALSO NOTE** that this script requires the NCSA Scheduler Fortran program to be installed! Please refer to the Installation section of this Readme.

## Compilation
### Blue Waters
A series of different Makefiles have been provided to compile the program under basicFusion/Makefiles. The two main flavors are static and dynamic Makefiles. Currently, the dynamic Makefiles (as of Jul 12, 2017) are not operational.

```
cp Makefile.bwStatic ../Makefile
```
Load the necessary modules:
```
module load hdf4 hdf5 szip
```
Run: `make`. If it compiles without any errors (warnings are acceptable), the compilation was successful.


## Program Execution (Blue Waters Only)

There are multiple ways one can execute the BF program. First, **note that it is strongly recommended that you do NOT simply run the program by invoking "./basicFusion..."**. Running the program this way executes it on a login-node, and the IO-intensive nature of this program means that it will eat up shared resources on the login node, causing other BW users to experience poor responsiveness in the login nodes. Not to mention, this is a breach of the BW terms of use.

A script has been written under `basicFusion/TerraGen/util/processBF_SX.sh` that handles submitting the basicFusion program for execution. This script expects to find the basicFusion executable, generated by compiling the program, in the basicFusion/bin directory. 

```
./processBF_SX.sh /path/to/input/txt/files 69400 70000 /path/to/output
```
Use `qstat | grep [your username]` to see the status of your jobs.

Currently, the `/path/to/input/txt/files` must be the directory where all the input granule list text files are (these text files cannot be in any subdirectories). The `/path/to/output` will be where all of the output HDF5 granules are placed. The script will determine---exactly how the genInputRange_SX.sh does---how many jobs, nodes per job, and processors per node to use based on the number of orbits you desire to process. The maximum values for number of jobs, nodes, and processors can be changed inside the script in the **JOB PARAMETERS** section.

Please refer to the command line description of ./processBF_SX.sh for a full listing of all the available parameters.
