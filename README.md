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

### Blue Waters
1. cd into the desired directory and type
    ```
    git clone https://github.com/TerraFusion/basicFusion
    ```
1. cd into the basicFusion/bin/util directory
1. Configure the external dependencies by running the configureEnv.sh script. This script handles downloading and setting up the Python dependencies used by the database generation scripts, as well as NCSA's Scheduler program used for parallel execution of the Basic Fusion code. Run `./configureEnv.sh` to see what arguments it needs to run. Use the -a flag to download everything:
    ```
    ./configureEnv.sh -a
    ```
    This will download the dependencies to the basicFusion/externLib directory.
1. Check your basicFusion/externLib directory to make sure that the BFpyEnv and Scheduler directories have been made. If so, then the programming environment has been successfully created.

## Database generation
### Blue Waters
A suite of scripts have been written under basicFusion/metadataInput/build to generate the SQLite database of all the input HDF granules. Run the findThenGenerate.sh script to: 1. Discover all of the input HDF files available and 2. Build the SQLite database from this list.

```
./findThenGenerate /path/to/inputfiles /path/to/output/SQLiteDB.sqlite
```

The /path/to/inputfiles directory MUST contain the following subdirectories inside: ASTER, MISR, MOPITT, MODIS, CERES. This script will parse each of these subdirectories for all of the available files. This script can take some time to generate depending on the total size of the input data.

This shell script uses a Python script called **fusionBuildDB** to parse the listing of all available files and then generate the SQLite database. This script is run using the Python Virtual Environment created during the **Installation** section.

## Input File Listing Generation
### Blue Waters
Once the database has been generated, we need to generate all of the input file listings as required by the basicFusion program itself. These files tell the BF program which input HDF files to process. Under basicFusion/metadata-input/genInput, the genInputRange_SX.sh script can generate any range of input files. This range is specified by starting orbit and ending orbit. Because the process of querying the database is a slow process, the execution of these queries on the compute nodes is necessary.
An example of how to invoke this script:

```
./genInputRange_SX.sh /path/to/SQLiteDB.sqlite 69400 70000 /path/to/output
```

Based on the orbit start and end times, as well as the parameters set inside of the script (max job size etc.), it determines how many resources to request of the Blue Waters TORQUE resource manager. It will then generate all of the necessary files for parallel computation in the basicFusion/jobFiles/BFinputGen directory and submit its jobs to the queue. The resultant output files will reside in your /path/to/output directory.

NOTE: Inside the genInputRange_SX.sh script, there are variables that can be changed to tweak how the script requests resources from Blue Waters. These variables are under the **JOB PARAMETERS** section, and it is recommended you change these from their default values to suit your needs. For most purposes, the default values should work fine.


## Program Execution
### Blue Waters
There are multiple ways one can execute the BF program. First, **note that it is strongly recommended that you do NOT simply run the program by invoking "./basicFusion..."**. Running the program this way executes it on a login-node, and the IO-intensive nature of this program means that it will eat up shared resources on the login node, causing other BW users to experience poor responsiveness in the login nodes. Not to mention, this is a breach of the BW terms of use.

UNDER CONSTRUCTION

### ROGER

1. Copy Makefile.roger found in the Makefiles directory to Makefile in the root project directory:  
    ```bash
    cp Makefiles/Makefile.roger ./Makefile
    ```
2. Load the necessary modules:  
    ```bash
    module load zlib libjpeg hdf4/4.2.12 hdf5  
    ```
    Note that it is useful if you put this command in your ~/.bash_rc file so that these modules are loaded every time you enter a new shell.
3. Run make under the package root directory:  
    ```bash
    make
    ```
4. The program is now ready for execution under the bin directory. It requires three arguments: Name of the output HDF5 file, path to a text file containing the input files, path to the orbit_info.bin file. The orbit_info.bin file contains information about orbit start and end times required for the subsetting of MOPITT and CERES. Please see the [relevant Wiki page](https://github.com/TerraFusion/basicFusion/wiki/orbit_info.bin-file) on how to generate this file if needed (a copy of this file should be included by default under the bin directory).
    - There are multiple ways to run the program. If using a **small** input file list, it can simply be invoked from the login node:
        ~~~~
        ./basicFusion out.h5 inputFiles.txt orbit_info.bin
        ~~~~ 
    - If not using small input, please refer to the [relevant wiki page](https://github.com/TerraFusion/basicFusion/wiki/ROGER-Parallel-Execution) for parallel execution of the program. Please be careful not to run this program with large input as doing so will consume a large amount of shared resources on the login node! This would be in violation of NCSA terms of use.
5. NOTES
    - Some sample input files are located in the inputFileDebug directory. The content inside the file may or may not point to valid file paths, but it nonetheless provides an example of "good" input to the Fusion program. [Please refer to the relevant wiki page](https://github.com/TerraFusion/basicFusion/wiki/Fusion-Program-Input-File-Specification) for details on how these input files must be structured.
    - The program by default runs unpacking code for some of the datasets. This unpacking behavior can be turned off by setting the proper environment variable:
        ```
        export TERRA_DATA_PACK=1
        ```
        Unpacking converts some of the integer-valued datasets into floating point values that correspond to real physical units. The data is originally packed from floating point values to integers after being retrieved from the satellites in order to conserve space. It is a form of data compression. Disabling the unpacking behavior will result in some significant changes to the structure of the output HDF5 file (some datasets/attributes will not be added if unpacking is not performed).
