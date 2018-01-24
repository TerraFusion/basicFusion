# Basic Fusion

<p align="center"> <img src="https://github.com/TerraFusion/basicFusion/blob/master/doc/img/nasaLogo-570x450.png" width="100" hspace="25"/> <img src="https://github.com/TerraFusion/basicFusion/blob/master/doc/img/ncsa_horizontal.png" width="150" hspace="25" /> <img src="https://github.com/TerraFusion/basicFusion/blob/master/doc/img/blue_waters_logo.png" width="150" hspace="25" /> <img src="https://github.com/TerraFusion/basicFusion/blob/master/doc/img/trademark-examples2.png" width="48" hspace="50" /> </p>

The Terra weather satellite is the flagship of NASA's Earth Observing System. Aboard Terra are five instruments, MOPITT, CERES, MISR, ASTER, and MODIS. Atmospheric researchers have been largely unable to use more than one or two instruments at a time due to the complexities involved in fusing the data from different file formats, different grids, and different data gathering methods. The Basic Fusion project aims at providing a solution to this issue by fusing the Level 1B data from the five instruments into one, unified "Basic Fusion" **(BF)** file set. One BF granule contains data from each instrument over a single Terra orbit.

## Installation

#### Dependencies
The Basic Fusion program requires the following Hierarchical Data Format (HDF) C libraries to function:

1. [HDF4 4.2.13](https://support.hdfgroup.org/release4/obtain.html)
1. [HDF5 1.8.16](https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.8/hdf5-1.8.16/)

No other version of HDF libraries may be used. It is required that users install both of these libraries from source. Do not attempt to use Blue Waters HDF. The HDF libraries also depend on jpeg, zlib, and szip (optional). Users may use system installs of these libraries when compiling the HDF source.

In addition to the C dependencies, many of the Python scripts depend on external packages. These are automatically resolved through the configureEnv.sh script described below.

### GitHub Download & Python Install

1. cd into the desired directory and type
    ```
    git clone https://YOUR_GITHUB_USERNAME@github.com/TerraFusion/basicFusion
    ```
1. cd into the `basicFusion/util` directory
1. Configure the external Python dependencies by running the configureEnv.sh script.  
    ```
    ./configureEnv.sh -a
    ```
    This will download the Python dependencies, install [NCSA Scheduler](https://github.com/ncsa/Scheduler), and download/install HDF libraries to the basicFusion/externLib directory. This script requires proper visibility to a system MPI library to install mpi4py. If mpi4py installation fails, please ensure you have a proper MPI environment module loaded.
    
A helper script is instaled in ~/bin/ that aids in activating the Python virtual environment. The virtual environment can then be activated by:

```
source activateBF
```

If users have more than one basicFusion repository on the file system and have called ./configureEnv.sh from multiple places, it is then strongly recommended to explicitly activate the virtual environment as such:

```
source /path/to/basicFusion/externLib/BFpyEnv/bin/activate
```

### Compilation

The following steps need to be followed to compile the BF program:
1. Compile the required HDF4 4.2.13 and HDF5 1.8.16 from source, linking against system zlib and, optionally, szip. This should have been accomplished with configureEnv.sh.
2. `cp basicFusion/Makefiles/Makefile.bwStatic basicFusion/Makefile`. Modify the variables in Makefile under `MODIFY THIS VARIABLE` as indicated by the comments.
3. Run `make`

Compilation will likely not succeed on the first try, so some tinkering with makefiles and libraries may need to be done.

Please see KNOWN ISSUES for issues specific to compiling the BF program.

### bfutils
bfutils is a Python package written specifically to support many of the high-level operations in Basic Fusion. This package is automatically installed into your BasicFusion virtenv's site-packages directory by configureEnv.sh. Some useful functions this package can perform:

- Finding Terra orbit start times
- Submitting Globus transfers
- Determining if an input Terra file is appropriate for inclusion in the BasicFusion product

After sourcing the virtenv, you can import the package in any script using:

`import bfutils`

This function acts as the authority on which files are proper and which are not. You can view the function interface by following these lines:

```
(BFpyEnv)[clipp@cg-gpu01:~/basicFusion/util]$ source activateBF
(BFpyEnv)[clipp@cg-gpu01:~/basicFusion/util]$ python  
>>> import bfutils
>>> help(bfutils)  
```



## Database generation

The BF program itself requires as an argument a text file that lists all of the input HDF files for a particular granule. The production of these input text files is aided by a suite of scripts that have been written in `basicFusion/metadata-input/`. Users can generate an SQLite database of all the input HDF files using the scripts in `basicFusion/metadataInput/build`. This database is necessary to gather the correct input files for each orbit. It can be generated by using the script in the build directory:

```
./findThenGenerate /path/to/inputfiles/
```

The `/path/to/inputfiles/` directory MUST contain the following subdirectories inside: ASTER, MISR, MOPITT, MODIS, CERES. This script will recursively search each of these subdirectories for all of the available files. This script can take ~10 minutes to complete. The SQLite database will be saved at basicFusion/metadata-input/accesslist.sqlite

**NOTE!!!**: This shell script uses a Python script called **fusionBuildDB** to parse the listing of all available files and then generate the SQLite database. This script is run using the Python Virtual Environment created during the **Installation** section. IT IS REQUIRED that you follow the Installation section first to ensure that the virtual environment is installed.

## Input File Listing Generation

Once the database has been generated, we need to generate all of the input file listings as required by the basicFusion program itself. These files tell the BF program which input HDF files to process. 

#### Generate a single input file
Under `basicFusion/metadata-input/genInput`, the genFusionInput.sh script generates one input file listing for a single BF granule. This script requires the SQLite database built in the Database Generation section for file querying. An example of how to run this script:

`./genFusionInput.sh /path/to/accesslist.sqlite 69400 /path/to/outputList.txt --SQL`

Your resulting input file list will be a properly formatted, canonical BasicFusion input file list.

#### Generate a range of input files
Under `basicFusion/metadata-input/genInput`, the genInputRange_SX.sh script can generate any range of input files. This range is specified by starting orbit and ending orbit. Because the process of querying the database is very slow, the execution of these queries on the compute nodes is necessary. This script will automatically handle submitting the database queries to the Blue Waters job queue.

An example of how to invoke this script:

```
./genInputRange_SX.sh /path/to/accesslist.sqlite 69400 70000 /path/to/output/dir
```

Use `showq -u [username]` (without square brackets) to see the status of your jobs.

Based on the orbit start and end times, as well as the parameters set inside of the script (max job size etc.), it determines how many resources to request of the Blue Waters TORQUE resource manager. It will then generate all of the necessary files for parallel computation in the `basicFusion/jobFiles/BFinputGen` directory and submit its jobs to the queue. The resultant output files will reside in your `/path/to/output` directory.

*This script is not meant for production. The script is not written to structure the output directories in any way, so the output directory will remain flat.*

**NOTE!!!**: Inside the genInputRange_SX.sh script, there are variables that can be changed to tweak how the script requests resources from Blue Waters/ROGER. These variables are under the **JOB PARAMETERS** section, and it is recommended you change these from their default values to suit your needs. Do not run this script without reviewing the **JOB PARAMETERS** to determine if the values there are appropriate for the current system and job that you want to run! **ALSO NOTE** that this script requires the NCSA Scheduler Fortran program to be installed! Please refer to the Installation section of this Readme.

## Program Execution

There are multiple ways one can execute the BF program. Users may simply execute the BF binary from the login node, however you should not execute more than 1 instantiation of the program on login nodes due to the IO-intensive nature of the program. 

### processBF_SX.sh
*This script is not designed for production-level granule generation*

A demo script has been written under `basicFusion/TerraGen/util/processBF_SX.sh` that handles submitting the basicFusion program for execution. This script expects to find the basicFusion executable, generated by compiling the program, in the basicFusion/bin directory. 

```
./processBF_SX.sh /path/to/input/txt/files 69400 70000 /path/to/output
```

Use `showq -u [username]` to see the status of your jobs.

Currently, the `/path/to/input/txt/files` must be a flat directory (these text files cannot be in any subdirectories). The `/path/to/output` will be a flat directory where all of the output HDF5 granules are placed. The script will determine---exactly how the genInputRange_SX.sh does---how many jobs, nodes per job, and processors per node to use based on the number of orbits you desire to process. The maximum values for number of jobs, nodes, and processors can be changed inside the script in the **JOB PARAMETERS** section.

**NOTE!!!**: Inside the processBF_SX.sh script, there are variables that can be changed to tweak how the script requests resources from Blue Waters/ROGER. These variables are under the **JOB PARAMETERS** section, and it is recommended you change these from their default values to suit your needs. Do not run this script without reviewing the **JOB PARAMETERS** to determine if the values there are appropriate for the current system and job that you want to run! **ALSO NOTE** that this script requires the NCSA Scheduler Fortran program to be installed! Please refer to the Installation section of this Readme.

Please refer to the command line description of ./processBF_SX.sh for a full listing of all the available parameters.

### submitWorkflow.py
*This script is the official production-level workflow for BasicFusion.*

This script resides in `basicFusion/util/TerraGen`. This script submits the Basic Fusion workflow to the Blue Waters scheduler. It is designed with a very specific set of assumptions that must be carefully adhered to. Please see the readme.md file in `basicFusion/util/TerraGen` for full details of its operation.


## Known Issues
- The `git` module is known to cause issues on ROGER when submitting to the queue.
- Compiling with the Blue Waters cc wrapper utility is known to cause unstable binary files that will unexpectedly fail when called as a subprocess from an MPI program. It is required on Blue Waters to build HDF libraries and the the BF code itself using gcc.
