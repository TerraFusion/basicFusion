# Terra Granule Generation
___

## submitWorkflow.py
This script is the official workflow for the Basic Fusion project. It represents the highest level of abstraction. submitWorkflow.py handles generating all necessary PBS files, log directories, and administrative metadata necessary to submit for massive parallel generation of Basic Fusion granules. It also handles automatically submitting the processing jobs to the Blue Waters scheduler, as well as defining the resources requested to the TORQUE resource manager. It is the burden of the user to ensure the following assumptions of the script are met:

1. The input Basic Fusion data resides on the Nearline archive system  
    - The tar directory is sorted by year, where each year directory is flat.  
    - Each tar file is named #archive.tar, where # is the orbit number.  
    - Each tar file contains all of the necessary input Terra files for that specific orbit with the exception of MISR_HRLL and MISR_AGP files.  
    - Each tar file contains a file called MISR_PATH_FILES.txt. This text file contains two lines, the name of the MISR_HRLL and MISR_AGP files to include in this orbit. HRLL and AGP files are not directly included in the tar files because both are small sets repeated many times throughout all Terra orbits. A single HRLL or AGP file may be present in thousands of orbits.  
2. The Python environment has been properly configured with `basicFusion/configureEnv.sh`. This script needs to be run in order to initialize the Python virtual environment.
3. The user has properly compiled the BasicFusion code:  
    - Compile `HDF4 4.2.13` and `HDF5 1.8.16` from source. Blue Waters system HDF *cannot* be used! These may be compiled with system zlib, libjpeg, and szip libraries.  
    - Once the HDF libraries have been compiled, the Basic Fusion code must be compiled using gcc (NOT cc as recommended by Blue Waters documentation), linking your user-level HDF libraries. All HDF environment modules *must* be unloaded before attempting to compile the code.  
4. The user has gained proper authority from the NCSA to use the `#PBS -l flags=commtransparent` PBS directive. This flag directs the PBS scheduler to disperse the MPI compute nodes across the system instead of attempting to group the nodes in a tight geometry. This is done to prevent network congestion of the system due to the IO intensive nature of the workflow.
5. The user has logged into the Globus Python CLI and activated both the Blue Waters and the Nearline Globus endpoints. This can be done by executing and completing the following terminal commands in sequence:

    ```
    source activateBF
    globus login
    globus endpoint activate [BW ENDPOINT ID]
    globus endpoint activate [NEARLINE ENDPOINT ID]
    ```
    
    The configureEnv.sh script exports the BW and Nearline endpoint IDs into the environment variables `GLOBUS_BW` and `GLOBUS_NL`. These values can thus be accessed by: `$GLOBUS_BW` and `$GLOBUS_NL`. 
    
If these assumptions are met, the user has a fair chance at successful granule generation. To execute submitWorkflow.py:

```
source activateBF
python submitWorkflow.py --help
```

Due to the verbosity of the script's arguments, it is recommended that users create a simple shell wrapper around the script so that arguments may be easily saved and changed if needed.

