Module bfutils.orbit
--------------------

Variables
---------
AST_re

CER_re

MIS_re_AGP

MIS_re_GP

MIS_re_GRP

MIS_re_HRLL

MOD_re

MOP_re

Functions
---------
findProcPartition(numProcess, numTasks)
    **DESCRIPTION:**  
        This function implements a bin-packing algorithm. It attempts
        to fill numProcess number of bins with numTasks number of elements
        as evenly as possible. This can be used for non-dynamic load balancing
        MPI programs.
        
    **ARGUMENTS:**   
        *numProcess (int)* -- The number of MPI processes (i.e. the number of 
                            bins to fill)  
        *numTasks   (int)* -- Number of individual tasks to perform

    **EFFECTS:**    
        None

    **RETURN:**    
        Returns a list of size numProcess. Each element in the list tells how 
        many orbits each MPI process will handle.

isBFfile(file_list)
    **DESCRIPTION:**  
        This function takes as input a list of strings containing a file path 
        and determines, based on a regex pattern, if the file is or is not a 
        'proper' Basic Fusion file. What constitutes a proper BF file is 
        documented on this project's GitHub.

    **ARGUMENTS:**  
        *file_list (list)*  -- A list of filenames or file paths.

    **EFFECTS:**  
        None

    **RETURN:**  
        A list of integers of size len( file_list ), where each element 
        directly corresponds to the elements in file_list.  
        For each element:  
        -1 indicates that the file is not proper.  
        0 indicates a proper MOPITT file match.  
        1 indicates a proper CERES file matchh.  
        2 indicates a proper MODIS file match.  
        3 indicates a proper ASTER file match.  
        4 indicates a proper MISR GRP file match.  
        5 indicates a proper MISR AGP file match.  
        6 indicates a proper MISR GMP file match.  
        7 indicates a proper MISR HRLL file match.

makeRunDir(dir)
    **DESCRIPTION:**  
        This function makes a directory called run# where # is the first integer greater than any other run# in
        the directory given by the argument dir.
        
    **ARGUMENTS:**  
        *dir (str)*   -- Directory to create the run#.
        
    **EFFECTS:**  
        Creates a directory.

    **RETURN:**  
        Returns the full path of the newly created directory.

orbitYear(orbit)
    **DESCRIPTION:**  
        Returns the year that orbit belongs to.
        
    **ARGUMENTS:**  
        *orbit (int)* -- Terra orbit
        
    **EFFECTS:**  
        None
        
    **RETURN:**  
        Year that orbit belongs to.      
        -1 on failure.

orbit_start(orbit, orbit_info='/mnt/a/u/sciteam/clipp/basicFusion/externLib/BFpyEnv/lib/python2.7/site-packages/bfutils/Orbit_Path_Time.txt')
    **DESCRIPTION:**  
        This function finds the starting time of the orbit according to the Orbit_Path_Info.txt file. Please
        see the GitHub documentation on how to generate this file.
        
    **ARGUMENTS:**  
        *orbit (int)* -- Orbit to find starting time of  
        *orbit_info (str)*    -- Path to the orbit_info.txt file
        
    **EFFECTS:**  
        None
        
    **RETURN:**   
        String containing the starting time in the format yyyymmddHHMMSS.
