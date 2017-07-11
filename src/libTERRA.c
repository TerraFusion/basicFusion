
/*


    AUTHOR:
        Landon Clipp

    EMAIL:
        clipp2@illinois.edu


*/

#include "libTERRA.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <hdf5.h>
#include <hdf.h>
#include <mfhdf.h>
#include <assert.h>
#define DIM_MAX 10


/*
                        insertDataset
    DESCRIPTION:
        This function takes the data given in the array data_out and writes it to the
        output HDF5 group given by datasetGroup_ID. The argument returnDatasetID is
        treated as a boolean, where 0 means do not return the output dataset ID (close
        the identifier) and non-zero means return the output dataset ID (do not close the
        identifier).

    ARGUMENTS:
        0. outdatasetName  -- The name that the output dataset will have. Can be set to NULL
                              if the same name as the input is desired. NOTE that any non-alphaneumeric
                              characters will be changed to '_' to comply with netCDF convention.
        1. outputFileID    -- A pointer to the file identifier of the output file.
        2. datasetGroup_ID -- A pointer to the group in the output file where the data is
                              to be written to.
        3. returnDatasetID -- An integer (boolean). 0 means do not return the identifier
                              for the newly created dataset in the output file. In such
                              a case, the function will close the identifier. Non-zero
                              means return the identifier (and do not close it). In such
                              a case, it will be the responsibility of the caller to close
                              the identifier when finished.
        4. rank            -- The rank (number of dimensions) of the array being written
        5. datasetDims     -- A single dimension array of size rank describing the size
                              of each dimension in the data_out array.
        6. dataType        -- An HDF5 datatype describing the type of data contained in
                              data_out. Please refer to the HDF5 API specification under
                              the link "Predefined Datatypes."
        7. datasetName     -- A character string describing what the output dataset is to
                              be named.
i       8. data_out        -- A single dimensional array containing information of the
                              data to be written. This is passed as a void pointer because
                              the type of data contained in the array may vary between
                              function calls. The appropriate casting is applied in this
                              function.

    EFFECTS:
        The data_out array will be written under the group provided by datasetGroup_ID.
        If returnDatasetID is non-zero, an identifier to the newly created dataset will
        be returned.

    RETURN:
        Case 1 -- returnDatasetID == 0:
            Returns EXIT_FAILURE upon an error.
            Returns EXIT_SUCCESS upon successful completion.
        Case 2 -- returnDatasetID != 0:
            Returns EXIT_FAILURE upon an error.
            Returns the identifier to the newly created dataset upon success.
*/
/* OutputFileID is not used. NO need to have this parameter. MY 2017-03-03 */
hid_t insertDataset(  hid_t const *outputFileID, hid_t *datasetGroup_ID, int returnDatasetID,
                      int rank, hsize_t* datasetDims, hid_t dataType, const char *datasetName, const void* data_out)
{
    hid_t memspace;
    hid_t dataset;
    herr_t status;
    char *correct_dsetname;

    memspace = H5Screate_simple( rank, datasetDims, NULL );


    /* This is necessary since "/" is a reserved character in HDF5,we have to change it "_". MY 2016-12-20 */
    correct_dsetname = correct_name(datasetName);

    dataset = H5Dcreate( *datasetGroup_ID, correct_dsetname, dataType, memspace,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
    if(dataset<0)
    {
        FATAL_MSG("Unable to create dataset \"%s\".\n", datasetName );
        H5Sclose(memspace);
        free(correct_dsetname);
        return (EXIT_FAILURE);
    }

    status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5S_ALL, (VOIDP)data_out );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to dataset \"%s\".\n", datasetName );
        H5Dclose(dataset);
        H5Sclose(memspace);
        free(correct_dsetname);
        return (EXIT_FAILURE);
    }

    /* Free all remaining memory */
    free(correct_dsetname);
    H5Sclose(memspace);

    /*
     * Check to see if caller wants the dataset ID returned. If not, go ahead and close the ID.
     */

    if ( returnDatasetID == 0 )
    {
        H5Dclose(dataset);
        return EXIT_SUCCESS;
    }


    return dataset;


}

hid_t insertDataset_comp( hid_t const *outputFileID, hid_t *datasetGroup_ID, int returnDatasetID,
                          int rank, hsize_t* datasetDims, hid_t dataType, char* datasetName, void* data_out)
{
    hid_t memspace;
    hid_t dataset;
    herr_t status;
    char *correct_dsetname;

    //hsize_t chunk_dims[DIM_MAX];
    hid_t plist_id = H5Pcreate(H5P_DATASET_CREATE);

    if(plist_id <0)
    {
        FATAL_MSG("Cannot create the HDF5 dataset creation property list.\n");
        return(EXIT_FAILURE);
    }
    // The chunk size is the same as the array size
    if(H5Pset_chunk(plist_id,rank,datasetDims)<0)
    {
        FATAL_MSG("Cannot set chunk for the HDF5 dataset creation property list.\n");
        H5Pclose(plist_id);
        return(EXIT_FAILURE);
    }

    short gzip_comp_level = 0;

    //Set compression level,USE_GZIP must be a number
    {
        const char *s;
        s = getenv("USE_GZIP");
        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) >0)
                gzip_comp_level= (unsigned int)strtol(s,NULL,0);
    }

    // GZIP is only valid when the level is between 1 and 9
    if(gzip_comp_level >0 && gzip_comp_level <10)
    {
        if(H5Pset_deflate(plist_id,gzip_comp_level)<0)
        {
            FATAL_MSG("Cannot set deflate for the HDF5 dataset creation property list.\n");
            H5Pclose(plist_id);
            return(EXIT_FAILURE);
        }
    }

    memspace = H5Screate_simple( rank, datasetDims, NULL );
    if(memspace <0)
    {
        FATAL_MSG("Cannot create the memory space.\n");
        H5Pclose(plist_id);
        return(EXIT_FAILURE);
    }

    /* This is necessary since "/" is a reserved character in HDF5,we have to change it "_". MY 2016-12-20 */
    correct_dsetname = correct_name(datasetName);
    /*
    dataset = H5Dcreate( *datasetGroup_ID, correct_dsetname, dataType, memspace,
                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
    */

    dataset = H5Dcreate( *datasetGroup_ID, correct_dsetname, dataType, memspace,
                         H5P_DEFAULT, plist_id, H5P_DEFAULT );
    if(dataset<0)
    {
         FATAL_MSG("/gH5Dcreate -- Unable to create dataset \"%s\".\n", datasetName );
        H5Pclose(plist_id);
        H5Sclose(memspace);
        free(correct_dsetname);
        return (EXIT_FAILURE);
    }

    status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5S_ALL, (VOIDP)data_out );
    if ( status < 0 )
    {
         FATAL_MSG("/gH5DWrite -- Unable to write to dataset \"%s\".\n", datasetName );
        H5Pclose(plist_id);
        H5Dclose(dataset);
        H5Sclose(memspace);
        free(correct_dsetname);
        return (EXIT_FAILURE);
    }

    /* Free all remaining memory */
    free(correct_dsetname);
    H5Pclose(plist_id);
    H5Sclose(memspace);

    /*
     * Check to see if caller wants the dataset ID returned. If not, go ahead and close the ID.
     */

    if ( returnDatasetID == 0 )
    {
        H5Dclose(dataset);
        return EXIT_SUCCESS;
    }


    return dataset;


}



/*
                    MOPITTinsertDataset
    DESCRIPTION:
        This function reads the radiance dataset from the input File ID, and writes the
        dataset into a new, already opened file.
        This function assumes that
        1. The input file ID has already been prepared
        2. The output file ID has already been prepared
        3. The output group ID has already been prepared

        An absolute path for the inputDatasetPath is required if passing in file ID, else it
        can be relative to your object ID.

        The caller MUST be sure to free the dataset identifier using H5Dclose() if returnDatasetID is set to non-zero (aka true)
        or else a memory leak will occur.

        *********************************************
        *Motivation for the returnDatasetID argument*
        *********************************************
        If the caller will not perform any operations on the dataset, they do not have to worry about freeing the memory
        associated with the identifier. The function will handle that. If the caller does want to operate on the dataset,
        the program will not free the ID but rather return it to the caller.

    ARGUMENTS:
        IN
            1. input file ID pointer. Identifier already exists.
            2. output file ID pointer. Identifier already exists.
            3. input dataset absolute path string
            4. Output dataset name
            5. dataset group ID. Group identifier already exists. This is the group that the data will be read into
            6. returnDatasetID: Set to 0 if caller does not want to return the dataset identifier.
                                Set to non-zero if the caller DOES want the dataset identifier. If set to non-zero,
                                caller must be sure to free the identifier using H5Dclose() when finished.
            7. int bound    -- Provides the starting index and ending index for the TrackCount dimensions in the MOPITT datasets.
                               Set to NULL if entire dataset is desired.
                               bound[0] contains the starting index, bound[1] contains ending index.
    EFFECTS:
        Creates a new HDF5 file and writes the radiance dataset into it.
        Opens an HDF file pointer but does not close it.
    RETURN:

        !TWO CASES!

        Case returnDatasetID == 0:
            Returns EXIT_FAILURE if error occurs anywhere. Else, returns EXIT_SUCCESS
        Case returnDatasetID != 0:
            Returns EXIT_FAILURE if error occurs anywhere. Else, returns the dataset identifier
*/

hid_t MOPITTinsertDataset( hid_t const *inputFileID, hid_t *datasetGroup_ID,
                           char * inDatasetPath, char* outDatasetName, hid_t dataType, int returnDatasetID, unsigned int bound[2] )
{

    hid_t dataset = 0;                              // dataset ID
    hid_t dataspace = 0;                            // filespace ID
    hid_t memspace = 0;
    hid_t newmemspace = 0;
    hsize_t *datasetDims = NULL;                       // size of each dimension
    hsize_t *datasetMaxSize = NULL;                    // maximum allowed size of each dimension in datasetDims
    hsize_t *datasetChunkSize = NULL;

    herr_t status_n, status;

    double * data_out;

    int rank;
    short fail = 0;

    /* Make sure that bound[1] is >= bound[0] */
    if ( bound[1] < bound[0] )
    {
        FATAL_MSG("The bounds provided for MOPITT subsetting are invalid!\n\tThe right bound is less than the left bound.\n");
        goto cleanupFail;
    }

    // Get the corrected dataset name.
    outDatasetName = correct_name(outDatasetName);
    if ( outDatasetName == NULL )
    {
        FATAL_MSG("Failed to get corrected name.\n");
        goto cleanupFail;
    }

    /*
     * open dataset and do error checking
     */

    dataset = H5Dopen( *inputFileID, inDatasetPath, H5F_ACC_RDONLY );

    if ( dataset < 0 )
    {
         FATAL_MSG("/gH5Dopen -- Could not open \"%s\".\n", inDatasetPath );
        dataset = 0;
        goto cleanupFail;
    }


    /*
     * Get dataset dimensions, dimension max sizes and dataset dimensionality (the rank, aka number of dimensions)
     */

    dataspace = H5Dget_space(dataset);
    if ( dataspace < 0 )
    {
        dataspace = 0;
        FATAL_MSG("Failed to get dataspace.\n");
        goto cleanupFail;
    }

    rank = H5Sget_simple_extent_ndims( dataspace );

    if ( rank < 0 )
    {
        FATAL_MSG("Unable to get rank of dataset.\n");
        goto cleanupFail;
    }

    /* Allocate memory for the following arrays (now that we have the rank) */

    datasetDims = malloc( sizeof(hsize_t) * 5 );
    if ( datasetDims == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }
    datasetMaxSize = malloc( sizeof(hsize_t) * rank );
    if ( datasetMaxSize == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }
    datasetChunkSize = malloc( sizeof(hsize_t) * rank );
    if ( datasetChunkSize == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }
    /* initialize all elements of datasetDims to 1 */

    for ( int i = 0; i < 5; i++ )
        datasetDims[i] = 1;

    status_n  = H5Sget_simple_extent_dims(dataspace, datasetDims, datasetMaxSize );

    if ( status_n < 0 )
    {
        FATAL_MSG("Unable to get dimensions.\n");
        goto cleanupFail;
    }

    /* If we are doing subsetting, we need to select a hyperslab (a subset) of the dataset to read from.
     * Perform this operation using the hyperslab functions.
     */

    if ( bound )
    {
        const hsize_t start[5] = { bound[0], 0, 0, 0, 0 }; // Define the starting point of the hyperslab
        const hsize_t count[5] = { 1, 1, 1, 1, 1 };        // Define how many blocks in each dimension to read (in our case, just 1)
        // Define the size of the block
        const hsize_t block[5] = { bound[1] - bound[0] + 1, datasetDims[1], datasetDims[2], datasetDims[3], datasetDims[4] };
        status_n = H5Sselect_hyperslab( dataspace, H5S_SELECT_SET, start, NULL, count, block );


        datasetDims[0] = bound[1] - bound[0] + 1;
        data_out = malloc( sizeof(double) * datasetDims[0] * datasetDims[1] * datasetDims[2] *  datasetDims[3] * datasetDims[4]);


        memspace = H5Screate_simple(5, datasetDims, NULL);
        if ( memspace < 0 )
        {
            FATAL_MSG("Failed to create memory space.\n");
            goto cleanupFail;
        }

        status = H5Dread( dataset, dataType, memspace, dataspace, H5P_DEFAULT, data_out );
        if ( status < 0 )
        {
            FATAL_MSG("Unable to read dataset into output data buffer.\n");
            goto cleanupFail;
        }
    }

    else
    {
        /*
         * Create a data_out buffer using the dimensions of the dataset specified by datasetDims
         */
        data_out = malloc( sizeof(double ) * datasetDims[0] * datasetDims[1] * datasetDims[2] *  datasetDims[3] * datasetDims[4]);

        /*
         * Now read dataset into data_out buffer
         */

        memspace = H5Screate_simple(5, datasetDims, NULL);
        if ( memspace < 0 )
        {
            FATAL_MSG("Failed to create memory space.\n");
            goto cleanupFail;
        }
        status = H5Dread( dataset, dataType, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out );
        if ( status < 0 )
        {
            FATAL_MSG("Unable to read dataset into output data buffer.\n");
            goto cleanupFail;
        }
    }


    /* Free all memory associated with the input data */

    if ( H5Dclose(dataset) < 0 )
        WARN_MSG("Failed to close dataset.\n");

    dataset = 0;

    /*******************************************************************************************
     *Reading the dataset is now complete. We have freed all related memory for the input data *
     *(except of course the data_out array) and can now work on writing this data to a new file*
     *******************************************************************************************/

    newmemspace = H5Screate_simple(rank, datasetDims, NULL );

    /* Now that dataset has been created, read into this data set our data_out array */
    if ( bound )
    {
        dataset = H5Dcreate( *datasetGroup_ID, outDatasetName, dataType, newmemspace,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
        if ( dataset < 0 )
        {
            FATAL_MSG("Failed to create dataset.\n");
            dataset = 0;
            goto cleanupFail;
        }
        status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out );
        if ( status < 0 )
        {
            FATAL_MSG("Unable to write to dataset.\n");
            goto cleanupFail;
        }
    }
    else
    {

        dataset = H5Dcreate( *datasetGroup_ID, outDatasetName, dataType, newmemspace,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
        if ( dataset < 0 )
        {
            FATAL_MSG("Failed to create dataset.\n");
            dataset = 0;
            goto cleanupFail;
        }
        status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out );
        if ( status < 0 )
        {
            FATAL_MSG("Unable to write to dataset.\n");
            goto cleanupFail;
        }
    }

    if ( H5Sclose(dataspace) < 0 )
        WARN_MSG("Failed to close dataspace\n");

    dataspace = 0;

    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( outDatasetName ) free(outDatasetName);
    if ( returnDatasetID == 0 )
    {
        if ( dataset ) H5Dclose(dataset);
    }
    if ( dataspace ) if ( H5Sclose(dataspace) < 0 ) WARN_MSG("Failed to close dataspace.\n");
    if ( memspace ) H5Sclose(memspace);
    if ( newmemspace ) H5Sclose(newmemspace);
    if ( datasetDims ) free(datasetDims);
    if ( datasetMaxSize ) free(datasetMaxSize);
    if ( datasetChunkSize ) free(datasetChunkSize);
    if ( data_out ) free(data_out);

    if ( fail != 0 ) return EXIT_FAILURE;                   // first check if something failed

    if ( returnDatasetID == 0 ) return EXIT_SUCCESS;   // if caller doesn't want ID, return success

    if ( fail == 0 ) return dataset;                   // otherwise, caller does want ID
    else                                            // if all these checks failed, something went wrong
    {
        FATAL_MSG("Something strange just happened.\n");
        return EXIT_FAILURE;
    }

}

/*
                openFile (HDF5)
    DESCRIPTION:
        This function take a pointer to a file identifier and opens the file specified by
        the inputFileName. The main purpose of this is to update the value of the file
        identifier so that access to the file can proceed. This function allows for READ
        ONLY and R/W access.
    ARGUMENTS:
        1. pointer to file identifier
        2. input file name string
        3. File access flag. Allowable values are:
            H5F_ACC_RDWR
                For Read/Write
            H5F_ACC_RDONLY
                For read only.
    EFFECTS:
        Opens the specified file and updates the file identifier (provided in the
        first argument) with the necessary information to access the file.

    RETURN:
        Returns EXIT_FAILURE upon failure to open file. Otherwise, returns EXIT_SUCCESS
*/

herr_t openFile( hid_t *file, char* inputFileName, unsigned flags  )
{
    /*
     * Open the file and do error checking
     */

    *file = H5Fopen( inputFileName, flags, H5P_DEFAULT );

    if ( *file < 0 )
    {
         FATAL_MSG("/gH5Fopen -- Could not open HDF5 file.\n" );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;

}

/*
                createOutputFile
    DESCRIPTION:
        This function creates an ouptut HDF5 file. If the file with outputFileName already exists, errors will be thrown.
    ARGUMENTS:
        1. A pointer to the output file identifier
        2. output file name string
    EFFECTS:
        Creates a new HDF5 file if it doesn't exist. Updates argument 1.
    RETURN:
        EXIT_FAILURE on failure (equivalent to non-zero number in most environments)
        EXIT_SUCCESS on success (equivalent to 0 in most systems)
*/

herr_t createOutputFile( hid_t *outputFile, char* outputFileName)
{
    *outputFile = H5Fcreate( outputFileName, H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT );
    if ( *outputFile < 0 )
    {
         FATAL_MSG("/gH5Fcreate -- Could not create HDF5 file. Does it already exist? If so );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}


/*
                        createGroup
    DESCRIPTION:
        This function creates a new group in the output HDF5 file. The new group will be
        created as a child of referenceGroup. referenceGroup can either be a file ID, in
        which case the new group will be created in the root directory, or a group ID, in
        which case the new group will be created as a child of referenceGroup.
    ARGUMENTS:
        1. A pointer to the parent group ID
        2. A pointer to the new group ID. This is a pointer so that the value of the
           caller's newGroup variable will be updated to contain the ID of the new group.
        3. The name of the new group.
    EFFECTS:
        Creates a new directory in the HDF file, updates the group pointer.
        The name of the new group is determined by the function correct_name().
        Caller must be aware of this fact that name of the new group will not necessarily
        be the value passed in at newGroupName.
    RETURN:
        EXIT_FAILURE on failure
        EXIT_SUCCESS on success
*/

herr_t createGroup( hid_t const *referenceGroup, hid_t *newGroup, char* groupName)
{

    char* newGroupName = correct_name(groupName);

    *newGroup = H5Gcreate( *referenceGroup, newGroupName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
    if ( *newGroup < 0 )
    {
         FATAL_MSG("/gH5Gcreate -- Could not create '%s' root group.\n", newGroupName);
        *newGroup = EXIT_FAILURE;
        free(newGroupName);
        return EXIT_FAILURE;
    }

    free(newGroupName);
    return EXIT_SUCCESS;
}

/*
                        attributeCreate
    DESCRIPTION:
        This function creates one single attribute for the object specified by objectID.
        It is the duty of the caller to free the attribute identifier using H5Aclose().
    ARGUMENTS:
        1. ObjectID: identifier for object to create the attribute for
        2. attrName: string for the attribute
        3. datatypeID: datatype identifier. This describes what type of information the
           attribute will hold. Google: "HDF5 Predefined Datatypes"
    EFFECTS:
        Creates uninitialized attribute at the object specified by objectID
    RETURN:
        Returns the attribute identifier upon success.
        Returns EXIT_FAILURE upon failure.
*/

hid_t attributeCreate( hid_t objectID, const char* attrName, hid_t datatypeID )
{
    hid_t attrID;
    hid_t attrDataspace;

    hsize_t dims;

    // create dataspace for the attribute
    dims = 1;
    attrDataspace = H5Screate_simple( 1, &dims, NULL );

    // create attribute
    attrID = H5Acreate( objectID, attrName, datatypeID, attrDataspace, H5P_DEFAULT, H5P_DEFAULT );
    if ( attrID < 0 )
    {
         FATAL_MSG("/gH5Acreate -- Could not create attribute \"%s\".\n", attrName );
        return EXIT_FAILURE;
    }

    // close dataspace
    if ( H5Sclose(attrDataspace) < 0 )
         FATAL_MSG("/gH5Sclose -- Could not close attribute dataset for \"%s\". Memory leak possible.\n", attrName );

    return attrID;
}

/* MY 2016-12-20, helper function to obtain vgroup reference number. This is used to check if a vgroup exists. */
int32 H4ObtainLoneVgroupRef(int32 file_id, char *groupname)
{

    /************************* Variable declaration **************************/

    int32  status_32,            /* returned status for functions returning an int32 */
           vgroup_id;
    int32  lone_vg_number,       /* current lone vgroup number */
           num_of_lones = 0;     /* number of lone vgroups */
    int32 *ref_array;            /* buffer to hold the ref numbers of lone vgroups   */
    char  *vgroup_name;
    uint16 name_len;
    int32 ret_value = 0;

    /********************** End of variable declaration **********************/


    /*
    * Get and print the names and class names of all the lone vgroups.
    * First, call Vlone with num_of_lones set to 0 to get the number of
    * lone vgroups in the file, but not to get their reference numbers.
    */
    num_of_lones = Vlone (file_id, NULL, num_of_lones );

    /*
    * Then, if there are any lone vgroups,
    */
    if (num_of_lones > 0)
    {
        /*
        * use the num_of_lones returned to allocate sufficient space for the
        * buffer ref_array to hold the reference numbers of all lone vgroups,
        */
        //ref_array = (int32 *) malloc(sizeof(int32) * num_of_lones);
        ref_array =  malloc(sizeof(ref_array) * num_of_lones);

        /*
        * and call Vlone again to retrieve the reference numbers into
        * the buffer ref_array.
        */
        num_of_lones = Vlone (file_id, ref_array, num_of_lones);

        /*
        * Display the name and class of each lone vgroup.
        */
        for (lone_vg_number = 0; lone_vg_number < num_of_lones; lone_vg_number++)
        {
            /*
            * Attach to the current vgroup then get and display its
            * name and class. Note: the current vgroup must be detached before
            * moving to the next.
            */
            vgroup_id = Vattach (file_id, ref_array[lone_vg_number], "r");
            status_32 = Vgetnamelen(vgroup_id, &name_len);
            vgroup_name = (char *) HDmalloc(sizeof(char *) * (name_len+1));
            if (vgroup_name == NULL)
            {
                 FATAL_MSG("/gHDmalloc failed. Not enough memory for vgroup_name!\n");
                free(ref_array);
                return EXIT_FAILURE;
            }
            status_32 = Vgetname (vgroup_id, vgroup_name);

            if(strncmp(vgroup_name,groupname,strlen(vgroup_name))==0)
            {
                ret_value = ref_array[lone_vg_number];
                Vdetach(vgroup_id);
                HDfree(vgroup_name);
                break;
            }
            status_32 = Vdetach (vgroup_id);
            if (vgroup_name != NULL) HDfree(vgroup_name);

        } /* END FOR */

        free (ref_array);

    } /* END IF */

    return ret_value;


}

/*
                            H4readData
    DESCRIPTION:
        This function reads the HDF4 dataset into a buffer array. The dataset desired
        can simply be given by its name and the fileID containing the dataset.

    ARGUMENTS:
        1. fileID      -- The HDF4 file identifier where the dataset is contained.
        2. datasetName -- The name of the desired dataset.
        3. data        -- A pointer to an array where the data will be written to. This
                          is given as a void double pointer because the type of array
                          being passed to this function is not known at compile time.
                          the data buffer given by the caller (a single pointer/array)
                          will be updated to point to the information read (hence, a
                          double pointer). This data will be stored on the heap.
        4. rank        -- A pointer to a variable. The caller will pass in a pointer
                          to its local rank variable. The caller's rank variable will be
                          updated with the rank (number of dimensions) of the dataset
                          read.
                          PASS NULL IF CALLER DOESN'T WANT

        5. dimsizes    -- The caller will pass in an array which
                          will be updated to contain the sizes of each dimension in the
                          dataset that was read.

                          This array MUST be of size DIM_MAX.
                          PASS NULL IF CALLER DOESN'T WANT

        6. dataType    -- An HDF4 datatype describing the type of data in the desired
                          dataset. Please refer to Sectino 3 of the HDF4 Reference Manual,
                          "HDF Constant Definition List."

    EFFECTS:
        Memory is allocated on the heap for the data that was read. The void** data variable is updated
        (by a single dereference) to point to this memory. The rank and dimsizes variables are also updated
        to contain the corresponding rank and dimension size of the read data.

    RETURN:
        Returns EXIT_FAILURE on failure.
        Else, returns EXIT_SUCCESS.
*/

int32 H4readData( int32 fileID, const char* datasetName, void** data, int32 *retRank, int32* retDimsizes, int32 dataType, int32*h4_start,int32*h4_stride,int32*h4_count )
{
    int32 sds_id, sds_index;
    int32 rank;
    int32 dimsizes[DIM_MAX];
    int32 ntype;                    // number type for the data stored in the data set
    int32 num_attrs;                // number of attributes
    intn status;
    int32 start[DIM_MAX] = {0};
    int32 stride[DIM_MAX] = {0};
    int32 count[DIM_MAX];

    int total_elems = 1;

    /* get the index of the dataset from the dataset's name */
    sds_index = SDnametoindex( fileID, datasetName );
    if( sds_index < 0 )
    {
         FATAL_MSG("/gSDnametoindex: Failed to get index of dataset.\n");
        return EXIT_FAILURE;
    }

    sds_id = SDselect( fileID, sds_index );
    if ( sds_id < 0 )
    {
         FATAL_MSG("/gSDselect: Failed to select dataset.\n");
        return EXIT_FAILURE;
    }

    /* Initialize dimsizes elements to 1 */
    for ( int i = 0; i < DIM_MAX; i++ )
    {
        dimsizes[i] = 1;
        if(h4_count != NULL)
            count[i] = 1;
    }

    /* get info about dataset (rank, dim size, number type, num attributes ) */
    status = SDgetinfo( sds_id, NULL, &rank, dimsizes, &ntype, &num_attrs);
    if ( status < 0 )
    {
         FATAL_MSG("/gSDgetinfo: Failed to get info from dataset.\n");
        SDendaccess(sds_id);
        return EXIT_FAILURE;
    }


    // Adding subsetting information.

    if(h4_start != NULL)
    {
        for(int i = 0; i<rank; i++)
            start[i] = h4_start[i];
    }

    if(h4_stride != NULL)
    {
        for(int i = 0; i<rank; i++)
            stride[i] = h4_stride[i];
    }
    else
    {
        for ( int i = 0; i < rank; i++ )
            stride[i] = 1;
    }

    if(h4_count != NULL)
    {
        for(int i = 0; i<rank; i++)
            count[i] = h4_count[i];
    }

    // Number of total elements
    for (int i = 0; i < rank; i++)
    {
        if(h4_count!=NULL)
            total_elems *= count[i];
        else
            total_elems *= dimsizes[i];
    }


    /* Allocate space for the data buffer.
     * I'm doing some pretty nasty pointer casting trickery here. First, I'm casting
     * the void** data to a double pointer of the appropriate data type, then I'm
     * dereferencing that double pointer so that the pointer in the calling
     * function then contains the return value of malloc. Confused? So am I. It works.
     * It was either this or make a separate function for each individual data type. So,
     * having a void double pointer was the best choice.
     */

    *data = NULL;

    // The following needs to re-work,just temporarily fix it for the time being. KY 2017-03-31, the original code is kept.
    switch ( dataType )
    {
    case DFNT_FLOAT32:
        *((float**)data) = malloc (total_elems * sizeof( float ) );
        break;

    case DFNT_FLOAT64:
        *((double**)data) = malloc (total_elems* sizeof(double));
        break;

    case DFNT_UINT16:
        *((unsigned short int**)data) = malloc (total_elems* sizeof(unsigned short int));
        break;

    case DFNT_UINT8:
        *((uint8_t**)data) = malloc (total_elems* sizeof(uint8_t));
        break;

    case DFNT_INT32:
        *((int32_t**)data) = malloc (total_elems* sizeof(int32_t));
        break;

    default:
        FATAL_MSG("Invalid data type.\nIt may be the case that your datatype has not been accounted for in the %s function.\nIf that is the case, simply add your datatype to the function as a switch case.\n",__func__ );
        SDendaccess(sds_id);
        return EXIT_FAILURE;
    }
#if 0
    switch ( dataType )
    {
    case DFNT_FLOAT32:
        *((float**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]*dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9] * sizeof( float ) );
        break;

    case DFNT_FLOAT64:
        *((double**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
                                    *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9]
                                    * sizeof( double ) );
        break;

    case DFNT_UINT16:
        *((unsigned short int**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
                                                *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9]
                                                * sizeof( unsigned short int ) );
        break;

    case DFNT_UINT8:
        *((uint8_t**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
                                     *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9]
                                     * sizeof( uint8_t ) );
        break;

    case DFNT_INT32:
        *((int32_t**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
                                     *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9]
                                     * sizeof( int32_t ) );
        break;

    default:
        FATAL_MSG("Invalid data type.\nIt may be the case that your datatype has not been accounted for in the %s function.\nIf that is the case, simply add your datatype to the function as a switch case.\n",__func__ );
        SDendaccess(sds_id);
        return EXIT_FAILURE;
    }

#endif

    if(h4_count!=NULL)
        status = SDreaddata( sds_id, start, stride, count, *data );
    else
        status = SDreaddata( sds_id, start, stride, dimsizes, *data );

    if ( status < 0 )
    {
         FATAL_MSG("/gSDreaddata: Failed to read data.\n");
        SDendaccess(sds_id);
        if ( data != NULL ) free(data);
        return EXIT_FAILURE;
    }


    SDendaccess(sds_id);

    if ( retRank != NULL ) *retRank = rank;
    if ( retDimsizes != NULL )
    {
        if(h4_count!=NULL)
            for ( int i = 0; i < DIM_MAX; i++ ) retDimsizes[i] = count[i];
        else
            for ( int i = 0; i < DIM_MAX; i++ ) retDimsizes[i] = dimsizes[i];
    }
    return EXIT_SUCCESS;
}

/*
                    attrCreateString
    DESCRIPTION:
        Similar to the function attributeCreate, this function creates an attribute for
        the object given by objectID. However, this function is specifically for
        string attributes. The difference between this function and attributeCreate is
        that this function only creates string attributes and it also initializes
        the attribute with a string given by the argument value. Creating a string is
        a little more complicated than creating a number type, so a separate function
        was warranted.

    ARGUMENTS:
        1. objectID -- The object to create an attribute for.
        2. name     -- The name of the attribute
        3. value    -- The string that the attribute will contain

    EFFECTS:
        A new string attribute will be created for the object objectID.

    RETURN:
        Returns EXIT_FAILURE upon an error.
        Else, returns the attribute identifier. It is the duty of the caller to close
        the identifier using H5Aclose().
*/

hid_t attrCreateString( hid_t objectID, char* name, char* value )
{
    /* To store a string in HDF5, we need to create our own special datatype from a
     * character type. Our "base type" is H5T_C_S1, a single byte null terminated
     * string.
     */
    hid_t stringType;
    hid_t attrID;
    herr_t status;

    stringType = H5Tcopy(H5T_C_S1);
    H5Tset_size( stringType, strlen(value));


    attrID = attributeCreate( objectID, name, stringType );
    if ( attrID == EXIT_FAILURE )
    {
         FATAL_MSG("/gH5Aclose: Unable to create %s attribute.\n", name);
        H5Tclose(stringType);
        return EXIT_FAILURE;
    }

    status = H5Awrite( attrID, stringType, value );
    if ( status < 0 )
    {
         FATAL_MSG("/gH5Awrite: Unable to write %s attribute.\n", name);
        H5Tclose(stringType);
        H5Aclose(attrID);
        return EXIT_FAILURE;
    }
    H5Tclose(stringType);

    return attrID;
}

/*
                    readThenWrite
    DESCRIPTION:
        This function is an abstraction of reading a dataset from an HDF4 file
        and then writing it to the output HDF5 file. This function calls H4readData
        which is itself an abstraction for reading input data into a data buffer, and
        then writes that buffer to the output file using the insertDataset function.
        Once all writing is done, it frees allocated memory and returns the HDF5 dataset
        identifier that was created in the output file.

    ARGUMENTS:
        0. outDatasetName -- The name that the output HDF5 dataset will have. Note that the actual
                             name the output dataset will have is the one given by correct_name(outDatasetName).
                             Therefore you cannot assume that the output name will be exactly what
                             is passed to this argument.
                             Set to NULL if the same name as the inDatasetName is desired.

        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. inDatasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. outputDataType -- An HDF5 datatype identifier. Must be of the same general type
                             of the input data type. Please reference the HDF5 API
                             specification under "Predefined Datatypes" for a list of HDF5
                             datatypes.
        5. inputFileID    -- The HDF4 input file identifier

    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/

hid_t readThenWrite( const char* outDatasetName, hid_t outputGroupID, const char* inDatasetName, int32 inputDataType,
                     hid_t outputDataType, int32 inputFileID )
{
    int32 dataRank;
    int32 dataDimSizes[DIM_MAX];
    unsigned int* dataBuffer = NULL;
    hid_t datasetID;

    herr_t status;

    status = H4readData( inputFileID, inDatasetName,
                         (void**)&dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );

    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Unable to read \"%s\" data.\n", inDatasetName );
        if ( dataBuffer != NULL ) free(dataBuffer);
        return (EXIT_FAILURE);
    }

    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    if ( outDatasetName )
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, outDatasetName, dataBuffer );
    else
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, inDatasetName, dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
        // Have warning const char* to char*:
        const char* tempStr = outDatasetName ? outDatasetName : inDatasetName;
        FATAL_MSG("Error writing \"%s\" dataset.\n", tempStr );
        free(dataBuffer);
        H5Dclose(datasetID);
        return (EXIT_FAILURE);
    }


    free(dataBuffer);

    return datasetID;
}
/*
                    readThenWriteSubset
    DESCRIPTION:
        This function is an abstraction of reading a dataset from an HDF4 file
        and then writing it to the output HDF5 file. This function calls H4readData
        which is itself an abstraction for reading input data into a data buffer, and
        then writes that buffer to the output file using the insertDataset function.
        Once all writing is done, it frees allocated memory and returns the HDF5 dataset
        identifier that was created in the output file.

    ARGUMENTS:
        0. outDatasetName -- The name that the output HDF5 dataset will have. Note that the actual
                             name the output dataset will have is the one given by correct_name(outDatasetName).
                             Therefore you cannot assume that the output name will be exactly what
                             is passed to this argument.
                             Set to NULL if the same name as the inDatasetName is desired.

        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. inDatasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. outputDataType -- An HDF5 datatype identifier. Must be of the same general type
                             of the input data type. Please reference the HDF5 API
                             specification under "Predefined Datatypes" for a list of HDF5
                             datatypes.
        5. inputFileID    -- The HDF4 input file identifier

        6. start
        7. stride
        8. count
    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/

hid_t readThenWriteSubset( const char* outDatasetName, hid_t outputGroupID, const char* inDatasetName, int32 inputDataType,
                           hid_t outputDataType, int32 inputFileID,int32 *start,int32*stride,int32*count )
{
    int32 dataRank;
    int32 dataDimSizes[DIM_MAX];
    unsigned int* dataBuffer = NULL;
    hid_t datasetID;

    herr_t status;

    status = H4readData( inputFileID, inDatasetName,
                         (void**)&dataBuffer, &dataRank, dataDimSizes, inputDataType,start,stride,count );

    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Unable to read \"%s\" data.\n", inDatasetName );
        if ( dataBuffer != NULL ) free(dataBuffer);
        return (EXIT_FAILURE);
    }

    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    if ( outDatasetName )
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, outDatasetName, dataBuffer );
    else
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, inDatasetName, dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
        // Have warning const char* to char*:
        const char* tempStr = outDatasetName ? outDatasetName : inDatasetName;
        FATAL_MSG("Error writing \"%s\" dataset.\n", tempStr );
        free(dataBuffer);
        H5Dclose(datasetID);
        return (EXIT_FAILURE);
    }


    free(dataBuffer);

    return datasetID;
}


/*
        correct_name

 DESCRIPTION:
    Returns a pointer to a valid HDF5 name. This is following the CF convention.
    The returned string is allocated with malloc, so it will be the duty of the caller
    to free this string when done.

 ARGUMENTS:
    1. const char* oldname -- string to be corrected

 EFFECTS:
    Allocates memory on heap for returned string.

 RETURN:
    A valid pointer to the string upon success. NULL upon failure.
*/

char *correct_name(const char* oldname)
{

    char* newname = NULL;
    short offset = 0;
    short neededCorrect = 0;
#define IS_DIGIT(c)  ('0' <= (c) && (c) <= '9')
#define IS_ALPHA(c)  (('a' <= (c) && (c) <= 'z') || ('A' <= (c) && (c) <= 'Z'))
#define IS_OTHER(c)  ((c) == '_')

    if(oldname == NULL)
    {
        FATAL_MSG("The oldname can't be empty.\n");
        return NULL;
    }

    /* If the first character in oldname is a number, we will need to prepend it with '_'.
     * Thus, malloc will need to allocate one extra space.
     */

    if ( IS_DIGIT(oldname[0]) )
    {
        offset = 1;
        neededCorrect = 1;
    }

    newname = calloc(strlen(oldname)+1+offset, 1);

    if(newname == NULL)
    {
        FATAL_MSG("Not enough memory to allocate newname.\n");
        return NULL;
    }

    if ( offset ) newname[0] = '_';

    size_t i;
    for ( i = 0; i < strlen(oldname); i++ )
    {
        if ( IS_DIGIT(oldname[i]) || IS_ALPHA(oldname[i]) )
            newname[i+offset] = oldname[i];
        else
        {
            newname[i+offset] = '_';
            neededCorrect = 1;
        }
    }

#if DEBUG
    if ( neededCorrect )
    {
        fprintf(stderr, "DEBUG INFO: Output object name differs from input object name.\n");
        fprintf(stderr, "Old name: %s\nNew name: %s\n", oldname, newname);
    }
#endif

    return newname;
}

/*
                    readThenWrite_ASTER_Unpack
    DESCRIPTION:
        This function is a specific readThenWrite function for ASTER. This function does the
        same thing as the general readThenWrite except it performs data unpacking. Essentially,
        unpacking the data means converting the radiance datasets from an integer type to
        a floating point type. Doing this greatly increases the size of the data however.
        On an abstracted level, this is what unpacking looks like:

        unpackedElement = scale * packedElement + offset

        The packedElement is an integer type, and the unpackedElement is a single precision
        float type.

    ARGUMENTS:
        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. datasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. inputFileID    -- The HDF4 input file identifier
        5. unc            -- The uncertainty value for the unpacking

    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/
/* MY 2016-12-20, routine to unpack ASTER data */
hid_t readThenWrite_ASTER_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
                                  int32 inputFileID,float unc )
{
    int32 dataRank = 0;
    int32 dataDimSizes[DIM_MAX] = {0};
    unsigned short* tir_dataBuffer = NULL;
    uint8_t* vsir_dataBuffer = NULL;
    float* output_dataBuffer = NULL;
    size_t buffer_size = 1;
    hid_t datasetID = 0;
    hid_t outputDataType = 0;

    intn status = -1;

    if(unc < 0)
    {
         FATAL_MSG("/gThere is no valid Unit Conversion Coefficient for this dataset  %s.\n",  datasetName );
        return (EXIT_FAILURE);

    }

    if(DFNT_UINT8 == inputDataType)
    {
        status = H4readData( inputFileID, datasetName,
                             (void**)&vsir_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );
    }
    else if(DFNT_UINT16 == inputDataType)
    {
        status = H4readData( inputFileID, datasetName,
                             (void**)&tir_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );

    }
    else
    {
         FATAL_MSG("/gUnsupported datatype. Datatype must be either DFNT_UINT16 or DFNT_UINT8.\n" );
        return (EXIT_FAILURE);
    }


    if ( status == EXIT_FAILURE )
    {
         FATAL_MSG("/gUnable to read %s data.\n",  datasetName );
        if ( vsir_dataBuffer != NULL ) free(vsir_dataBuffer);
        if ( tir_dataBuffer != NULL ) free(tir_dataBuffer);
        return (EXIT_FAILURE);
    }

    {
        float* temp_float_pointer = NULL;
        uint8_t* temp_uint8_pointer = vsir_dataBuffer;
        unsigned short* temp_uint16_pointer = tir_dataBuffer;
        /* Now we need to unpack the data */
        for(int i = 0; i <dataRank; i++)
            buffer_size *=dataDimSizes[i];

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

        temp_float_pointer = output_dataBuffer;

        if(DFNT_UINT8 == inputDataType)
        {
            for(int i = 0; i<buffer_size; i++)
            {
                if(*temp_uint8_pointer == 0)
                    *temp_float_pointer = -999;
                else if(*temp_uint8_pointer == 1)
                    *temp_float_pointer = 0;
                else if(*temp_uint8_pointer == 255)// Now make no data differentiate from saturated data
                    *temp_float_pointer = -998;
                else
                    *temp_float_pointer = (float)((*temp_uint8_pointer -1))*unc;
                temp_float_pointer++;
                temp_uint8_pointer++;
            }
        }
        else if(DFNT_UINT16 == inputDataType)
        {
            for(int i = 0; i<buffer_size; i++)
            {
                if(*temp_uint16_pointer == 0)
                    *temp_float_pointer = -999;
                else if(*temp_uint16_pointer == 1)
                    *temp_float_pointer = 0;
                else if(*temp_uint16_pointer == 4095)// Now make no data differentiate from saturated data
                    *temp_float_pointer = -998;
                else
                    *temp_float_pointer = (float)((*temp_uint16_pointer-1))*unc;
                temp_float_pointer++;
                temp_uint16_pointer++;

            }
        }

    }
    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    outputDataType = H5T_NATIVE_FLOAT;

    short use_chunk = 0;

    /* If using chunk */
    {
        const char *s;
        s = getenv("USE_CHUNK");

        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) == 1)
                use_chunk = 1;
    }

    if(use_chunk == 1)
    {
        datasetID = insertDataset_comp( &outputFile, &outputGroupID, 1, dataRank,
                                        temp, outputDataType, datasetName, output_dataBuffer );
    }
    else
    {
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, datasetName, output_dataBuffer );
    }


    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", datasetName );
        if ( vsir_dataBuffer != NULL ) free(vsir_dataBuffer);
        if ( tir_dataBuffer != NULL ) free(tir_dataBuffer);
        if ( output_dataBuffer != NULL ) free(output_dataBuffer);
        return (EXIT_FAILURE);
    }

    if ( vsir_dataBuffer != NULL ) free(vsir_dataBuffer);
    if ( tir_dataBuffer != NULL ) free(tir_dataBuffer);
    if ( output_dataBuffer != NULL ) free(output_dataBuffer);
    return datasetID;
}

/*
                    readThenWrite_MISR_Unpack
    DESCRIPTION:
        This function is a specific readThenWrite function for MISR. This function does the
        same thing as the general readThenWrite except it performs data unpacking. Essentially,
        unpacking the data means converting the radiance datasets from an integer type to
        a floating point type. Doing this greatly increases the size of the data however.
        On an abstracted level, this is what unpacking looks like:

        unpackedElement = scale * packedElement + offset

        The packedElement is an integer type, and the unpackedElement is a single precision
        float type.

    ARGUMENTS:
        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. datasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. retDatasetName -- The actual output dataset name. The output name cannot be
                             guaranteed to be the same as the input name (argument datasetName).
                             Function will modify the char* pointed to by this argument to point
                             to the output dataset name.

                             CALLER MUST FREE THIS VARIABLE!!!! It is allocated by malloc!

        4. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        5. inputFileID    -- The HDF4 input file identifier
        6. scale_factor   -- Value of "scale" in the unpacking equation

    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/
/* MY-2016-12-20 Routine to unpack MISR data */
hid_t readThenWrite_MISR_Unpack( hid_t outputGroupID, char* datasetName, char** retDatasetNamePtr,int32 inputDataType,
                                 int32 inputFileID,float scale_factor )
{
    int32 dataRank = 0;
    int32 dataDimSizes[DIM_MAX] = {0};
    unsigned short* input_dataBuffer = NULL;
    float * output_dataBuffer = NULL;
    hid_t datasetID = 0;
    hid_t outputDataType = 0;
    intn status = 0;
    char* newdatasetName = NULL;

    if(scale_factor < 0)
    {
         FATAL_MSG("/gThe scale_factor of %s is less than 0.\n",  datasetName );
        return (EXIT_FAILURE);

    }
    status = H4readData( inputFileID, datasetName,
                         (void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL);
    if ( status < 0 )
    {
         FATAL_MSG("/gUnable to read %s data.\n",  datasetName );
        if ( input_dataBuffer != NULL ) free(input_dataBuffer);
        return (EXIT_FAILURE);
    }

    /* Before unpacking the data, we want to re-arrange the name */
    char* RDQIName = "/RDQI";
    char* temp_sub_dsetname = strstr(datasetName,RDQIName);


    if(temp_sub_dsetname!=NULL && strncmp(RDQIName,temp_sub_dsetname,strlen(temp_sub_dsetname)) == 0)
    {
        newdatasetName = malloc( strlen(datasetName) - strlen(RDQIName) + 1 );
        memset(newdatasetName,'\0',strlen(datasetName)-strlen(RDQIName)+1);

        strncpy(newdatasetName,datasetName,strlen(datasetName)-strlen(RDQIName));
    }
    else  // We will fail here.
    {
         FATAL_MSG("/gError: The dataset name doesn't end with /RDQI\n");
        return EXIT_FAILURE;
    }



    /* Data Unpack */
    {

        float* temp_float_pointer = NULL;
        unsigned short* temp_uint16_pointer = input_dataBuffer;
        size_t buffer_size = 1;
        unsigned short  rdqi = 0;
        unsigned short  rdqi_mask = 3;
        size_t num_la_data = 0;
        uint8_t has_la_data = 0;

        unsigned int* la_data_pos = NULL;
        unsigned int* temp_la_data_pos_pointer = NULL;

        for(int i = 0; i <dataRank; i++)
            buffer_size *=dataDimSizes[i];


        for(int i = 0; i<buffer_size; i++)
        {
            rdqi = (*temp_uint16_pointer)&rdqi_mask;
            if(rdqi == 1)
                num_la_data++;
        }

        if(num_la_data>0)
        {
            has_la_data = 1;
            la_data_pos = malloc(sizeof la_data_pos*num_la_data);
            temp_la_data_pos_pointer = la_data_pos;
        }

        /* We need to check if there are any the low accuracy data */


        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);
        temp_float_pointer = output_dataBuffer;

        unsigned short temp_input_val;

        if(has_la_data == 1)
        {
            for(int i = 0; i<buffer_size; i++)
            {

                rdqi = (*temp_uint16_pointer)&rdqi_mask;
                if(rdqi == 2 || rdqi == 3)
                    *temp_float_pointer = -999.0;
                else
                {
                    if(rdqi == 1)
                    {
                        *temp_la_data_pos_pointer = i;
                        temp_la_data_pos_pointer++;
                    }
                    temp_input_val = (*temp_uint16_pointer)>>2;
                    if(temp_input_val == 16378 || temp_input_val == 16380)
                        *temp_float_pointer = -999.0;
                    else
                        *temp_float_pointer = scale_factor*((float)temp_input_val);
                }
                temp_uint16_pointer++;
                temp_float_pointer++;
            }
        }
        else
        {
            for(int i = 0; i<buffer_size; i++)
            {
                rdqi = (*temp_uint16_pointer)&rdqi_mask;
                if(rdqi == 2 || rdqi == 3)
                    *temp_float_pointer = -999.0;
                else
                {
                    temp_input_val = (*temp_uint16_pointer)>>2;
                    if(temp_input_val == 16378 || temp_input_val == 16380)
                        *temp_float_pointer = -999.0;
                    else
                        *temp_float_pointer = scale_factor*((float)temp_input_val);
                }
                temp_uint16_pointer++;
                temp_float_pointer++;
            }
        }

        /* Here we want to record the low accuracy data information for this dataset.*/
        if(has_la_data ==1)
        {

            hid_t la_pos_dsetid =0 ;
            int la_pos_dset_rank = 1;
            hsize_t la_pos_dset_dims[1];
            la_pos_dset_dims[0]= num_la_data;
            char* la_pos_dset_name_suffix="_low_accuracy_pos";
            size_t la_pos_dset_name_len = strlen(la_pos_dset_name_suffix)+strlen(newdatasetName)+1;
            char* la_pos_dset_name=malloc(la_pos_dset_name_len);
            la_pos_dset_name[la_pos_dset_name_len-1]='\0';
            strcpy(la_pos_dset_name,newdatasetName);
            strcat(la_pos_dset_name,la_pos_dset_name_suffix);

            /* Create a dataset to remember the postion of low accuracy data */
            la_pos_dsetid = insertDataset( &outputFile, &outputGroupID, 1, la_pos_dset_rank,
                                           la_pos_dset_dims, H5T_NATIVE_INT, la_pos_dset_name, la_data_pos );

            if ( la_pos_dsetid == EXIT_FAILURE )
            {
                 FATAL_MSG("/gError writing %s dataset.\n", newdatasetName );
                if(la_pos_dset_name)
                    free(la_pos_dset_name);
                if(la_data_pos)
                    free(la_data_pos);
                H5Dclose(la_pos_dsetid);
                return (EXIT_FAILURE);
            }
            if(la_pos_dset_name)
                free(la_pos_dset_name);
            if(la_data_pos)
                free(la_data_pos);

            H5Dclose(la_pos_dsetid);
        }
        //else { } may add an attribute to the group later.
    }


    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];


    outputDataType = H5T_NATIVE_FLOAT;

    short use_chunk = 0;

    /* If using chunk */
    {
        const char *s;
        s = getenv("USE_CHUNK");

        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) == 1)
                use_chunk = 1;
    }

    if(use_chunk == 1)
    {
        datasetID = insertDataset_comp( &outputFile, &outputGroupID, 1, dataRank,
                                        temp, outputDataType, newdatasetName, output_dataBuffer );
    }
    else
    {
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, newdatasetName, output_dataBuffer );
    }

    //datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
    //    temp, outputDataType, newdatasetName, output_dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", datasetName );
        if(newdatasetName) free(newdatasetName);
        if( input_dataBuffer) free(input_dataBuffer);
        if( output_dataBuffer) free(output_dataBuffer);
        return (EXIT_FAILURE);
    }

    if ( retDatasetNamePtr )
        *retDatasetNamePtr= correct_name(newdatasetName);


    /*
       tempFloat = -999.0;
       if(H5LTset_attribute_float( datasetID, correctedName,"_FillValue",&tempFloat,1)<0) {
            FATAL_MSG("/gError writing %s dataset's fillvalue attributes.\n", datasetName );
           if(newdatasetName) free(newdatasetName);
           if( input_dataBuffer) free(input_dataBuffer);
           if( output_dataBuffer) free(output_dataBuffer);
           return (EXIT_FAILURE);

       }
    */

    free(input_dataBuffer);
    free(output_dataBuffer);
    if(newdatasetName) free(newdatasetName);

    return datasetID;
}

/*
                    readThenWrite_MODIS_Unpack
    DESCRIPTION:
        This function is a specific readThenWrite function for MODIS. This function does the
        same thing as the general readThenWrite except it performs data unpacking. Essentially,
        unpacking the data means converting the radiance datasets from an integer type to
        a floating point type. Doing this greatly increases the size of the data however.
        On an abstracted level, this is what unpacking looks like:

        unpackedElement = scale * packedElement + offset

        The packedElement is an integer type, and the unpackedElement is a single precision
        float type.

    ARGUMENTS:
        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. datasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. inputFileID    -- The HDF4 input file identifier

    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/

/* MY 2016-12-20 Unpack MODIS data */
hid_t readThenWrite_MODIS_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
                                  int32 inputFileID)
{

    //unsigned short special_values_unpacked[] = {65535,65534,65533,65532,65531,65530,65529,65528,65527,65526,65525,65500};
    //float special_values_packed[] = {-999.0,-998.0,-997.0,-996.0,-995.0,-994.0,-993.0,-992.0,-991.0,-990.0,-989.0,-988.0};
    char* radi_scales="radiance_scales";
    char* radi_offset="radiance_offsets";
    int32 dataRank = 0;
    int32 dataDimSizes[DIM_MAX] = {0};
    unsigned short* input_dataBuffer = NULL;
    float* output_dataBuffer = NULL;
    hid_t datasetID = 0;
    hid_t outputDataType = 0;


    unsigned short special_values_start = 65535;
    unsigned short special_values_stop = 65500;

    float special_values_packed_start = -999.0;

    intn status = -1;

    status = H4readData( inputFileID, datasetName,
                         (void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );
    if ( status < 0 )
    {
         FATAL_MSG("/gUnable to read %s data.\n",  datasetName );
        if ( input_dataBuffer ) free(input_dataBuffer);
        return (EXIT_FAILURE);
    }


    /* Data Unpack */
    {

        /* 1. Obtain radiance_scales and radiance_offsets. */
        int32 sds_id = -1;
        int32 sds_index = -1;
        int32 radi_sc_index = -1;
        int32 radi_off_index = -1;
        int32 radi_sc_type = -1;
        int32 radi_off_type = -1;
        int32 num_radi_sc_values = -1;
        int32 num_radi_off_values = -1;
        char temp_attr_name[H4_MAX_NC_NAME];

        float* radi_sc_values = NULL;
        float* radi_off_values = NULL;;


        /* get the index of the dataset from the dataset's name */
        sds_index = SDnametoindex( inputFileID, datasetName );
        if( sds_index < 0 )
        {
             FATAL_MSG("/g-- SDnametoindex -- Failed to get index of dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        sds_id = SDselect( inputFileID, sds_index );
        if ( sds_id < 0 )
        {
             FATAL_MSG("/gSDselect -- Failed to get the ID of the dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        radi_sc_index = SDfindattr(sds_id,radi_scales);
        if(radi_sc_index < 0)
        {
             FATAL_MSG("/gCannot find attribute %s of variable %s\n",radi_scales,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        if(SDattrinfo (sds_id, radi_sc_index, temp_attr_name, &radi_sc_type, &num_radi_sc_values)<0)
        {
             FATAL_MSG("/gCannot obtain SDS attribute %s of variable %s\n",radi_scales,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        radi_off_index = SDfindattr(sds_id,radi_offset);
        if(radi_off_index < 0)
        {
             FATAL_MSG("/gCannot find attribute %s of variable %s\n",radi_offset,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }


        if(SDattrinfo (sds_id, radi_off_index, temp_attr_name, &radi_off_type, &num_radi_off_values)<0)
        {
             FATAL_MSG("/gCannot obtain SDS attribute %s of variable %s\n",radi_offset,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        if(radi_sc_type != DFNT_FLOAT32 || radi_sc_type != radi_off_type || num_radi_sc_values != num_radi_off_values)
        {
             FATAL_MSG("/gError: ");
            fprintf(stderr, "Either the scale/offset datatype is not 32-bit floating-point type\n\tor there is inconsistency between scale and offset datatype or number of values\n");
            fprintf(stderr, "\tThis is for the variable %s\n",datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }


        radi_sc_values = calloc((size_t)num_radi_sc_values,sizeof radi_sc_values);
        radi_off_values= calloc((size_t)num_radi_off_values,sizeof radi_off_values);

        if(SDreadattr(sds_id,radi_sc_index,radi_sc_values) <0)
        {
             FATAL_MSG("/gCannot obtain SDS attribute value %s of variable %s\n",radi_scales,datasetName);
            free(radi_sc_values);
            free(radi_off_values);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }


        if(SDreadattr(sds_id,radi_off_index,radi_off_values) <0)
        {
             FATAL_MSG("/gCannot obtain SDS attribute value %s of variable %s\n",radi_scales,datasetName);
            free(radi_sc_values);
            free(radi_off_values);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        SDendaccess(sds_id);


        float* temp_float_pointer = NULL;
        unsigned short* temp_uint16_pointer = input_dataBuffer;
        size_t buffer_size = 1;
        size_t band_buffer_size = 1;
        int num_bands = dataDimSizes[0];

        if(num_bands != num_radi_off_values)
        {
             FATAL_MSG("/gError: Number of band (the first dimension size) of the variable %s\n\tis not the same as the number of scale/offset values\n",datasetName);
            free(radi_sc_values);
            free(radi_off_values);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        assert(dataRank>1);
        for(int i = 1; i <dataRank; i++)
            band_buffer_size *=dataDimSizes[i];

        buffer_size = band_buffer_size*num_bands;

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

        temp_float_pointer = output_dataBuffer;


        for(int i = 0; i<num_bands; i++)
        {
            float temp_scale_offset = radi_sc_values[i]*radi_off_values[i];
            for(int j = 0; j<band_buffer_size; j++)
            {
                /* Check special values  , here I may need to make it a little clear.*/
                if(((*temp_uint16_pointer)<=special_values_start) && ((*temp_uint16_pointer)>=special_values_stop))
                    *temp_float_pointer = special_values_packed_start +(special_values_start-(*temp_uint16_pointer));
                else
                {
                    *temp_float_pointer = radi_sc_values[i]*(*temp_uint16_pointer) - temp_scale_offset;
                }
                temp_uint16_pointer++;
                temp_float_pointer++;
            }
        }
        free(radi_sc_values);
        free(radi_off_values);



    }


    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    outputDataType = H5T_NATIVE_FLOAT;
#if 0
    short use_chunk = 0;

    /* If using chunk */
    {
        const char *s;
        s = getenv("USE_CHUNK");

        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) == 1)
                use_chunk = 1;
    }

    if(use_chunk == 1)
    {
        datasetID = insertDataset_comp( &outputFile, &outputGroupID, 1, dataRank,
                                        temp, outputDataType, datasetName, output_dataBuffer );
    }
    else
    {
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, datasetName, output_dataBuffer );
    }
#endif


    datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                               temp, outputDataType, datasetName, output_dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", datasetName );
        free(input_dataBuffer);
        free(output_dataBuffer);
        return (EXIT_FAILURE);
    }


    free(input_dataBuffer);
    free(output_dataBuffer);


    return datasetID;
}

/* Unpack MODIS uncertainty */
hid_t readThenWrite_MODIS_Uncert_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
        int32 inputFileID)
{

    char* scaling_factor="scaling_factor";
    char* specified_uncert="specified_uncertainty";
    int32 dataRank = 0;
    int32 dataDimSizes[DIM_MAX] = {0};
    uint8_t* input_dataBuffer = NULL;
    float* output_dataBuffer = NULL;
    hid_t datasetID = 0;
    hid_t outputDataType = 0;

    //Ideally the following 3 values should be retrieved from SDS APIs
    uint8_t fvalue = 255;
    uint8_t valid_min= 0;
    uint8_t valid_max = 15;
    float fvalue_packed = -999.0;

    intn status = -1;


    status = H4readData( inputFileID, datasetName,
                         (void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );
    if ( status < 0 )
    {
         FATAL_MSG("/gUnable to read %s data.\n",  datasetName );
        if( input_dataBuffer ) free(input_dataBuffer);
        return (EXIT_FAILURE);
    }


    /* Data Unpack */
    {

        /* 1. Obtain scaling_factor and specified uncertainty . */
        int32 sds_id = -1;
        int32 sds_index = -1;
        int32 sc_index = -1;
        int32 uncert_index = -1;
        int32 sc_type = -1;
        int32 uncert_type = -1;
        int32 num_sc_values = -1;
        int32 num_uncert_values = -1;
        char temp_attr_name[H4_MAX_NC_NAME];
        float* sc_values = NULL;
        float* uncert_values = NULL;;


        /* get the index of the dataset from the dataset's name */
        sds_index = SDnametoindex( inputFileID, datasetName );
        if( sds_index < 0 )
        {
             FATAL_MSG("/gSDnametoindex -- Failed to get index of dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        sds_id = SDselect( inputFileID, sds_index );
        if ( sds_id < 0 )
        {
             FATAL_MSG("/gSDselect -- Failed to get ID of dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        sc_index = SDfindattr(sds_id,scaling_factor);
        if(sc_index < 0)
        {
             FATAL_MSG("/gSDfindattr -- Cannot find attribute %s of variable %s\n",scaling_factor,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            return EXIT_FAILURE;
        }

        if(SDattrinfo (sds_id, sc_index, temp_attr_name, &sc_type, &num_sc_values)<0)
        {
             FATAL_MSG("/gSDattrinfo -- Cannot obtain SDS attribute %s of variable %s\n",scaling_factor,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            return EXIT_FAILURE;
        }

        uncert_index = SDfindattr(sds_id,specified_uncert);
        if(uncert_index < 0)
        {
             FATAL_MSG("/gSDfindattr -- Cannot find attribute %s of variable %s\n",specified_uncert,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            return EXIT_FAILURE;
        }


        if(SDattrinfo (sds_id, uncert_index, temp_attr_name, &uncert_type, &num_uncert_values)<0)
        {
             FATAL_MSG("/gSDattrinfo -- Cannot obtain attribute %s of variable %s\n",specified_uncert,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            return EXIT_FAILURE;
        }

        if(sc_type != DFNT_FLOAT32 || num_sc_values != num_uncert_values)
        {
             FATAL_MSG("/gError: Either the scale datatype is not 32-bit floating-point type or there is \n\tinconsistency of number of values between scale and specified uncertainty.\n");
            fprintf(stderr, "\tThis is for the variable %s\n",datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            return EXIT_FAILURE;
        }


        sc_values = calloc((size_t)num_sc_values,sizeof sc_values);
        uncert_values= calloc((size_t)num_uncert_values,sizeof uncert_values);

        if(SDreadattr(sds_id,sc_index,sc_values) <0)
        {
             FATAL_MSG("/gSDreadattr -- Cannot obtain SDS attribute value %s of variable %s\n",scaling_factor,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            free(sc_values);
            free(uncert_values);
            return EXIT_FAILURE;
        }

        if(SDreadattr(sds_id,uncert_index,uncert_values) <0)
        {
             FATAL_MSG("/gSDattrinfo -- Cannot obtain SDS attribute value %s of variable %s\n",specified_uncert,datasetName);
            free(input_dataBuffer);
            SDendaccess(sds_id);
            free(sc_values);
            free(uncert_values);
            return EXIT_FAILURE;
        }

        SDendaccess(sds_id);


        float* temp_float_pointer = NULL;
        uint8_t* temp_uint8_pointer = input_dataBuffer;
        size_t buffer_size = 1;
        size_t band_buffer_size = 1;
        int num_bands = dataDimSizes[0];

        if(num_bands != num_uncert_values)
        {
             FATAL_MSG("/gError: Number of band (the first dimension size) of the variable %s is not\n\tthe same as the number of scale/offset values\n",datasetName);
            free(input_dataBuffer);
            free(sc_values);
            free(uncert_values);
            return EXIT_FAILURE;
        }

        assert(dataRank>1);
        for(int i = 1; i <dataRank; i++)
            band_buffer_size *=dataDimSizes[i];

        buffer_size = band_buffer_size*num_bands;

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

        temp_float_pointer = output_dataBuffer;


        for(int i = 0; i<num_bands; i++)
        {
            for(int j = 0; j<band_buffer_size; j++)
            {
                /* Check special values  , here I may need to make it a little clear.*/
                if((*temp_uint8_pointer)== fvalue)
                    *temp_float_pointer = fvalue_packed;
                else if(((*temp_uint8_pointer)<valid_min) && ((*temp_uint8_pointer)>valid_max))
                {
                    /* currently set any invalid value to fill values */
                    *temp_float_pointer = fvalue_packed;
                }
                else
                {
                    *temp_float_pointer = uncert_values[i]*exp((*temp_uint8_pointer)/sc_values[i]);
                }
                temp_uint8_pointer++;
                temp_float_pointer++;
            }
        }
        free(sc_values);
        free(uncert_values);

    }


    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    outputDataType = H5T_NATIVE_FLOAT;
    short use_chunk = 0;

    /* If using chunk */
    {
        const char *s;
        s = getenv("USE_CHUNK");

        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) == 1)
                use_chunk = 1;
    }

    if(use_chunk == 1)
    {
        datasetID = insertDataset_comp( &outputFile, &outputGroupID, 1, dataRank,
                                        temp, outputDataType, datasetName, output_dataBuffer );
    }
    else
    {
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, datasetName, output_dataBuffer );
    }


    //datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
    //     temp, outputDataType, datasetName, output_dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", datasetName );
        free(input_dataBuffer);
        free(output_dataBuffer);
        return (EXIT_FAILURE);
    }


    free(input_dataBuffer);
    free(output_dataBuffer);


    return datasetID;
}
/*
                    readThenWrite_MODIS_GeoMetry_Unpack
    DESCRIPTION:
        This function is a specific readThenWrite function for MODIS. This function does the
        same thing as the general readThenWrite except it performs data unpacking. Essentially,
        unpacking the data means converting the radiance datasets from an integer type to
        a floating point type. Doing this greatly increases the size of the data however.
        On an abstracted level, this is what unpacking looks like:

        unpackedElement = scale * packedElement + offset

        The packedElement is an integer type, and the unpackedElement is a single precision
        float type.

    ARGUMENTS:
        1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
                             be written. Can either be an actual group ID or can be the
                             file ID (in the latter case, data will be written to the root
                             directory).
        2. datasetName    -- A string containing the name of the dataset in the input HDF4
                             file. The output dataset will have the same name.
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. inputFileID    -- The HDF4 input file identifier

    EFFECTS:
        Reads the input file dataset and then writes to the HDF5 output file. Returns
        a dataset identifier for the new dataset created in the output file. It is the
        responsibility of the caller to close the dataset ID when finished using
        H5Dclose().

    RETURN:
        Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
        any errors.
*/

/* MY 2017-03-03 Unpack MODIS Geometry data */
hid_t readThenWrite_MODIS_GeoMetry_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
        int32 inputFileID)
{

    int32 dataRank = 0;
    int32 dataDimSizes[DIM_MAX] = {0};
    short* input_dataBuffer = NULL;
    float* output_dataBuffer = NULL;
    hid_t datasetID = 0;
    hid_t outputDataType = 0;

    intn status = -1;

    status = H4readData( inputFileID, datasetName,
                         (void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType,NULL,NULL,NULL );
    if ( status < 0 )
    {
         FATAL_MSG("/gUnable to read %s data.\n",  datasetName );
        if ( input_dataBuffer ) free(input_dataBuffer);
        return (EXIT_FAILURE);
    }


    /* Data Unpack */
    {
        /*TODO
          We need to obtain scale_factor,units, _FillValue, valid_range and then
             unpack them to floating-point data.

             _FillValue = -32767 to _FillValue = -999.0
             valid_range  to valid_range*scale_factor
             units keep
             value * scale_factor, check _FillValue and change the value to -999.0.
             because of time contraint and information from the user's guide.
             scale_factor is always 0.01. We just multiple the value *0.01.
              */

        /* get the index of the dataset from the dataset's name */
        /* The following if 0 block may be useful for the future implementation. */
#if 0
        sds_index = SDnametoindex( inputFileID, datasetName );
        if( sds_index < 0 )
        {
             FATAL_MSG("/g-- SDnametoindex -- Failed to get index of dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        sds_id = SDselect( inputFileID, sds_index );
        if ( sds_id < 0 )
        {
             FATAL_MSG("/gSDselect -- Failed to get the ID of the dataset.\n");
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        radi_sc_index = SDfindattr(sds_id,radi_scales);
        if(radi_sc_index < 0)
        {
             FATAL_MSG("/gCannot find attribute %s of variable %s\n",radi_scales,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }

        if(SDattrinfo (sds_id, radi_sc_index, temp_attr_name, &radi_sc_type, &num_radi_sc_values)<0)
        {
             FATAL_MSG("/gCannot obtain SDS attribute %s of variable %s\n",radi_scales,datasetName);
            SDendaccess(sds_id);
            free(input_dataBuffer);
            return EXIT_FAILURE;
        }
#endif



        float* temp_float_pointer = NULL;
        short* temp_int16_pointer = input_dataBuffer;
        size_t buffer_size = 1;

        for(int i = 0; i <dataRank; i++)
            buffer_size *=dataDimSizes[i];

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

        temp_float_pointer = output_dataBuffer;
        short scaled_fillvalue = -32767;
        float _fillvalue = -999.0;
        float scale_factor = 0.01;

        for(int i = 0; i<buffer_size; i++)
        {
            /* Check fill values, need to retrieve instead of hard-code. No resources, follow the user's guide.*/
            if((*temp_int16_pointer)==scaled_fillvalue)
                *temp_float_pointer = _fillvalue;
            else
                *temp_float_pointer = scale_factor*(*temp_int16_pointer);

            temp_int16_pointer++;
            temp_float_pointer++;
        }

    }


    /* END READ DATA. BEGIN INSERTION OF DATA */

    /* Because we are converting from HDF4 to HDF5, there are a few type mismatches
     * that need to be resolved. Thus, we need to take the DimSizes array, which is
     * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
     * A simple casting might work, but that is dangerous considering hsize_t and int32
     * are not simply two different names for the same type. They are two different types.
     */
    hsize_t temp[DIM_MAX];
    for ( int i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) dataDimSizes[i];

    outputDataType = H5T_NATIVE_FLOAT;
#if 0
    short use_chunk = 0;

    /* If using chunk */
    {
        const char *s;
        s = getenv("USE_CHUNK");

        if(s && isdigit((int)*s))
            if((unsigned int)strtol(s,NULL,0) == 1)
                use_chunk = 1;
    }

    if(use_chunk == 1)
    {
        datasetID = insertDataset_comp( &outputFile, &outputGroupID, 1, dataRank,
                                        temp, outputDataType, datasetName, output_dataBuffer );
    }
    else
    {
        datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                                   temp, outputDataType, datasetName, output_dataBuffer );
    }
#endif


    datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank,
                               temp, outputDataType, datasetName, output_dataBuffer );

    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", datasetName );
        free(input_dataBuffer);
        free(output_dataBuffer);
        return (EXIT_FAILURE);
    }


    free(input_dataBuffer);
    free(output_dataBuffer);


    return datasetID;
}

herr_t convert_SD_Attrs(int32 sd_id,hid_t h5parobj_id,char*h5obj_name,char*sds_name)
{

    int   i;
    intn  h4_status;
    int32 sds_id,sds_index;
    int32 dim_sizes[H4_MAX_VAR_DIMS];
    int32 rank, data_type, n_attrs;
    int32   n_values;
    char    dummy_sds_name[H4_MAX_NC_NAME];
    char    attr_name[H4_MAX_NC_NAME];
    char*   attr_values= NULL;


    if(sds_name == NULL)
        sds_id = sd_id;
    else
    {
        sds_index = SDnametoindex(sd_id,sds_name);
        sds_id = SDselect (sd_id, sds_index);
    }

    h4_status = SDgetinfo (sds_id, dummy_sds_name, &rank, dim_sizes,
                           &data_type, &n_attrs);

#if 0
    printf ("name = %s\n", dummy_sds_name);
    printf ("rank = %d\n", rank);
    printf ("dimension sizes are : ");
    printf ("number of attributes is  %d\n", n_attrs);
#endif

    for( i = 0; i <n_attrs; i++)
    {
        h4_status = SDattrinfo (sds_id, i, attr_name, &data_type, &n_values);
        attr_values = malloc(n_values*DFKNTsize(data_type));
        h4_status = SDreadattr (sds_id, i, attr_values);
        copy_h5_attrs(data_type,n_values,attr_name,attr_values,h5parobj_id,h5obj_name);
        free (attr_values);
    }

    if(sds_name != NULL)
        SDendaccess(sds_id);
    return 0;
}

herr_t copy_h5_attrs(int32 h4_type,int32 n_values,char* attr_name,char* attr_value,hid_t par_id, char* h5obj_name)
{

    size_t h5_size = 0;
    int h5_sign = -2;
    hid_t h5memtype = 0;
    hid_t* h5memtype_ptr = &h5memtype;
    h4type_to_h5type(h4_type,h5memtype_ptr);
    h5memtype = *h5memtype_ptr;

    if(h5memtype == H5T_STRING)
    {
        hid_t str_type = H5Tcopy(H5T_C_S1);
        H5Tset_size(str_type,n_values+1);
        char*attr_value_new = malloc(n_values+1);
        strncpy(attr_value_new,attr_value,n_values);
        attr_value_new[n_values]='\0';
        H5Tset_strpad(str_type,H5T_STR_NULLTERM);
        herr_t ret= H5LTset_attribute_string(par_id,h5obj_name,attr_name,(const char*)attr_value_new);
        free(attr_value_new);
        return ret;
    }
    else
    {
        switch (H5Tget_class(h5memtype))
        {

        case H5T_INTEGER:
        {
            h5_size = H5Tget_size(h5memtype);
            h5_sign = H5Tget_sign(h5memtype);

            if (h5_size == 1)   // Either h5_signed char or unh5_signed char
            {
                if (h5_sign == H5T_SGN_2)
                    return H5LTset_attribute_char(par_id,h5obj_name,attr_name,(const char*)attr_value,n_values);
                else
                    return H5LTset_attribute_uchar(par_id,h5obj_name,attr_name,(const unsigned char*)attr_value,n_values);

            }
            else if (h5_size == 2)
            {
                if (h5_sign == H5T_SGN_2)
                    return H5LTset_attribute_short(par_id,h5obj_name,attr_name,(const short*)attr_value,n_values);
                else
                    return H5LTset_attribute_ushort(par_id,h5obj_name,attr_name,(const unsigned short*)attr_value,n_values);
            }
            else if (h5_size == 4)
            {
                if (h5_sign == H5T_SGN_2)
                    return H5LTset_attribute_int(par_id,h5obj_name,attr_name,(const int*)attr_value,n_values);
                else
                    return H5LTset_attribute_uint(par_id,h5obj_name,attr_name,(const unsigned int*)attr_value,n_values);
            }
            else if (h5_size == 8)
            {
                if (h5_sign == H5T_SGN_2)
                    return H5LTset_attribute_long_long(par_id,h5obj_name,attr_name,(const long long*)attr_value,n_values);
                else
                    return -1;
            }
            else return -1;
        }
        case H5T_FLOAT:
        {
            h5_size = H5Tget_size(h5memtype);
            if (h5_size == 4) return H5LTset_attribute_float(par_id,h5obj_name,attr_name,(const float*)attr_value,n_values);
            else if (h5_size == 8) return H5LTset_attribute_double(par_id,h5obj_name,attr_name,(const double*)attr_value,n_values);
            else return -1;
        }
        default:
            return -1;
        }
    }
    return 0;
}


herr_t H4readSDSAttr( int32 h4FileID, char* datasetName, char* attrName, void* buffer )
{
    int32 statusn = 0;
    int32 dsetIndex = SDnametoindex(h4FileID,datasetName);
    if ( dsetIndex < 0 )
    {
        FATAL_MSG("Failed to get dataset index.\n");
        return EXIT_FAILURE;
    }
    int32 dsetID = SDselect(h4FileID, dsetIndex);
    if ( dsetID < 0 )
    {
        FATAL_MSG("Failed to get dataset ID.\n");
        return EXIT_FAILURE;
    }
    int32 attrIdx = SDfindattr(dsetID,attrName);
    if ( attrIdx < 0 )
    {
        FATAL_MSG("Failed to get attribute index.\n");
        return EXIT_FAILURE;
    }
    statusn = SDreadattr(dsetID,attrIdx,(VOIDP) buffer );
    if ( statusn < 0 )
    {
        FATAL_MSG("Failed to read attribute.\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

/*                              getTime
    DESCRIPTION:
        This function takes a path (relative or absolute) to one of the instrument files
        and returns a string containing the date information contained in the file name.
        The string returned is allocated on the heap.

            !!!NOTE!!!

        IT IS THE DUTY OF THE CALLER
        to free pathname when done to avoid memory leaks.

        The instrument argument is defined as follows:
        0 = MOPITT
        1 = CERES
        2 = MODIS
        3 = ASTER
        4 = MISR
    ARGUMENTS:
        char* pathname -- the pathname to the hdf file
        int instrument -- an identifier specifying which instrument pathname refers to
    EFFECTS:
        Allocates a string on the heap and returns that string
    RETURN:
        A valid string upon success.
        NULL upon failure.
 */
char* getTime( char* pathname, int instrument )
{
    char* retString = NULL;
    char* start = NULL;
    char* end = NULL;
    int len = 0;

    /**********
     * MOPITT *
     **********/
    if ( instrument == 0 )
    {
        start = strstr ( pathname, "MOP01-" );
        if ( start == NULL )
        {
            FATAL_MSG("Expected MOPITT path.\n\tReceived \"%s\"\n", pathname);
            return NULL;
        }

        /* start is now pointing to the 'M'. We need to offset it to
         * point to the beginning of the date in the filename. char is
         * size 1, so we can simply offset it by 6
         */
        start += 6;
        end = strstr( pathname, ".he5" );
        if ( end == NULL )
        {
            FATAL_MSG("Error in reading the pathname for %s.\n","MOPITT");
            return NULL;
        }

        /* end points to the '.'. Offset it by -1 */
        end += -1;

        /* We want just the 7 characters after start */
        len = 7;
        retString = calloc ( len,1 );
        strncpy( retString, start, len);

        return retString;
    }

    /* repeat the same general process for each instrument */

    /*********
     * CERES *
     *********/
    else if ( instrument == 1 )
    {
        start = strstr( pathname, "CER_" );
        if ( start == NULL )
        {
            FATAL_MSG("Expected CERES path.\n\tReceived \"%s\"\n",pathname );
            return NULL;
        }

        end = pathname + strlen( pathname ) - 1;

        /* end should be pointing to a number. Do a check to make sure this is true. */
        if ( *end < '0' || *end > '9' )
        {
            FATAL_MSG("Error in reading the pathname for %s.\n", "CERES" );
            return NULL;
        }

        len = 9;
        retString = calloc(len,1);
        strncpy( retString, end-9, len );
        return retString;

    }

    /*********
     * MODIS *
     *********/
    else if ( instrument == 2 )
    {
        /* this do-while loop isn't actually a loop. I just want to be able to make use
         * of "break" to avoid unecessary checks. It was either this or the use of
         * goto. Couldn't think of an elegant way to do the following code using else-if
         * statements.
         */
        do
        {
            start = strstr( pathname, "MOD021KM.A" );
            if ( start != NULL )
            {
                start += 10;
                break;
            }

            start = strstr( pathname, "MOD03.A" );
            if ( start != NULL )
            {
                start += 7;
                break;
            }

            start = strstr( pathname, "MOD02HKM.A" );
            if ( start != NULL )
            {
                start += 10;
                break;
            }

            start = strstr( pathname, "MOD02QKM.A" );
            if ( start != NULL )
            {
                start += 10;
                break;
            }

            FATAL_MSG("Expected MODIS path.\n\tReceived \"%s\"\n", pathname );
            return NULL;

        } while ( 0 );

        len = 13;

        retString = calloc(len,1);
        strncpy( retString, start, len-1 );
        return retString;
    }

    /*********
     * ASTER *
     *********/
    else if ( instrument == 3 )
    {
        start = strstr( pathname, "L1T_" );
        if ( start == NULL )
        {
            FATAL_MSG("Expected ASTER path.\n\tReceived \"%s\"\n", pathname );
            return NULL;
        }

        // Skip the version number
        start += 7;
        len = 15;
        retString = calloc(len,1);
        strncpy( retString, start, len-1 );
        return retString;
    }

    /********
     * MISR *
     ********/
    else if ( instrument == 4 )
    {
        start = strstr( pathname, "MISR_" );
        if ( start == NULL )
        {
            FATAL_MSG("Expected MISR path.\n\tReceived \"%s\"\n",pathname );
            return NULL;
        }

        // Just want the orbit number
        start += 32;
        len = 7;
        retString = calloc(len,1);
        strncpy( retString, start, len-1 );
        return retString;
    }

    FATAL_MSG("Incorrect instrument argument.\n");

    return NULL;
}

/*-------------------------------------------------------------------
 * The following function has been copied from the H4toH5 library.
 * https://bitbucket.hdfgroup.org/projects/HDFEOS/repos/h4h5tools/browse/lib/src/h4toh5util.c
 *
    Function:    h4type_to_h5type

    DESCRIPTION:
        This function converts the given HDF4 datatype to an HDF5 datatype.
    ARGUMENTS:
        In:
            const int32 h4type -- The input hdf4 datatype
        Out:
            hid_t* h5memtype -- The corresponding HDF5 datatype
    EFFECTS:
        Updates caller variable to contain correct HDF5 datatype
    RETURN:
        FAIL upon failure
        SUCCEED upon success

 */
int  h4type_to_h5type(
    const int32 h4type,
    hid_t* h5memtype)
{

    size_t h4memsize = 0;
    size_t h4size = 0;
    switch (h4type)
    {

    case DFNT_CHAR8:

        h4size = 1;
        h4memsize = sizeof(int8);
        /* assume DFNT_CHAR8 C type character. */
        *h5memtype = H5T_STRING;
        //if(h5type) *h5type =  H5T_STRING;
        break;

    case DFNT_UCHAR8:

        h4size = 1;
        h4memsize = sizeof(int8);
        *h5memtype = H5T_STRING;
        //if(h5type) *h5type = H5T_STRING;
        break;

    case DFNT_INT8:

        h4size = 1;
        //if(h5type) *h5type = H5T_STD_I8BE;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype = H5T_NATIVE_SCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert signed INT8\n");
            return FAIL;
        }
        break;

    case DFNT_UINT8:

        h4size =1;
        //if(h5type) *h5type = H5T_STD_U8BE;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert unsigned INT8\n");
            return FAIL;
        }
        break;

    case DFNT_NINT8:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 1;
        //if(h5type) *h5type = H5T_NATIVE_SCHAR;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_SCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert native INT8\n");
            return FAIL;
        }
        break;

    case DFNT_NUINT8:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 1;
        //if(h5type) *h5type = H5T_NATIVE_UCHAR;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert unsighed naive INT8\n");
            return FAIL;
        }
        break;

    case DFNT_LINT8:
        h4size = 1;
        //if(h5type) *h5type = H5T_STD_I8LE;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {

            FATAL_MSG("cannot convert little-endian INT8\n");
            return FAIL;
        }
        break;

    case DFNT_LUINT8:
        h4size = 1;
        //if(h5type) *h5type = H5T_STD_U8LE;
        h4memsize = sizeof(int8);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert little-endian unsigned INT\n8");
            return FAIL;
        }
        break;

    case DFNT_INT16:
        h4size = 2;
        //if(h5type) *h5type = H5T_STD_I16BE;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert signed int16\n");
            return FAIL;
        }
        break;

    case DFNT_UINT16:
        h4size = 2;
        //if(h5type) *h5type = H5T_STD_U16BE;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert unsigned int16\n");
            return FAIL;
        }
        break;

    case DFNT_NINT16:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 2;
        //if(h5type) *h5type = H5T_NATIVE_SHORT;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert native int16\n");
            return FAIL;
        }
        break;

    case DFNT_NUINT16:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 2;
        //if(h5type) *h5type = H5T_NATIVE_USHORT;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert unsigned native int16\n");
            return FAIL;
        }
        break;

    case DFNT_LINT16:
        h4size = 2;
        //if(h5type) *h5type = H5T_STD_I16LE;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert little-endian int16\n");
            return FAIL;
        }
        break;

    case DFNT_LUINT16:
        h4size = 2;
        //if(h5type) *h5type = H5T_STD_U16LE;
        h4memsize = sizeof(int16);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert little-endian unsigned int16\n");
            return FAIL;
        }
        break;

    case DFNT_INT32:
        h4size = 4;
        //if(h5type) *h5type = H5T_STD_I32BE;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert signed int32\n");
            return FAIL;
        }
        break;

    case DFNT_UINT32:
        h4size = 4;
        //if(h5type) *h5type = H5T_STD_U32BE;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert unsigned int32\n");
            return FAIL;
        }
        break;

    case DFNT_NINT32:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 4;
        //if(h5type) *h5type = H5T_NATIVE_INT;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert native signed int32\n");
            return FAIL;
        }
        break;

    case DFNT_NUINT32:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting results may not be correct.\n");*/
        h4size =4;
        //if(h5type) *h5type = H5T_NATIVE_UINT;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert signed int32\n");
            return FAIL;
        }
        break;

    case DFNT_LINT32:
        h4size =4;
        //if(h5type) *h5type = H5T_STD_I32LE;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else
        {
            FATAL_MSG("cannot convert little-endian signed int32\n");
            return FAIL;
        }
        break;

    case DFNT_LUINT32:
        h4size =4;
        //if(h5type) *h5type = H5T_STD_U32LE;
        h4memsize = sizeof(int32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else
        {
            FATAL_MSG("cannot convert little-endian unsigned int32\n");
            return FAIL;
        }
        break;

    case DFNT_INT64:
        h4size = 8;
        //if(h5type) *h5type = H5T_STD_I64BE;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_LLONG;
        else
        {
            FATAL_MSG("cannot convert signed int64\n");
            return FAIL;
        }
        break;

    case DFNT_UINT64:
        h4size = 8;
        //if(h5type) *h5type = H5T_STD_U64BE;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_ULLONG;
        else
        {
            FATAL_MSG("cannot convert unsigned int64\n");
            return FAIL;
        }
        break;

    case DFNT_NINT64:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting result may not be correct.\n");*/
        h4size = 8;
        //if(h5type) *h5type = H5T_NATIVE_INT;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_LLONG;
        else
        {
            FATAL_MSG("cannot convert native signed int64");
            return FAIL;
        }
        break;

    case DFNT_NUINT64:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting results may not be correct.\n");*/
        h4size =8;
        //if(h5type) *h5type = H5T_NATIVE_UINT;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_ULLONG;
        else
        {
            FATAL_MSG("cannot convert signed int64\n");
            return FAIL;
        }
        break;

    case DFNT_LINT64:
        h4size =8;
        //if(h5type) *h5type = H5T_STD_I64LE;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_CHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_SHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_INT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_LONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_LLONG;
        else
        {
            FATAL_MSG("cannot convert little-endian signed int64\n");
            return FAIL;
        }
        break;

    case DFNT_LUINT64:
        h4size =8;
        //if(h5type) *h5type = H5T_STD_U64LE;
        h4memsize = sizeof(long);
        if(h4memsize == H5Tget_size(H5T_NATIVE_CHAR))
            *h5memtype =  H5T_NATIVE_UCHAR;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_SHORT))
            *h5memtype = H5T_NATIVE_USHORT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_INT))
            *h5memtype = H5T_NATIVE_UINT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LONG))
            *h5memtype = H5T_NATIVE_ULONG;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LLONG))
            *h5memtype = H5T_NATIVE_ULLONG;
        else
        {
            FATAL_MSG("cannot convert little-endian unsigned int64\n");
            return FAIL;
        }
        break;

    case DFNT_FLOAT32:
        h4size =4;
        //if(h5type) *h5type = H5T_IEEE_F32BE;
        h4memsize = sizeof(float32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else
        {
            FATAL_MSG("cannot convert float32\n");
            return FAIL;
        }
        break;

    case DFNT_FLOAT64:
        h4size = 8;
        //if(h5type) *h5type = H5T_IEEE_F64BE;
        h4memsize = sizeof(float64);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LDOUBLE))
            *h5memtype = H5T_NATIVE_LDOUBLE;
        else
        {
            FATAL_MSG("cannot convert float64\n");
            return FAIL;
        }
        break;

    case DFNT_NFLOAT32:
        /*printf("warning, Native HDF datatype is encountered");
        printf(" the converting results may not be correct.\n");*/
        h4size = 4;
        //if(h5type) *h5type = H5T_NATIVE_FLOAT;
        h4memsize = sizeof(float32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else
        {
            FATAL_MSG("cannot convert native float32\n");
            return FAIL;
        }
        break;

    case DFNT_NFLOAT64:
        /*    printf("warning, Native HDF datatype is encountered");
            printf(" the converting result may not be correct.\n");*/
        h4size = 8;
        //if(h5type) *h5type = H5T_NATIVE_DOUBLE;
        h4memsize = sizeof(float64);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LDOUBLE))
            *h5memtype = H5T_NATIVE_LDOUBLE;
        else
        {
            FATAL_MSG("cannot convert native float64\n");
            return FAIL;
        }
        break;

    case DFNT_LFLOAT32:
        h4size = 4;
        //if(h5type) *h5type = H5T_IEEE_F32LE;
        h4memsize = sizeof(float32);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else
        {
            FATAL_MSG("cannot convert little-endian float32\n");
            return FAIL;
        }
        break;

    case DFNT_LFLOAT64:
        h4size = 8;
        //if(h5type) *h5type = H5T_IEEE_F64LE;
        h4memsize = sizeof(float64);
        if(h4memsize == H5Tget_size(H5T_NATIVE_FLOAT))
            *h5memtype = H5T_NATIVE_FLOAT;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_DOUBLE))
            *h5memtype = H5T_NATIVE_DOUBLE;
        else if(h4memsize == H5Tget_size(H5T_NATIVE_LDOUBLE))
            *h5memtype = H5T_NATIVE_LDOUBLE;
        else
        {
            FATAL_MSG("cannot convert little-endian float64\n");
            return FAIL;
        }
        break;

    default:
    {
        FATAL_MSG("cannot find the corresponding datatype in HDF5.\nReceived type was: %d\n", h4type);
        return FAIL;
    }
    }
    return SUCCEED;
}

/****************change_dim_attr_NAME_value*******************************
*
* Source: h4h5tools
    https://bitbucket.hdfgroup.org/projects/HDFEOS/repos/h4h5tools/browse/lib/src/h4toh5sds.c
*
* Function:     change_dim_attr_NAME_value
* Purpose:      change dimension NAME attribute value to follow the netCDF-4 data model
*
* Return:       FAIL if failed, SUCCEED if successful.
*
* In :
*
                h5dset_id:         HDF5 dataset ID this attribute is attached.
*-------------------------------------------------------------------------
*/


int change_dim_attr_NAME_value(hid_t h5dset_id)
{

    hid_t tid = -1;
    hid_t sid = -1;
    hid_t aid = -1;

    /* Assign the attribute value for NAME to follow netCDF-4 data model. */
    char attr_value[] = "This is a netCDF dimension but not a netCDF variable.";


    /* Delete the original attribute. */
    if(H5Adelete(h5dset_id,"NAME") <0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME\n");
        return FAIL;
    }

    if((tid = H5Tcopy(H5T_C_S1)) <0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME\n");
        return FAIL;
    }

    if (H5Tset_size(tid, strlen(attr_value) + 1) <0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME\n");
        H5Tclose(tid);
        return FAIL;
    }

    if (H5Tset_strpad(tid, H5T_STR_NULLTERM) <0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME\n");
        H5Tclose(tid);
        return FAIL;
    }
    if((sid = H5Screate(H5S_SCALAR))<0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME\n");
        H5Tclose(tid);
        return FAIL;
    }

    /* Create and write a new attribute. */
    if ((aid = H5Acreate(h5dset_id, "NAME", tid, sid, H5P_DEFAULT, H5P_DEFAULT)) < 0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME");
        H5Tclose(tid);
        H5Sclose(sid);
        return FAIL;
    }

    if (H5Awrite(aid, tid, (void*)attr_value) <0)
    {
        FATAL_MSG("cannot delete HDF5 attribute NAME");
        H5Tclose(tid);
        H5Sclose(sid);
        H5Aclose(aid);
        return FAIL;
    }


    if (aid != -1)
        H5Aclose(aid);
    if (sid != -1)
        H5Sclose(sid);
    if (tid != -1)
        H5Tclose(tid);

    return SUCCEED;

}



/*
            copyDimension

    DESCRIPTION:
        This function copies the dimension scales from the HDF4 object to the HDF5 object.
        If the dimension scale does not yet exist in the HDF5 file, it is created. If it does exist, the
        HDF5 object will have its dimensions attached to the appropriate scale.

    ARGUMENTS:
        IN
            char* outDimName      -- Name of the output dimension scale. Datasets passed to this function should be
                                     given a unique dimension scale name in the case that their HDF4 dimensions have the
                                     same name (across multiple HDF4 files) but are not identical dimensions (different
                                     dimension size, different values inside the dimension). This string will be appended
                                     to the name of the input HDF4 dataset in the output HDF5 dataset. For instance, if
                                     the input HDF4 dimension is named:
                                        "inHDF4dimName"
                                     and outDimName is:
                                        "_g1"
                                     The name of the dimension in the HDF5 file will be:
                                        "inHDF4dimName_g1"

                                     Pass NULL in for this argument if a unique dimension is not required. If NULL is passed,
                                     this function will attempt to share the generated HDF5 dimension for all objects that
                                     reference it in the input HDF4 files.

                                     This string must be null terminated, or else a seg fault may occur.

            int32 h4fileID        -- The HDF4 file ID returned by SDstart
            char* h4datasetName   -- The name of the HDF4 dataset from which to copy from
            hid_t h5dimGroupID    -- The HDF5 group ID in which to store and/or find the dimension scales
            hid_t h5dsetID        -- The HDF5 dataset ID in which to attach dimension scales

    EFFECTS:
        Creates dimension scale if it doesn't exist under the h5dimGroupID group. Attaches the dimension scale
        to the h5dsetID dataset.

    RETURN:
        FAIL upon failure.
        SUCCEED upon success.

*/

herr_t copyDimension( char* outDimName, int32 h4fileID, char* h4datasetName, hid_t h5dimGroupID, hid_t h5dsetID )
{
    hsize_t tempInt = 0;
    char* correct_dsetname = NULL;
    char* dimName = NULL;
    herr_t errStatus = 0;
    int32 ntype = 0;
    int32 h4dsetID = 0;
    intn statusn = 0;
    int rank = 0;
    int status = 0;
    hid_t h5dimID = 0;
    void *dimBuffer = NULL;
    short fail = 0;
    hid_t memspace = 0;
    htri_t dsetExists = 0;
    short wasHardCodeCopy = 0;
    char* catString = NULL;
    char* temp = NULL;
    char tempStack[STR_LEN] = {'\0'};

    /* Get dataset index */
    int32 sds_index = SDnametoindex(h4fileID, h4datasetName );
    if ( sds_index == FAIL )
    {
        FATAL_MSG("Failed to get SD index.\n");
        goto cleanupFail;
    }
    /* select the dataset */
    h4dsetID = SDselect(h4fileID, sds_index);
    if ( h4dsetID == FAIL )
    {
        h4dsetID = 0;
        FATAL_MSG("Failed to select the HDF4 dataset.\n");
        goto cleanupFail;
    }

    /* get the rank of the dataset so we know how many dimensions to copy */
    statusn = SDgetinfo(h4dsetID, NULL, &rank, NULL, NULL, NULL );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to get SD info.\n");
        goto cleanupFail;
    }


    int32 dim_index;

    // COPY OVER DIMENSION SCALES TO HDF5 OBJECT
    dimName = calloc(500, 1);
    int32 size = 0;
    int32 num_attrs = 0;
    hid_t h5type = 0;
    int32 h4dimID = 0;

    for ( dim_index = 0; dim_index < rank; dim_index++ )
    {
        // Get the dimension ID
        h4dimID = SDgetdimid ( h4dsetID, dim_index );
        if ( h4dimID == FAIL )
        {
            FATAL_MSG("Failed to get dimension ID.\n");
            h4dimID = 0;
            goto cleanupFail;
        }


        /* get various useful info about this dimension */
        statusn = SDdiminfo( h4dimID, dimName, &size, &ntype, &num_attrs );
        //int32 dimRank = 0;
        //int32 dimSizes = 0;
        //statusn = SDgetinfo( h4dimID, dimName, &dimRank, &dimSizes, &ntype, &num_attrs);
        if ( statusn == FAIL )
        {
            FATAL_MSG("Failed to get dimension info.\n");
            goto cleanupFail;
        }


        /* If outDimName is provided, we need to append this string to the dimName variable.*/
       
        correct_dsetname = correct_name(dimName);

        if ( outDimName ) 
        {
            // store allocated memory in temp pointer
            strcpy(tempStack, dimName);
            temp = calloc ( sizeof(tempStack) + sizeof(outDimName) + 1, 1 );
            // copy dimName to the temp pointer memory
            strcpy(temp, tempStack);
            // concatenate dimName with outDimName
            strcat(temp,outDimName);
            // Fix the name to comply with netCDF standards
            catString = correct_name(temp);
            // free the temp memory (catString is separately allocated memory, malloc'ed in the correct_name function
            free(temp); temp = NULL;
        }
        else
        {
            catString = correct_name(dimName);
        }
        /* Since dimension scales are shared, it is possible this dimension already exists in HDF5 file (previous
           call to this function created it). Check to see if it does. If so, use the dimension that eixsts.
        */
        
        free(correct_dsetname); correct_dsetname = NULL;

        dsetExists = H5Lexists(h5dimGroupID, catString, H5P_DEFAULT);
        // if dsetExists is <= 0, then dimension does not yet exist.
        if ( dsetExists <= 0 )
        {
            /* If the dimension is one of the following, we will do an explicit dimension scale copy (hard code
             * the scale values)
             */
            if ( strstr( catString, "Band_250M" ) != NULL )
            {
                tempInt = 2;
                float floatBuffer[2] = {1.0f, 2.0f};
                h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, H5T_NATIVE_FLOAT, catString, floatBuffer);
                if ( h5dimID == EXIT_FAILURE )
                {
                    h5dimID = 0;
                    FATAL_MSG("Failed to insert dataset.\n");
                    goto cleanupFail;
                }
                /* The dimensions in this if else chain are pure dimensions in HDF4, but we want to convert them to a real
                 * dimension in the HDF5 file. Set this flag to 1 to indicate that the final result IS a real dimension.
                 * Flag will be used to prevent setting the NAME attribute downstream
                 */
                wasHardCodeCopy = 1;
            }
            else if ( strstr( catString, "Band_500M" ) != NULL )
            {
                tempInt = 5;
                float floatBuffer[5] = {3.0f, 4.0f, 5.0f, 6.0f, 7.0f};
                h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, H5T_NATIVE_FLOAT, catString, floatBuffer);
                if ( h5dimID == EXIT_FAILURE )
                {
                    h5dimID = 0;
                    FATAL_MSG("Failed to insert dataset.\n");
                    goto cleanupFail;
                }
                wasHardCodeCopy = 1;
            }
            else if ( strstr( catString, "Band_1KM_RefSB" ) != NULL )
            {
                float floatBuffer[15] = { 8.0f, 9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 13.5f, 14.0f, 14.5f, 15.0f, 16.0f, 17.0f, 18.0f, 19.0f, 26.0f };
                tempInt = 15;
                h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, H5T_NATIVE_FLOAT, catString, floatBuffer);
                if ( h5dimID == EXIT_FAILURE )
                {
                    h5dimID = 0;
                    FATAL_MSG("Failed to insert dataset.\n");
                    goto cleanupFail;
                }
                wasHardCodeCopy = 1;
            }
            else if ( strstr( catString, "Band_1KM_Emissive" ) != NULL )
            {
                float floatBuffer[16] = { 20.0f, 21.0f, 22.0f, 23.0f, 24.0f, 25.0f, 27.0f, 28.0f, 29.0f, 30.0f, 31.0f, 32.0f, 33.0f, 34.0f, 35.0f, 36.0f };
                tempInt = 16;
                h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, H5T_NATIVE_FLOAT, catString, floatBuffer);
                if ( h5dimID == EXIT_FAILURE )
                {
                    h5dimID = 0;
                    FATAL_MSG("Failed to insert dataset.\n");
                    goto cleanupFail;
                }
                wasHardCodeCopy = 1;
            }

            /* Else, the dimension is none of those listed. Do a normal copy by reading scale values from
               the input file (a non-hard coded copy)
             */
            else
            {
                wasHardCodeCopy = 0;
                /* SDdiminfo will return 0 for ntype if the dimension has no scale information. This is the case when
                   it is a pure dimension.  We need two separate cases for when it is and is not a pure dim
                */

                if ( ntype != 0 )
                {
                    /* get the correct HDF5 datatype from ntype */
                    status = h4type_to_h5type( ntype, &h5type );
                    if ( status != 0 )
                    {
                        FATAL_MSG("Failed to convert HDF4 to HDF5 datatype.\n");
                        goto cleanupFail;
                    }

                    /* read the dimension scale into a buffer */
                    dimBuffer = malloc(size);
                    //int32 start[1] = {0};
                    //int32 stride[1] = {1};
                    //statusn = SDreaddata( h4dimID, start, stride, &dimSizes, dimBuffer );
                    statusn = SDgetdimscale(h4dimID, dimBuffer);
                    if ( statusn != 0 )
                    {
                        FATAL_MSG("Failed to get dimension scale.\n");
                        goto cleanupFail;
                    }

                    /* make a new dataset for our dimension scale */

                    tempInt = size;
                    h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, h5type, catString, dimBuffer);
                    if ( h5dimID == EXIT_FAILURE )
                    {
                        h5dimID = 0;
                        FATAL_MSG("Failed to insert dataset.\n");
                        goto cleanupFail;
                    }

                    free(dimBuffer);
                    dimBuffer = NULL;
                }

                else
                {
                    hsize_t tempSize2 = 0;

                    tempSize2 = (hsize_t) size;
                    memspace = H5Screate_simple( 1, &tempSize2, NULL );

                    h5dimID = H5Dcreate( h5dimGroupID, catString, H5T_NATIVE_INT, memspace,
                                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
                    if ( h5dimID < 0 )
                    {
                        FATAL_MSG("Failed to create dataset.\n");
                        h5dimID = 0;
                        goto cleanupFail;
                    }

                    H5Sclose(memspace);
                    memspace = 0;

                } // end else
            } // end else

            errStatus = H5DSset_scale(h5dimID, catString);
            if ( errStatus != 0 )
            {
                FATAL_MSG("Failed to set dataset as a dimension scale.\n");
                goto cleanupFail;
            }


            /* Correct the NAME attribute of the pure dimension to conform with netCDF
               standards.
            */
            if ( ntype == 0 && !wasHardCodeCopy )
            {
                statusn = change_dim_attr_NAME_value(h5dimID);
                if ( statusn == FAIL )
                {
                    FATAL_MSG("Failed to change the NAME attribute of a dimension.\n");
                    goto cleanupFail;
                }
            }
        } // end if ( dsetExists <= 0 )

        else
        {
            h5dimID = H5Dopen2(h5dimGroupID, catString, H5P_DEFAULT);
            if ( h5dimID < 0 )
            {
                h5dimID = 0;
                FATAL_MSG("Failed to open dataset.\n");
                goto cleanupFail;
            }
        }


        errStatus = H5DSattach_scale(h5dsetID, h5dimID, dim_index);
        if ( errStatus != 0 )
        {
            FATAL_MSG("Failed to attach dimension scale.\n");
            goto cleanupFail;
        }

        free(correct_dsetname);
        correct_dsetname = NULL;
        H5Dclose(h5dimID);
        h5dimID = 0;
        free(catString); catString = NULL;
    }   // end for loop

    fail = 0;
    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( dimName ) free ( dimName );
    if ( h5dimID ) H5Dclose(h5dimID);
    if ( dimBuffer ) free(dimBuffer);
    if ( correct_dsetname ) free(correct_dsetname);
    if ( memspace ) H5Sclose(memspace);
    if ( catString ) free(catString);
    if ( temp ) free(temp);

    if ( fail ) return FAIL;

    return SUCCEED;

}
// Just see if it works
herr_t copyDimensionSubset( int32 h4fileID, char* h4datasetName, hid_t h5dimGroupID, hid_t h5dsetID,int32 s_size,char*fm_number_str, int gran_number)
{
    hsize_t tempInt = 0;
    char* dimName = NULL;
    herr_t errStatus = 0;
    int32 ntype = 0;
    int32 h4dsetID = 0;
    intn statusn = 0;
    int rank = 0;
    int status = 0;
    hid_t h5dimID = 0;
    void *dimBuffer = NULL;
    short fail = 0;
    hid_t memspace = 0;
    htri_t dsetExists = 0;
    short wasHardCodeCopy = 0;

    char* correct_dimName = NULL;
    char* ceres_sub_dim_name = NULL;
    char ceres_granule_suffix[3] = {'\0'};

    /* Get dataset index */
    int32 sds_index = SDnametoindex(h4fileID, h4datasetName );
    if ( sds_index == FAIL )
    {
        FATAL_MSG("Failed to get SD index.\n");
        goto cleanupFail;
    }
    /* select the dataset */
    h4dsetID = SDselect(h4fileID, sds_index);
    if ( h4dsetID == FAIL )
    {
        h4dsetID = 0;
        FATAL_MSG("Failed to select the HDF4 dataset.\n");
        goto cleanupFail;
    }

    /* get the rank of the dataset so we know how many dimensions to copy */
    statusn = SDgetinfo(h4dsetID, NULL, &rank, NULL, NULL, NULL );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to get SD info.\n");
        goto cleanupFail;
    }


    int32 dim_index;

    // COPY OVER DIMENSION SCALES TO HDF5 OBJECT
    dimName = calloc(500, 1);
    int32 size = 0;
    int32 num_attrs = 0;
    hid_t h5type = 0;
    int32 h4dimID = 0;

    for ( dim_index = 0; dim_index < rank; dim_index++ )
    {
        // Get the dimension ID
        h4dimID = SDgetdimid ( h4dsetID, dim_index );
        if ( h4dimID == FAIL )
        {
            FATAL_MSG("Failed to get dimension ID.\n");
            h4dimID = 0;
            goto cleanupFail;
        }

        /* get various useful info about this dimension */
        statusn = SDdiminfo( h4dimID, dimName, &size, &ntype, &num_attrs );
        //int32 dimRank = 0;
        //int32 dimSizes = 0;
        //statusn = SDgetinfo( h4dimID, dimName, &dimRank, &dimSizes, &ntype, &num_attrs);
        if ( statusn == FAIL )
        {
            FATAL_MSG("Failed to get dimension info.\n");
            goto cleanupFail;
        }

        size = s_size;
        /* Since dimension scales are shared, it is possible this dimension already exists in HDF5 file (previous
           call to this function created it). Check to see if it does. If so, use the dimension that eixsts.
        */

        correct_dimName = correct_name(dimName);
        // Now we need to add the granule number for this dimenson since each granule will be different.
        memset(ceres_granule_suffix,0,3);
        sprintf(ceres_granule_suffix,"%d",gran_number);
        ceres_sub_dim_name=malloc(strlen(correct_dimName)+strlen(fm_number_str)+strlen(ceres_granule_suffix)+1);
        memset(ceres_sub_dim_name,0,strlen(correct_dimName)+strlen(fm_number_str)+strlen(ceres_granule_suffix)+1);

        strncpy(ceres_sub_dim_name,correct_dimName,strlen(correct_dimName));
        strncat(ceres_sub_dim_name,fm_number_str,strlen(fm_number_str));
        strncat(ceres_sub_dim_name,ceres_granule_suffix,strlen(ceres_granule_suffix));

        //dsetExists = H5Lexists(h5dimGroupID, correct_dsetname, H5P_DEFAULT);
        dsetExists = H5Lexists(h5dimGroupID, ceres_sub_dim_name, H5P_DEFAULT);
        // if dsetExists is <= 0, then dimension does not yet exist.
        if ( dsetExists <= 0 )
        {
            /* If the dimension is one of the following, we will do an explicit dimension scale copy (hard code
             * the scale values)
             */
            {
                wasHardCodeCopy = 0;
                /* SDdiminfo will return 0 for ntype if the dimension has no scale information. This is the case when
                   it is a pure dimension.  We need two separate cases for when it is and is not a pure dim
                */

                if ( ntype != 0 )
                {
                    /* get the correct HDF5 datatype from ntype */
                    status = h4type_to_h5type( ntype, &h5type );
                    if ( status != 0 )
                    {
                        FATAL_MSG("Failed to convert HDF4 to HDF5 datatype.\n");
                        goto cleanupFail;
                    }

                    /* read the dimension scale into a buffer */
                    dimBuffer = malloc(size);
                    //int32 start[1] = {0};
                    //int32 stride[1] = {1};
                    //statusn = SDreaddata( h4dimID, start, stride, &dimSizes, dimBuffer );
                    statusn = SDgetdimscale(h4dimID, dimBuffer);
                    if ( statusn != 0 )
                    {
                        FATAL_MSG("Failed to get dimension scale.\n");
                        goto cleanupFail;
                    }

                    /* make a new dataset for our dimension scale */

                    tempInt = size;
                    h5dimID = insertDataset(&outputFile, &h5dimGroupID, 1, 1, &tempInt, h5type, ceres_sub_dim_name, dimBuffer);
                    if ( h5dimID == EXIT_FAILURE )
                    {
                        h5dimID = 0;
                        FATAL_MSG("Failed to insert dataset.\n");
                        goto cleanupFail;
                    }

                    free(dimBuffer);
                    dimBuffer = NULL;
                }

                else
                {
                    hsize_t tempSize2 = 0;

                    tempSize2 = (hsize_t) size;
                    memspace = H5Screate_simple( 1, &tempSize2, NULL );

                    h5dimID = H5Dcreate( h5dimGroupID, ceres_sub_dim_name, H5T_NATIVE_INT, memspace,
                                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
                    if ( h5dimID < 0 )
                    {
                        FATAL_MSG("Failed to create dataset.\n");
                        h5dimID = 0;
                        goto cleanupFail;
                    }

                    H5Sclose(memspace);
                    memspace = 0;

                } // end else
            }

            errStatus = H5DSset_scale(h5dimID, ceres_sub_dim_name);
            if ( errStatus != 0 )
            {
                FATAL_MSG("Failed to set dataset as a dimension scale.\n");
                goto cleanupFail;
            }


            /* Correct the NAME attribute of the pure dimension to conform with netCDF
               standards.
            */
            if ( ntype == 0 && !wasHardCodeCopy )
            {
                statusn = change_dim_attr_NAME_value(h5dimID);
                if ( statusn == FAIL )
                {
                    FATAL_MSG("Failed to change the NAME attribute of a dimension.\n");
                    goto cleanupFail;
                }
            }
        } // end if ( dsetExists <= 0 )

        else
        {
            h5dimID = H5Dopen2(h5dimGroupID, ceres_sub_dim_name, H5P_DEFAULT);
            if ( h5dimID < 0 )
            {
                h5dimID = 0;
                FATAL_MSG("Failed to open dataset.\n");
                goto cleanupFail;
            }
        }


        errStatus = H5DSattach_scale(h5dsetID, h5dimID, dim_index);
        if ( errStatus != 0 )
        {
            FATAL_MSG("Failed to attach dimension scale.\n");
            goto cleanupFail;
        }

        free(correct_dimName);
        correct_dimName = NULL;
        free(ceres_sub_dim_name);
        ceres_sub_dim_name = NULL;
        H5Dclose(h5dimID);
        h5dimID = 0;
    }   // end for loop

    fail = 0;
    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    // The following free, espeically free(dimName) may be an issue.
    if ( dimName ) free ( dimName );
    if ( h5dimID ) H5Dclose(h5dimID);
    if ( dimBuffer ) free(dimBuffer);
    if ( correct_dimName ) free(correct_dimName);
    if ( memspace ) H5Sclose(memspace);
    if ( fail ) return FAIL;

    return SUCCEED;

}

herr_t attachDimension(hid_t fileID,char*dimname, hid_t dsetID,int dim_index)
{

    hid_t h5dimID = H5Dopen2(fileID, dimname, H5P_DEFAULT);
    if ( h5dimID < 0 )
    {
        FATAL_MSG("Failed to open dataset.\n");
        return FAIL;
    }

    if(H5DSattach_scale(dsetID,h5dimID,dim_index))
    {
        FATAL_MSG("Failed to attach the dimension scale.\n");
        H5Dclose(h5dimID);
        return FAIL;
    }
    H5Dclose(h5dimID);
    return SUCCEED;
}

size_t obtainDimSize(hid_t dsetID)
{

    hid_t dspace = H5Dget_space(dsetID);
    size_t ret_size =(size_t)H5Sget_simple_extent_npoints(dspace);
    H5Sclose(dspace);
    return ret_size;

}

herr_t Generate2D_Dataset(hid_t h5_group,char* dsetname,hid_t h5_type,void* databuffer,hid_t dim0_id,hid_t dim1_id,size_t dim0_size,size_t dim1_size)
{
    hsize_t temp[2];
    temp[0] = (hsize_t) (dim0_size);
    temp[1] = (hsize_t) (dim1_size);
    hid_t dummy_output_file_id, datasetID;

    datasetID = insertDataset( &dummy_output_file_id, &h5_group, 1, 2,
                               temp, h5_type, dsetname, databuffer );

    if ( datasetID == EXIT_FAILURE )
    {
         FATAL_MSG("/gError writing %s dataset.\n", dsetname );
        return -1;
    }

    if(H5DSattach_scale(datasetID,dim0_id,0))
    {
        FATAL_MSG("Failed to attach the dimension scale.\n");
        return FAIL;
    }

    if(H5DSattach_scale(datasetID,dim1_id,1))
    {
        FATAL_MSG("Failed to attach the dimension scale.\n");
        return FAIL;
    }

    H5Dclose(datasetID);

    return SUCCEED;

}


/*
            MOPITTaddDimension

    DESCRIPTION:
        This function adds a dimension scale to the output HDF5 file. It writes the data stored in scaleBuffer to the dimension.
        The dimension is created under the HDF5 group/file ID given by h5dimGroupID. It does not attach the dimension to any
        dataset.

    EFFECTS:
        A new dimension scale is added to the output HDF5 file.

        IT IS THE DUTY OF THE CALLER to close the returned identifier with H5Dclose() once finished.

    ARGUMENTS:
        INPUT
            1. hid_t h5dimGroupID      -- The file or group identifier under which to store the dimension
            2. const char* dimName     -- The name to be given to the dimension
            3. hsize_t dimSize         -- The integer size of the dimension
            4. const void* scaleBuffer -- The data to be written to the scale
            5. hid_t dimScaleNumType   -- The HDF5 number type of scaleBuffer

    RETURN:
        Returns the identifier of the dimension upon success.
        Else, it returns FAIL.

*/

hid_t MOPITTaddDimension ( hid_t h5dimGroupID, const char* dimName, hsize_t dimSize, const void* scaleBuffer, hid_t dimScaleNumType )
{
    hid_t dsetID = 0;
    herr_t status = 0;

    dsetID = insertDataset ( &outputFile, &h5dimGroupID, 1, 1, &dimSize, dimScaleNumType, dimName, scaleBuffer);
    if ( dsetID == EXIT_FAILURE )
    {
        dsetID = 0;
        FATAL_MSG("Failed to insert dataset.\n");
        return FAIL;
    }

    status = H5DSset_scale( dsetID, dimName );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set dataset as a dimension scale.\n");
        return FAIL;
    }

    return dsetID;

}

/*
        initializeTimeOffset

    DESCRIPTION:
        Initializes the array for TAI93 to UTC conversion. This array contains double precision values
        (which are actually all integer values) that denote the number of seconds to offset a TAI93 value to
        UTC time. The index of the array represents days since Jan 1 1993 00:00:00.

        This function must be periodically updated to contain new leap second information as it arrives, otherwise
        the array may not be able to account for all TAI93 times.

        UTC time is an offset version of Atomic Time. The amount of offset from TAI is periodically incremented or decremented
        depending on the amount of fluctuation in the Earth's orbit and rotation so that the times remain roughly congruent
        with solar time. These offsets are not applied according to a formula, but rather by the current astronomical conditions.
        Therefore it is impossible to predict future offset values -- this function is only valid for previous dates and
        DOES NOT ACCOUNT FOR FUTURE OFFSETS. This implies that this function must be periodically updated to contain the new
        offset information.

        TAI93 time is defined to be equal to 0 on Jan 1, 1993 00:00:00.

        Directions to update this function:
            To add additional offsets to the TAI93toUTCoffset, one needs to inquire the exact dates the offsets were added.
            Convert these dates to "days since epoch" (epoch being Jan 1, 1993), being careful to account for leap years.
            You can use the fact that at the end of June 30th 2017, 8946 full days have passed since epoch. Then:

            1. Update NUM_DAYS to be equal to the number of days since epoch since the last known offset was added.
            2. Add the additional necessary for loops to initialize the TAI93toUTCoffset with the RUNNING TOTAL offset

            Note that offsets are applied either at the end of June 30th and/or December 31st. i.e. The offsets are applied
            not during the month of June or December, but AT THE END.

    ARGUMENTS:
        None

    EFFECTS:
        Updates the global variable TAI93toUTCoffset to point to the allocated memory on the heap containing the proper
        offset values.

        IT IS THE DUTY OF THE CALLER to free the TAI93toUTCoffset array when the offset values are no longer needed.
        The caller must also set TAI93toUTCoffset to NULL once freed to prevent main.c from erroneously freeing it
        again.

    RETURN:
        EXIT_FAILURE
        EXIT_SUCCESS

*/

double* TAI93toUTCoffset = NULL;
#define NUM_DAYS 8946

herr_t initializeTimeOffset()
{
    if ( TAI93toUTCoffset != NULL )
    {
        FATAL_MSG("Failed to allocate memory for time TAI93-UTC offset array.\n\tEither the array already points to valid memory or the\n\tarray was freed and not set back to NULL.\n");
    }

    TAI93toUTCoffset = malloc(sizeof(double) * NUM_DAYS) ;
    if ( TAI93toUTCoffset == NULL )
    {
        FATAL_MSG("Failed to allocate memory for time offset array.\n");
        return EXIT_FAILURE;
    }

    /* Initialize the offset Array */
    for ( int i = 0; i <= 181; i++ )            // Jan 1993 -> June 1993
        TAI93toUTCoffset[i] = 0.0;
    for ( int i = 182; i <= 546; i++ )          // July 1993 -> June 1994
        TAI93toUTCoffset[i] = -1.0;
    for ( int i = 547; i <= 1095; i++ )         // July 1994 -> Dec 1995
        TAI93toUTCoffset[i] = -2.0;
    for ( int i = 1096; i <= 1642; i++ )        // Jan 1996 -> June 1997
        TAI93toUTCoffset[i] = -3.0;
    for ( int i = 1643; i <= 2191; i++ )        // July 1997 -> Dec 1998
        TAI93toUTCoffset[i] = -4.0;
    for ( int i = 2192; i <= 4748; i++ )        // Jan 1999 -> Dec 2005
        TAI93toUTCoffset[i] = -5.0;
    for ( int i = 4749; i <= 5845; i++ )        // Jan 2006 -> Dec 2008
        TAI93toUTCoffset[i] = -6.0;
    for ( int i = 5846; i <= 7210; i++ )        // Jan 2009 -> June 2012
        TAI93toUTCoffset[i] = -7.0;
    for ( int i = 7211; i <= 8215; i++ )        // July 2012 -> June 2015
        TAI93toUTCoffset[i] = -8.0;
    for ( int i = 8216; i <= 8767; i++ )        // July 2015 -> Dec 2016
        TAI93toUTCoffset[i] = -9.0;
    for ( int i = 8768; i < NUM_DAYS; i++ )    // Jan 2017 -> June 2017
        TAI93toUTCoffset[i] = -10.0;                  // Currently, value not known past June 30th 2017

    return EXIT_SUCCESS;
}

/*
            TAItoUTCconvert

    DESCRIPTION:
        This function converts an array with TAI93 times (International Atomic Time - 1993) to UTC (Universal Coordinated Time).
        A single dimensional array of double values must be passed to the function along with the number of elements in the array.
        The function will modify the array to contain the converted UTC times.


    EFFECTS:
        Modifies array passed to it with the converted UTC values. Temporarily allocates offset array on heap, frees before
        return.

    INPUTS:
        IN
            unsigned int size       -- Number of elements in buffer.
        IN/OUT
            double* buffer          -- One dimensional array containing the TAI93 values (units given in seconds)

    RETURN:
        Returns FAIL upon failure. Otherwise, SUCCEED.

*/


herr_t TAItoUTCconvert ( double* buffer, unsigned int size )
{
    /* this function assumes that buffer is one dimensional */
    herr_t status = 0;

    if ( buffer == NULL )
    {
        FATAL_MSG("Argument cannot be NULL.\n");
        return FAIL;
    }

    if ( TAI93toUTCoffset == NULL )
        status = initializeTimeOffset();

    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to initialize TAI to UTC time offset.\n");
        return FAIL;
    }

    int daysSinceEpoch = 0;

    for ( int i = 0; i < size; i++ )
    {
        /* Divide buffer value by 86400 (number of seconds in day) to get a floating point
           number for the day, then truncate this value to get daysSinceEpoch.
        */

        daysSinceEpoch = (int) ( buffer[i] / 86400.0 );                 // Casting to int will truncate the float (acts as the floor function)

        if ( daysSinceEpoch < NUM_DAYS && daysSinceEpoch >= 0 )
        {
            buffer[i] += TAI93toUTCoffset[daysSinceEpoch];
        }
        else if ( daysSinceEpoch >= NUM_DAYS )                           // Currently, value not known past June 30th 2017
        {
            FATAL_MSG("MOPITT time values converted\n\tusing out of date UTC-TAI93 offset.\n\tConverted time values may be incorrect!\n\tPlease update the offset array in the function \"initializeTimeOffset()\".\n" );
            free(TAI93toUTCoffset);
            TAI93toUTCoffset = NULL;
            return FAIL;

        }
        else                        // something bad happened
        {
            FATAL_MSG("Failed to convert MOPITT time values to UTC.\n");
            free(TAI93toUTCoffset);
            TAI93toUTCoffset = NULL;
            return FAIL;
        }
    }

    return SUCCEED;
}

/*
        H5allocateMemDouble

    DESCRIPTION:
        This function allocates enough memory to store the dataset give by datasetPath.

    ARGUMENTS:
        IN
            hid_t inputFile   -- The HDF5 input group/file identifier
            char* datasetPath -- The path of the dataset relative to inputFile
        OUT
            void** buffer     -- The buffer in which the data will be stored. Does not need to be pre-allocated.
            long int* size    -- Returns the number of elements in the allocated array. Can be set to NULL if don't care.

    EFFECTS:
        Sets the caller's buffer pointer (of the appropriate pointer type) to point to the allocated memory
        on the heap.

        IT IS THE DUTY OF THE CALLER to release buffer to avoid memory leaks.

    RETURN:
        EXIT_FAILURE
        EXIT_SUCCESS
*/

herr_t H5allocateMemDouble ( hid_t inputFile, const char* datasetPath, void** buffer, long int* size )
{
    if ( datasetPath == NULL || buffer == NULL )
    {
        FATAL_MSG("Invalid arguments passed to function. Pointers cannot be NULL.\n");
        return FAIL;
    }
    hid_t dataset = 0;
    hid_t dataspace = 0;

    herr_t statusn;
    unsigned short fail = 0;
    hsize_t * datasetDims = NULL;
    int rank = 0;


    dataset = H5Dopen2( inputFile, datasetPath, H5F_ACC_RDONLY );
    if ( dataset < 0 )
    {
        FATAL_MSG("Failed to read HDF5 dataset.\n");
        dataset = 0;
        goto cleanupFail;
    }

    dataspace = H5Dget_space(dataset);
    if ( dataspace < 0 )
    {
        dataspace = 0;
        FATAL_MSG("Failed to get dataspace.\n");
        goto cleanupFail;
    }

    rank = H5Sget_simple_extent_ndims(dataspace);
    if ( rank < 0 )
    {
        FATAL_MSG("Failed to get rank of dataset.\n");
        goto cleanupFail;
    }

    datasetDims = malloc( sizeof(hsize_t) * DIM_MAX );
    if ( datasetDims == NULL )
    {
        FATAL_MSG("Malloc failed to allocate memory.\n");
        goto cleanupFail;
    }

    for ( int i = 0; i < DIM_MAX; i++ )
        datasetDims[i] = 0;

    statusn = H5Sget_simple_extent_dims(dataspace, datasetDims, NULL);
    if ( statusn < 0 )
    {
        FATAL_MSG("Unable to get dimension size.\n");
        goto cleanupFail;
    }

    unsigned long long numElems = 0;

    for ( int i = 0; i < DIM_MAX; i++ )
        numElems += datasetDims[i];

    *buffer = malloc( sizeof(double) * numElems );

    if ( size ) *size = numElems;

    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( dataset ) H5Dclose(dataset);
    if ( dataspace ) H5Sclose(dataspace);
    if ( datasetDims ) free(datasetDims);
    if ( fail ) return EXIT_FAILURE;

    return EXIT_SUCCESS;
}

/*
        getTAI93

    DESCRIPTION:
        This function takes as input the GDateInfo_t struct (as defined in libTERRA.h) which contains date information,
        and returns the corresponding TAI93 (defined as TAI = 0 on Jan 1 1993 00:00:00) timestamp.

        This function will need to be periodically updated if being used after the year 2035 to contain more running-total
        leap year information (as shown in the if-else tree).

    ARGUMENTS:
        IN
            1. GDateInfo_t date       -- The input date information.
        OUT
            2. double* TAI93timestamp -- The corresponding TAI93 time

    EFFECTS:
        Modifies the memory pointed to by TAI93timestamp

    RETURN:
        EXIT_FAILURE
        EXIT_SUCCESS

*/
herr_t getTAI93 ( GDateInfo_t date, double* TAI93timestamp )
{
    if ( date.year < 1993 )
    {
        FATAL_MSG("Cannot convert to TAI93 with dates before the year 1993.\n");
        return EXIT_FAILURE;
    }
    if ( TAI93timestamp == NULL )
    {
        FATAL_MSG("TAI93timestamp argument cannot be NULL.\n");
        return EXIT_FAILURE;
    }

    double TAI93 = 0.0;

    TAI93 += ( date.year-1993 );         // How many full years since 1993, multiplied by number of seconds in a year
    TAI93 *= 365.0;
    TAI93 *= 86400.0;
    // Find how many leap years since 1993
    /* Note that the current year is not considered in this else if tree whether or not it is a leap year. That is handled downstream */
    unsigned short numLeapYears = 0;
    if ( date.year >= 1993 && date.year <= 1995 )
        numLeapYears = 0;
    else if ( date.year >= 1996 && date.year <= 2000 )
        numLeapYears = 1;
    else if ( date.year >= 2001 && date.year <= 2004 )
        numLeapYears = 2;
    else if ( date.year >= 2005 && date.year <= 2008 )
        numLeapYears = 3;
    else if ( date.year >= 2009 && date.year <= 2012 )
        numLeapYears = 4;
    else if ( date.year >= 2013 && date.year <= 2016 )
        numLeapYears = 5;
    else if ( date.year >= 2017 && date.year <= 2020 )
        numLeapYears = 6;
    else if ( date.year >= 2021 && date.year <= 2024 )
        numLeapYears = 7;
    else if ( date.year >= 2025 && date.year <= 2028 )
        numLeapYears = 8;
    else if ( date.year >= 2029 && date.year <= 2032 )
        numLeapYears = 9;
    else if ( date.year >= 2033 && date.year <= 2036 )
        numLeapYears = 10;
    else
    {
        FATAL_MSG("Failed to find how many leap years since 1993. Maybe this function is\n\tout of date?\n");
        return EXIT_FAILURE;
    }


    TAI93 += (double)(numLeapYears * 86400);        // Account for additional days during leap years

    int leapYear = isLeapYear( (int) date.year);
    if ( leapYear < 0 )
    {
        FATAL_MSG("Failed to determine if it is a leap year.\n");
        return EXIT_FAILURE;
    }
    // Account for the number of seconds in a month
    // Note that we increment TAI93 based on how many FULL months have passed.

    const unsigned short month = (unsigned short) date.month;

    switch ( month )
    {
    case 1:              // Current month is Jan
        TAI93 += 0.0;
        break;
    case 2:              // Feb
        TAI93 += (double) (31 * 86400);
        break;
    case 3:             // Current month is March (previous month is Feb which may or may not have leap day)
        TAI93 += (double)((31+28)*86400);
        break;
    case 4:             // April
        TAI93 += (double) ((31+28+31)*86400);
        break;
    case 5:             // May
        TAI93 += (double) ((31+28+31+30)*86400);
        break;
    case 6:             // June
        TAI93 += (double) ((31+28+31+30+31)*86400);
        break;
    case 7:             // July
        TAI93 += (double) ((31+28+31+30+31+30)*86400);
        break;
    case 8:             // Aug
        TAI93 += (double) ((31+28+31+30+31+30+31)*86400);
        break;
    case 9:             // Sept
        TAI93 += (double) ((31+28+31+30+31+30+31+31)*86400);
        break;
    case 10:            // Oct
        TAI93 += (double) ((31+28+31+30+31+30+31+31+30)*86400);
        break;
    case 11:            // Nov
        TAI93 += (double) ((31+28+31+30+31+30+31+31+30+31)*86400);
        break;
    case 12:            // Dec
        TAI93 += (double) ((31+28+31+30+31+30+31+31+30+31+30)*86400);
        break;
    default:
        FATAL_MSG("Problem occurred trying to convert date to TAI93 time.\n");
        return EXIT_FAILURE;
    }

    // Add extra day if leap year and month is March or after
    if ( leapYear && month >= 3)
    {
        TAI93 += 86400.0;
    }

    // Account for the number of FULL days that have passed in this month
    const short days = (short) date.day - 1;        // date.day is 1-indexed
    if ( days >= 0 && days <= 31 )
        TAI93 += (double) ((days) * 86400);          // Number of full days passed multiplied by number of seconds in a day
    else
    {
        FATAL_MSG("Failed to account for the number of days passed in the current month.\n");
        return EXIT_FAILURE;
    }

    // Account for the number of FULL hours that have passed in this day

    const short hours = (short) date.hour;          // hour is 0-indexed
    if ( hours >= 0 && hours <= 23 )
        TAI93 += (double)(hours * 3600);        // Number of full hours passed multiplied by number of seconds in an hour
    else
    {
        FATAL_MSG("Failed to acount for the number of hours passed in a day.\n");
        return EXIT_FAILURE;
    }

    // Account for the number of FULL minutes that have passed in this hour
    const short minutes = (short) date.minute;      // Minutes are 0-indexed
    if ( minutes >= 0 && minutes <= 60 )            // From 0 to 60 because leap seconds may make minute indicator show 60
        TAI93 += (double)(minutes * 60);
    else
    {
        FATAL_MSG("Failed to account for number of minutes passed.\n");
        return EXIT_FAILURE;
    }

    // Finally, increment the seconds
    TAI93 += date.second;


    *TAI93timestamp = TAI93;

    return EXIT_SUCCESS;
}

/*
 * isLeapyear
 * DESCRIPTION:
 *      Determins if the given year is a leap year.
 * ARGUMENTS:
 *      IN
 *      int year
 * RETURN:
 *      -1 upon failure
 *      0 if not a leap year.
 *      1 if it is a leap year.
 */
int isLeapYear( int year )
{
    if ( year < 0 )
    {
        FATAL_MSG("I highly doubt there were satellites flying about before the year 0.\n");
        return -1;
    }
    int retVal = 0;

    if ( year % 4 == 0 )           // divisible by 4
    {
        if ( year % 100 == 0 )     // divisible by 100
        {
            if ( year % 400 == 0 )
                retVal = 1;
            else                        // not divisible by 400
                retVal = 0;
        }

        else                            // not divisible by 100
            retVal = 1;
    }
    else                                // not divisible by 4
        retVal = 0;

    return retVal;
}

/*
        binarySearchDouble

    DESCRIPTION:
        This function performs a binary search on a sorted array of double values.
        If the exact target is not found, the first element that is greater than the target value is returned
        in the argument targetIndex.

    ARGUMENTS:
        IN
            1. double* array                   -- The array to perform the search on
            2. double target                   -- The value to search for
            3. hsize_t size                    -- The number of elements in array
            4. short unsigned int firstGreater -- Set to nonzero to return the first element GREATER than target (if exact match not found).
                                                  Set to 0 to return element BEFORE the first element that is greater than target.
        OUT
            5. long int* targetIndex -- The index of where the target value is. If target doesn't exist in the array,
                                        this will return the first element in array that is larger than target.

    EFFECTS:
        Modifies the memory pointed to by targetIndex to contain the selected index of the array.

    RETURN:
        0 if an exact match is found.
        1 if an exact match was not found (first element larger than target is returned).
       -1 if an error occured.

*/
herr_t binarySearchDouble ( const double* array, double target, hsize_t size, short unsigned int firstGreater, long int* targetIndex )
{
    long int left = 0;
    long int right = (long int)(size - 1);
    long int middle;

    while ( left <= right )
    {
        middle = (left + right)/2;
        if ( array[middle] < target )
        {
            left = middle+1;
            /* If the search was unable to find an exact match (case where left > right), set middle to the first index that is greater than target.
               This is useful information for subsetting. It is possible that middle will be set to the very beginning or to
               the very end of the array. This is okay, this is also useful information.
             */

            if ( left > right )
            {
                while ( middle+1 < size && array[middle] < target )
                    middle++;
            }
            continue;
        }

        if ( array[middle] > target )
        {
            right = middle - 1;
            /* If the search was unable to find an exact match, set middle to the first index that is greater than target.
               This is useful information for subsetting. It is possible that middle will be set to the very beginning or to
               the very end of the array. This is okay, this is also useful information.
             */

            if ( left > right )
            {
                while ( middle-1 >= 0 && array[middle-1] > target )
                    middle--;
            }
            continue;
        }

        *targetIndex = middle;
        return 0;
    }

    /* Final error check to make sure that array[middle] either equals target or is greater than target */
    if ( middle < 0 || middle >= size || (array[middle] < target && middle != size-1))
    {
        FATAL_MSG("Binary search produced an invalid result. Debug information:\n\ttargetIndex = %ld\n\tsize = %u\n", middle, (unsigned int) size);
        return -1;
    }

    if ( firstGreater == 0 && middle > 0 )
    {
        middle--;
    }

    *targetIndex = middle;
    return 1;

}

/*
            MOPITT_OrbitInfo

    DESCRIPTION:
        This function takes as input the starting and ending times of the orbit (given as OInfo_t struct), the input hid_t
        HDF5 file/group identifier, the path to the time dataset (relative to inputFile), and gives the starting and ending
        indices.

    ARGUMENTS:
        IN
            1. const hid_t inputFile  -- The HDF5 file or group identifier under which the time dataset is stored.
            2. OInfo_t cur_orbit_info -- A struct containing the start and end dates of the orbit. Defined in libTERRA.h
            3. const char* timePath   -- The path to the time dataset relative to inputFile
        OUT
            4. unsigned int* start_indx_ptr    -- The index to begin subsetting from
            5. unsigned int* end_indx_ptr      -- The index to end the subsetting at

    EFFECTS:
        Modifies start and end_indx_ptr to contain the appropriate values

    RETURN:
        0 if success.
        1 if MOPITT file should be skipped (no error, but shouldn't include MOPITT file in output).
        2 if other failure.

*/

herr_t MOPITT_OrbitInfo( const hid_t inputFile, OInfo_t cur_orbit_info, const char* timePath, unsigned int* start_indx_ptr,
                         unsigned int* end_indx_ptr )
{
    herr_t retStatus = 0;
    double* timeData = NULL;
    long int numElems = 0;
    herr_t status = 0;
    hid_t dataset = 0;
    hid_t dataspace = 0;
    hid_t groupID = 0;

    status = H5allocateMemDouble ( inputFile, timePath, (void**) &timeData, &numElems );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        timeData = NULL;
        goto cleanupFail;
    }


    status = H5LTread_dataset_double( inputFile, timePath, timeData);
    if ( status < 0 )
    {
        FATAL_MSG("Failed to read dataset.\n");
        goto cleanupFail;
    }

    /* We now need to convert the orbit info given by current_orbit_info into TAI93 start time and TAI93 end time.
       This way, we will know exactly which elements in timeData will be selected for the start and end pointers.
       We convert to TAI93 because the MOPITT time values are given in TAI93.
     */
    GDateInfo_t time;
    double startTAI93;
    double endTAI93;

    time.year = cur_orbit_info.start_year;
    time.month = cur_orbit_info.start_month;
    time.day = cur_orbit_info.start_day;
    time.hour = cur_orbit_info.start_hour;
    time.minute = cur_orbit_info.start_minute;
    time.second = (double) cur_orbit_info.start_second;

    status = getTAI93 ( time, &startTAI93 );
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get TAI93 timestamp.\n");
        goto cleanupFail;
    }

    time.year = cur_orbit_info.end_year;
    time.month = cur_orbit_info.end_month;
    time.day = cur_orbit_info.end_day;
    time.hour = cur_orbit_info.end_hour;
    time.minute = cur_orbit_info.end_minute;
    time.second = (double) cur_orbit_info.end_second;


    status = getTAI93 ( time, &endTAI93 );
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get TAI93 timestamp.\n");
        goto cleanupFail;
    }



    /* Perform a binary search on the timeData array to find the start and end indices */
    long int startIdx, endIdx;

    status = binarySearchDouble ( timeData, startTAI93, (hsize_t) numElems, 1, &startIdx );
    if ( status == -1 )
    {
        FATAL_MSG("Failed to find MOPITT start index.\n");
        goto cleanupFail;
    }
    status = binarySearchDouble ( timeData, endTAI93, (hsize_t) numElems, 0, &endIdx );
    if ( status == -1 )
    {
        FATAL_MSG("Failed to find MOPITT end index.\n");
        goto cleanupFail;
    }

    /* If the start and end indices are equal, this file is probably out of bounds of the orbit. */
    if ( startIdx == endIdx )
    {
        retStatus = 1;
    }

    *start_indx_ptr = startIdx;
    *end_indx_ptr = endIdx;

    if ( 0 )
    {
cleanupFail:
        retStatus = 2;
    }

#if 0

    /* This is for debugging only */
    printf("\nstartYear = %u\nstartMonth = %u\nstartDay = %u\nstartHour = %u\nstartMinute = %u\nstartSecond = %lf\n", time.year, (unsigned short) time.month, (unsigned short) time.day, (unsigned short) time.hour, (unsigned short) time.minute, time.second);
    printf("\nendYear = %u\nendMonth = %u\nendDay = %u\nendHour = %u\nendMinute = %u\nendSecond = %lf\n", time.year, (unsigned short) time.month, (unsigned short) time.day, (unsigned short) time.hour, (unsigned short) time.minute, time.second);
    printf("startTAI93: %lf endTAI93: %lf\n", startTAI93, endTAI93);


#endif





    if(timeData) free(timeData);
    if (dataset) H5Dclose(dataset);
    if (dataspace) H5Sclose(dataspace);
    if ( groupID ) H5Gclose(groupID);

    return retStatus;
}


int comp_greg(GDateInfo_t j1, GDateInfo_t j2)
{

#define cmp_number(a,b) (((a) > (b)) ? 1 : (((a) == (b)) ? 0 : -1))

    int cmp_year = cmp_number(j1.year,j2.year);
    if(cmp_year == 0)
    {
        int cmp_month = cmp_number(j1.month,j2.month);
        if(cmp_month == 0)
        {
            int cmp_day = cmp_number(j1.day,j2.day);
            if(cmp_day == 0)
            {
                int cmp_hour = cmp_number(j1.hour,j2.hour);
                if(cmp_hour == 0)
                {
                    int cmp_minute = cmp_number(j1.minute,j2.minute);
                    if(cmp_minute == 0)
                    {
                        int cmp_second = cmp_number(j1.second,j2.second);
                        return cmp_second;
                    }
                    else
                        return cmp_minute;
                }
                else
                    return cmp_hour;
            }
            else
                return cmp_day;
        }
        else
            return cmp_month;
    }
    else
        return cmp_year;

}

/*
        get_greg

    DESCRIPTION:
        get_greg converts the julian time given in the argument to a Gregorian calendar
        date. The code can be seen in two separate parts: the section that converts the
        date itself, and the code that converts the Gregorian time. The date portion of
        the code was ported over by Shashank Bansal from a Python implementation. The
        original Python can be found here:

        https://gist.github.com/jiffyclub/1294443

    ARGUMENTS:
        double julian       -- The Julian value to be converted
        int* yearp          -- Where the converted year will go
        int* monthp         -- Where the converted month will go
        int* dayp           -- Where the converted day will go
        int* hourp          -- Where the converted hour will go
        int* mmp            -- Where the converted minute will go
        double* ssp         -- Where the converted second will go

    EFFECTS:
        Modifes the memory pointed to by the arguments to contain the converted Gregorian date

    RETURN:
        void
*/

void get_greg(double julian, int*yearp,int*monthp,int*dayp,int*hourp,int*mmp,double*ssp)
{

    int JGREG = 15 + 31*(10+12*1582);

    double julian_fract = julian-(int)julian;
    double total_ss,ss;
    int hh,mm;
    if(julian_fract >0.5)
        total_ss = 86400*(julian_fract-0.5);
    else
        total_ss = 86400*(julian_fract+0.5);

    hh = (int)(total_ss/3600);
    mm = (int)((total_ss-(hh*3600))/60);
    ss = total_ss-(hh*60.0+mm)*60.0;


    /* Calculte fraction */

    julian = julian + 0.5;

    double F, I, B, C, D, E, G, A;
    int day, month, year;

    F = modf(julian, &I);
    A = trunc(((int)I - 1867216.25)/36524.25);

    if(I > 2299160)
    {
        B = I + 1 + A - trunc(A / 4.0);
    }

    else
        B = I;

    C = B + 1524;
    D = trunc((C - 122.1) / 365.25);
    E = trunc(365.25 * D);
    G = trunc((C - E) / 30.6001);

    day = C - E + F - trunc(30.6001 * G);

    if(G < 13.5) 
        month = G - 1;
    else 
        month = G - 13;

    if(month > 2.5) 
        year = D - 4716;
    else 
        year = D - 4715;

    *yearp = year;
    *monthp = month;
    *dayp   = day;
    *hourp  = hh;
    *mmp    = mm;
    *ssp    = ss;

    return;
}
/*
        binarySearchUTC

        TODO: NEED to CLEAN UP the comments.
    DESCRIPTION:
        This function performs a binary search on a sorted incrased order array of double values.
        If the exact target is not found, the first element that is greater than the target value is returned
        in the argument targetIndex.

    ARGUMENTS:
        IN
            1. double* array         -- The array to perform the search on
            2. double target         -- The value to search for
            3. hsize_t size          -- The number of elements in array
        OUT
            4. long int* targetIndex -- The index of where the target value is. If target doesn't exist in the array,
                                        this will return the first element in array that is larger than target.

    EFFECTS:
        Modifies the memory pointed to by targetIndex to contain the selected index of the array.

    RETURN:
        0 if an exact match is found.
        1 if an exact match was not found (first element larger than target is returned).
       -1 if an error occured.

*/
int binarySearchUTC ( const double* jd, GDateInfo_t target, int start_index,int end_index)
{
    int middle = 0;

    GDateInfo_t gran_mark_index_time,gran_mark_adj_time;

    int temp_year = 0;
    int temp_month = 0;
    int temp_day = 0;
    int temp_hour = 0;
    int temp_minute = 0;
    double temp_second = 0.;
    int* temp_yearp =&temp_year;
    int* temp_monthp=&temp_month;
    int* temp_dayp=&temp_day;
    int* temp_hourp=&temp_hour;
    int* temp_minutep=&temp_minute;
    double* temp_secondp=&temp_second;

    if(start_index >end_index)
        return -1;
    middle = (start_index+end_index)/2;

    if(middle == end_index)
        return end_index;

#if 0
    printf("orbit year is %d\n",target.year);
    printf("orbit month is %d\n",target.month);
    printf("orbit day is %d\n",target.day);
    printf("orbit hour is %d\n",target.hour);
    printf("orbit minute is %d\n",target.minute);
    printf("orbit second is %lf\n",target.second);
#endif
    get_greg(jd[middle],temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);
    gran_mark_index_time.year = *temp_yearp;
    gran_mark_index_time.month = *temp_monthp;
    gran_mark_index_time.day = *temp_dayp;
    gran_mark_index_time.hour = *temp_hourp;
    gran_mark_index_time.minute = *temp_minutep;
    gran_mark_index_time.second = *temp_secondp;

#if 0
    printf("N orbit year is %d\n",gran_mark_index_time.year);
    printf("N orbit month is %d\n",gran_mark_index_time.month);
    printf("N orbit day is %d\n",gran_mark_index_time.day);
    printf("N orbit hour is %d\n",gran_mark_index_time.hour);
    printf("N orbit minute is %d\n",gran_mark_index_time.minute);
    printf("N orbit second is %lf\n",gran_mark_index_time.second);
#endif



    int cmp_orbit_mark_time = comp_greg(target,gran_mark_index_time);

    get_greg(jd[middle+1],temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);
    gran_mark_adj_time.year = *temp_yearp;
    gran_mark_adj_time.month = *temp_monthp;
    gran_mark_adj_time.day = *temp_dayp;
    gran_mark_adj_time.hour = *temp_hourp;
    gran_mark_adj_time.minute = *temp_minutep;
    gran_mark_adj_time.second = *temp_secondp;

    int cmp_orbit_adj_time = comp_greg(target,gran_mark_adj_time);
    if ( cmp_orbit_mark_time >=0 && cmp_orbit_adj_time <0)
        return middle;
    else if(cmp_orbit_mark_time <0)
        return binarySearchUTC(jd,target,0,middle);
    else
        return binarySearchUTC(jd,target,middle,end_index);

}

int comp_greg_utc(double sgreg,GDateInfo_t sutc)
{

    GDateInfo_t sgreg_utc;
    int temp_year = 0;
    int temp_month = 0;
    int temp_day = 0;
    int temp_hour = 0;
    int temp_minute = 0;
    double temp_second = 0.0;
    int* temp_yearp =&temp_year;
    int* temp_monthp=&temp_month;
    int* temp_dayp=&temp_day;
    int* temp_hourp=&temp_hour;
    int* temp_minutep=&temp_minute;
    double* temp_secondp=&temp_second;

    get_greg(sgreg,temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);
    sgreg_utc.year = *temp_yearp;
    sgreg_utc.month = *temp_monthp;
    sgreg_utc.day = *temp_dayp;
    sgreg_utc.hour = *temp_hourp;
    sgreg_utc.minute = *temp_minutep;
    sgreg_utc.second = *temp_secondp;
    return comp_greg(sgreg_utc,sutc);

}

int utc_time_diff(struct tm start_date, struct tm end_date)
{


    //struct tm start_date;
    //struct tm end_date;
    time_t start_time, end_time;
    int seconds;

#if 0
    start_date.tm_hour = 0;
    start_date.tm_min = 0;
    start_date.tm_sec = 0;
    start_date.tm_mon = 1;
    start_date.tm_mday = 1;
    start_date.tm_year = 112;

    end_date.tm_hour = 0;
    end_date.tm_min = 0;
    end_date.tm_sec = 0;
    end_date.tm_mon = 2;
    end_date.tm_mday = 1;
    end_date.tm_year = 112;
#endif

    start_time = mktime(&start_date);
    end_time = mktime(&end_date);

    seconds = (int)(difftime(end_time, start_time)) ;

    return seconds;
}


/* This function is self-explanatory */
int numDigits(int digit)
{
    if ( digit < 0 ) digit = ( digit == INT_MAX ) ? INT_MIN : -digit;
    if ( digit < 10 ) return 1;
    if ( digit < 100 ) return 2;
    if ( digit < 1000) return 3;
    if ( digit < 10000) return 4;
    if ( digit < 100000) return 5;
    if ( digit < 1000000) return 6;
    if ( digit < 10000000) return 7;
    if ( digit < 100000000) return 8;
    if ( digit < 1000000000) return 9;
    return 10;
}


/*
 *      updateGranList
 *
 * DESCRIPTION:
 *      This function handles creating a list of input granules as a string. It takes a running total of all currently visited
 *      granules, "granList", a new granule to add, "newGran", and appends newGran to granList. This handles all of the necessary
 *      memory operations for a variable length string. Adding a new granule to the list is an amortized constant operation.
 *
 * ARGUMETS:
 *      IN:
 *          const char newGran     -- A null terminated string containing the new granule. Should not have any newline characters.
 *                                    absence of the null terminator risks memory access violations!
 *      
 *      IN/OUT:
 *          char** granList        -- A pointer to a character array. This function updates the character array pointer as needed
 *                                    when allocating memory. The granList double pointer should point to a NULL value the first
 *                                    time this function is called because granList will be dereferenced once to contain the
 *                                    return value of malloc.
 *          size_t* curSize        -- The current size in bytes of the memory pointed to by *granList. Is updated when new memory
 *                                    is allocated. *curSize should be zero when first calling this function.
 *
 * EFFECTS:
 *      Allocates new memory as needed. Updates *granList and *curSize variables as needed.
 *
 *      IT IS THE DUTY OF THE CALLER to free the granList memory when finished!
 *
 * RETURN:
 *      EXIT_FAILURE.
 *      EXIT_SUCCESS.
 */

herr_t updateGranList( char** granList, const char* newGran, size_t * curSize )
{
    if ( granList == NULL || curSize == NULL || newGran == NULL )
    {
        FATAL_MSG("No arguments to this function can be NULL.\n\tgranList = %p\n\tcurSize = %p\n\tnewGran = %p\n", granList,
                   curSize, newGran);
        return EXIT_FAILURE;
    }
    size_t granLen = strlen( newGran );
    if ( granLen < 2 )
    {
        FATAL_MSG("The newGran argument contains no string!\n");
        return EXIT_FAILURE;
    }

    size_t granListSize = 0;

    if ( *granList != NULL )
        granListSize = strlen(*granList);

    if ( *granList == NULL )
    {
        if ( *curSize != 0 )
        {
            FATAL_MSG("A NULL pointer was passed for the string but the size was indicated to be non-zero.\n");
            return EXIT_FAILURE;
        }

        *granList = calloc( granLen+2, 1 );
        if ( *granList == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            return EXIT_FAILURE;
        }

        *curSize = granLen+2;
    }

    else if ( granListSize + granLen + 2 > *curSize )
    {
        
        granListSize = strlen(*granList);
        // granLen+2 to account for newline and null terminator that will be added
        void* tempPtr = realloc( *granList, max((*curSize) * 2 , granListSize+granLen+2));
        if ( tempPtr == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            return EXIT_FAILURE;
        }
        *granList = (char*) tempPtr;

        *curSize = max( (*curSize) * 2 , granListSize+granLen+2);
    }

    strncat( *granList, newGran, granLen );

    // Set the comma and null terminator on the ends of the granule list
    size_t newGranListSize = strlen(*granList);
    (*granList)[newGranListSize]   = ',';
    (*granList)[newGranListSize+1] = '\0';

    return EXIT_SUCCESS;
}

