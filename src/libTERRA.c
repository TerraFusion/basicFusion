
/*
	
	
	AUTHOR:
		Landon Clipp
		
	EMAIL:
		clipp2@illinois.edu
		
	NOTE:
		I recommend you set your text editor to represent tabs as 4 columns. It'll make
		everything look a lot neater, trust me.
		
*/

#include "libTERRA.h"
#include <stdio.h>
#include <stdlib.h>
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
		1. outputFileID    -- A pointer to the file identifier of the output file.
		2. datasetGroup_ID -- A pointer to the group in the output file where the data is
							  to be written to.
		3. returnDatasetID -- An integer (boolean). 0 means do not return the identifier
							  for the newly created dataset in the output file. In such
							  a case, the function will close the identifier. Non-zero
							  means return the identifier (and do not close it). In such
							  a case, it will be the responsibility of the caller to close
							  the identifier when finished.
		4. rank			   -- The rank (number of dimensions) of the array being written
		5. datasetDims     -- A single dimension array of size rank describing the size
							  of each dimension in the data_out array.
		6. dataType		   -- An HDF5 datatype describing the type of data contained in
							  data_out. Please refer to the HDF5 API specification under
							  the link "Predefined Datatypes."
		7. datasetName	   -- A character string describing what the output dataset is to
							  be named.
i		8. data_out		   -- A single dimensional array containing information of the
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

hid_t insertDataset( hid_t const *outputFileID, hid_t *datasetGroup_ID, int returnDatasetID, 
					 int rank, hsize_t* datasetDims, hid_t dataType, char* datasetName, void* data_out) 
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
    if(dataset<0) {
        fprintf(stderr,"[%s:%s:%d] H5Dcreate -- Unable to create dataset \"%s\".\n", __FILE__,__func__ ,__LINE__ , datasetName );

		return (EXIT_FAILURE);
    }
						 
	status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5S_ALL, (VOIDP)data_out );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5DWrite -- Unable to write to dataset \"%s\".\n", __FILE__,__func__ ,__LINE__ , datasetName );
		
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

/*
					MOPITTinsertDataset
	DESCRIPTION:
		This function reads the radiance dataset from the input File ID, and writes the
		dataset into a new, already opened file.
		This function assumes that 
		1. The input file ID has already been prepared
		2. The output file ID has already been prepared
		3. The output group ID has already been prepared
		
		An absolute path for the inputDatasetPath is required, but the outDatasetPath is relative to the datasetGroup.
		It actually would work fine if you passed a group identifier for argument 1, but your inDatasetPath must then
		be relative to that group identifier. It's recommended however that you simply pass a file ID with an absolute path.
		This just makes the function more intuitive to use, but it is up to the client user.
		
		NOTE that if you are using a group ID for anything, the dataset paths must refer to a path that EXISTS
		
		The caller MUST be sure to free the dataset identifier using H5Dclose() if returnDatasetID is set to non-zero (aka true)
		or else a memory leak will occur.
		
		*********************************************	
		*Motivation for the returnDatasetID argument*
		*********************************************
		If the caller will not perform any operations on the dataset, they do not have to worry about freeing the memory
		associated with the identifier. The function will handle that. If the caller does want to operate on the dataset,
		the program will not free the ID but rather return it to the caller.
		
	ARGUMENTS:
		1. input file ID pointer. Identifier already exists.
		2. output file ID pointer. Identifier already exists.
		3. input dataset absolute path string
		4. Output dataset name
		5. dataset group ID. Group identifier already exists. This is the group that the data will be read into
		6. returnDatasetID: Set to 0 if caller does not want to return the dataset identifier.
							Set to non-zero if the caller DOES want the dataset identifier. If set to non-zero,
							caller must be sure to free the identifier using H5Dclose() when finished.
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
						   char * inDatasetPath, char* outDatasetPath, hid_t dataType, int returnDatasetID )
{
	
	hid_t dataset;								// dataset ID
	hid_t dataspace;							// filespace ID
	hid_t memspace;								// memoryspace ID
	
	hsize_t *datasetDims; 						// size of each dimension
	hsize_t *datasetMaxSize;					// maximum allowed size of each dimension in datasetDims
	hsize_t *datasetChunkSize;
	
	herr_t status_n, status;
	
	double * data_out;
	
	int rank;
	
	/*
	 * open dataset and do error checking
	 */
	 
	dataset = H5Dopen( *inputFileID, inDatasetPath, H5F_ACC_RDONLY );
	
	if ( dataset < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5Dopen -- Could not open \"%s\".\n",__FILE__, __func__,__LINE__, inDatasetPath );
		H5Dclose(dataset);
		return EXIT_FAILURE;
	}
	
	
	/*
	 * Get dataset dimensions, dimension max sizes and dataset dimensionality (the rank, aka number of dimensions)
	 */
	 
	dataspace = H5Dget_space(dataset);
	
	rank = H5Sget_simple_extent_ndims( dataspace );
	
	if ( rank < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5S_get_simple_extent_ndims -- Unable to get rank.\n",__FILE__,__func__,__LINE__ );
		H5Sclose(dataspace);
		return (EXIT_FAILURE);
	}
	
	/* Allocate memory for the following arrays (now that we have the rank) */
	
	datasetDims = malloc( sizeof(hsize_t) * 5 );
	datasetMaxSize = malloc( sizeof(hsize_t) * rank );
	datasetChunkSize = malloc( sizeof(hsize_t) * rank );
	
	/* initialize all elements of datasetDims to 1 */
	
	for ( int i = 0; i < 5; i++ )
		datasetDims[i] = 1;
	
	
	
	status_n  = H5Sget_simple_extent_dims(dataspace, datasetDims, datasetMaxSize );
	
	if ( status_n < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5S_get_simple_extent_dims -- Unable to get dimensions.\n",__FILE__, __func__,__LINE__ );
		H5Dclose(dataset);
		H5Sclose(dataspace);
		return (EXIT_FAILURE);
	}
	
	
	
	/*
	 * Create a data_out buffer using the dimensions of the dataset specified by MOPITTdims
	 */
	
	data_out = malloc( sizeof(double ) * datasetDims[0] * datasetDims[1] * datasetDims[2] *  datasetDims[3] * datasetDims[4]);
	
	/*
	 * Now read dataset into data_out buffer
	 * H5T_NATIVE_FLOAT is specifying that we are reading floating points
	 * H5S_ALL specifies that we are reading the entire dataset instead of just a portion
	 */

	status = H5Dread( dataset, dataType, dataspace, H5S_ALL, H5P_DEFAULT, data_out );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5Dread: Unable to read from dataset \"%s\".\n",__FILE__, __func__,__LINE__, inDatasetPath );
		H5Sclose(dataspace);
		H5Dclose(dataset);
		return (EXIT_FAILURE);
	}
	
	
	
	/* Free all memory associated with the input data */
	
	H5Sclose(dataspace);
	H5Dclose(dataset);
	
	/*******************************************************************************************
	 *Reading the dataset is now complete. We have freed all related memory for the input data *
	 *(except of course the data_out array) and can now work on writing this data to a new file*
	 *******************************************************************************************/
	
	memspace = H5Screate_simple( rank, datasetDims, NULL );
	
	dataset = H5Dcreate( *datasetGroup_ID, outDatasetPath, dataType, memspace, 
						 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
						 
	/* Now that dataset has been created, read into this data set our data_out array */
	
	status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5S_ALL, data_out );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5DWrite -- Unable to write to dataset \"%s\".\n",__FILE__, __func__,__LINE__, outDatasetPath );
		H5Dclose(dataset);
		H5Sclose(memspace);
		free(data_out);
		exit (EXIT_FAILURE);
	}
	
	
	
	/* Free all remaining memory */
	
	H5Sclose(memspace);
	free(data_out);
	free(datasetDims);
	free(datasetMaxSize);
	free(datasetChunkSize);
	
	/*
	 * Check to see if caller wants the dataset ID returned. If not, go ahead and close the ID.
	 */
	 
	if ( returnDatasetID == 0 )
	{	
		status = H5Dclose(dataset);
        if ( status < 0 )
        {
            fprintf( stderr, "[%s:%s:%d] H5Dclose -- Unable to close dataset.\n",__FILE__, __func__,__LINE__ );
            return EXIT_FAILURE;
        }
		return EXIT_SUCCESS;
	}
	
	return dataset;

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
		fprintf( stderr, "[%s:%s:%d] H5Fopen -- Could not open HDF5 file.\n", __FILE__, __func__, __LINE__ );
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
		fprintf( stderr, "[%s:%s:%d] H5Fcreate -- Could not create HDF5 file. Does it already exist? If so, delete or don't\n\t    call this function.\n",__FILE__, __func__,__LINE__ );
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
	RETURN:
		EXIT_FAILURE on failure
		EXIT_SUCCESS on success
*/

herr_t createGroup( hid_t const *referenceGroup, hid_t *newGroup, char* newGroupName)
{

//printf("newGroupName is %s\n",newGroupName);
	*newGroup = H5Gcreate( *referenceGroup, newGroupName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
	if ( *newGroup < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5Gcreate -- Could not create '%s' root group.\n",__FILE__,__func__,__LINE__ , newGroupName);
		*newGroup = EXIT_FAILURE;
		return EXIT_FAILURE;
	}
	
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
		fprintf( stderr, "[%s:%s:%d] H5Acreate -- Could not create attribute \"%s\".\n",__FILE__, __func__,__LINE__, attrName );
		return EXIT_FAILURE;
	}
	
	// close dataspace
	if ( H5Sclose(attrDataspace) < 0 )
		fprintf( stderr, "[%s:%s:%d] H5Sclose -- Could not close attribute dataset for \"%s\". Memory leak possible.\n",__FILE__,__func__,__LINE__, attrName );
	
	return attrID;
}

/* MY 2016-12-20, helper function to obtain vgroup reference number. This is used to check if a vgroup exists. */
int32 H4ObtainLoneVgroupRef(int32 file_id, char *groupname) {

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
                fprintf(stderr, "[%s:%s:%d] HDmalloc failed. Not enough memory for vgroup_name!\n",__FILE__,__func__,__LINE__);
	            return EXIT_FAILURE;
	        }
            status_32 = Vgetname (vgroup_id, vgroup_name);

            if(strncmp(vgroup_name,groupname,strlen(vgroup_name))==0) {
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
		4. rank		   -- A pointer to a variable. The caller will pass in a pointer
						  to its local rank variable. The caller's rank variable will be
						  updated with the rank (number of dimensions) of the dataset
						  read.
						  PASS NULL IF CALLER DOESN'T WANT

		5. dimsizes	   -- The caller will pass in an array which
						  will be updated to contain the sizes of each dimension in the
						  dataset that was read.
 
						  This array MUST be of size DIM_MAX.
						  PASS NULL IF CALLER DOESN'T WANT

		6. dataType	   -- An HDF4 datatype describing the type of data in the desired
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

int32 H4readData( int32 fileID, char* datasetName, void** data, int32 *retRank, int32* retDimsizes, int32 dataType )
{
	int32 sds_id, sds_index;
	int32 rank;
	int32 dimsizes[DIM_MAX];
	int32 ntype;					// number type for the data stored in the data set
	int32 num_attrs;				// number of attributes
	intn status;
	int32 start[DIM_MAX] = {0};
	int32 stride[DIM_MAX] = {0};
	
	/* get the index of the dataset from the dataset's name */
	sds_index = SDnametoindex( fileID, datasetName );
	if( sds_index < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] SDnametoindex: Failed to get index of dataset.\n", __FILE__, __func__, __LINE__);
		return EXIT_FAILURE;
	}
	
	sds_id = SDselect( fileID, sds_index );
	if ( sds_id < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] SDselect: Failed to select dataset.\n", __FILE__, __func__, __LINE__);
		return EXIT_FAILURE;
	}
	
	/* Initialize dimsizes elements to 1 */
	for ( int i = 0; i < DIM_MAX; i++ )
	{
		dimsizes[i] = 1;
	}
	
	/* get info about dataset (rank, dim size, number type, num attributes ) */
	status = SDgetinfo( sds_id, NULL, &rank, dimsizes, &ntype, &num_attrs);
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] SDgetinfo: Failed to get info from dataset.\n", __FILE__, __func__, __LINE__);
		return EXIT_FAILURE;
	}
	
	
	/* Allocate space for the data buffer.
	 * I'm doing some pretty nasty pointer casting trickery here. First, I'm casting
	 * the void** data to a double pointer of the appropriate data type, then I'm 
	 * dereferencing that double pointer so that the pointer in the calling
	 * function then contains the return value of malloc. Confused? So am I. It works.
	 * It was either this or make a separate function for each individual data type. So,
	 * having a void double pointer was the best choice.
	 */
	if ( dataType == DFNT_FLOAT32 )
	{
		*((float**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]*dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9] * sizeof( float ) );
	}
	else if ( dataType == DFNT_FLOAT64 )
	{
		*((double**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
				   *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9] 
				   * sizeof( double ) );
	}
	else if ( dataType == DFNT_UINT16 )
	{
		*((unsigned short int**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
				   *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9] 
				   * sizeof( unsigned short int ) );
	}
	else if ( dataType == DFNT_UINT8 )
	{
		*((uint8_t**)data) = malloc (dimsizes[0]*dimsizes[1] * dimsizes[2] * dimsizes[3] * dimsizes[4]
				   *dimsizes[5] * dimsizes[6] * dimsizes[7] * dimsizes[8] * dimsizes[9] 
				   * sizeof( uint8_t ) );
	}
	else
	{
		fprintf( stderr, "[%s:%s:%d] Invalid data type.\nIt may be the case that your datatype has not been accounted for in the %s function.\nIf that is the case, simply add your datatype to the function in the else-if tree.\n", __FILE__, __func__, __LINE__, __func__ );
		SDendaccess(sds_id);
		return EXIT_FAILURE;
	}
	
	for ( int i = 0; i < rank; i++ )
		stride[i] = 1;
	
	status = SDreaddata( sds_id, start, stride, dimsizes, *data );		
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] SDreaddata: Failed to read data.\n", __FILE__, __func__, __LINE__);
		SDendaccess(sds_id);
		return EXIT_FAILURE;
	}
	
	
	SDendaccess(sds_id);

	if ( retRank != NULL ) *retRank = rank;
	if ( retDimsizes != NULL )
		for ( int i = 0; i < DIM_MAX; i++ ) retDimsizes[i] = dimsizes[i];
	
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
		fprintf( stderr, "[%s:%s:%d] H5Aclose: Unable to create %s attribute.\n", __FILE__, __func__,__LINE__, name);
		H5Tclose(stringType);
        return EXIT_FAILURE;
	}
	
	status = H5Awrite( attrID, stringType, value );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] H5Awrite: Unable to write %s attribute.\n", __FILE__, __func__,__LINE__, name);
		H5Tclose(stringType);
        return EXIT_FAILURE;
	}
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s:%s:%d] H5Aclose: Unable to close %s attribute.\n", __FILE__, __func__,__LINE__, name);
        H5Tclose(stringType);
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
		1. outputGroupID  -- The HDF5 group/directory identifier for where the data is to
							 be written. Can either be an actual group ID or can be the
							 file ID (in the latter case, data will be written to the root
							 directory).
		2. datasetName    -- A string containing the name of the dataset in the input HDF4
							 file. The output dataset will have the same name.
		3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
							 input dataset. Please reference Section 3 of the HDF4
							 Reference Manual for a list of HDF types.
		4. outputDataType -- An HDF5 datatype identifier. Must be of the same general type
							 of the input data type. Please reference the HDF5 API
							 specification under "Predefined Datatypes" for a list of HDF5
							 datatypes.
		5. inputFileID	  -- The HDF4 input file identifier
	
	EFFECTS:
		Reads the input file dataset and then writes to the HDF5 output file. Returns
		a dataset identifier for the new dataset created in the output file. It is the
		responsibility of the caller to close the dataset ID when finished using
		H5Dclose().
	
	RETURN:
		Returns the dataset identifier if successful. Else returns EXIT_FAILURE upon
		any errors.
*/

hid_t readThenWrite( hid_t outputGroupID, char* datasetName, int32 inputDataType, 
					   hid_t outputDataType, int32 inputFileID )
{
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	unsigned int* dataBuffer;
	hid_t datasetID;
	
	herr_t status;
	
	status = H4readData( inputFileID, datasetName,
		(void**)&dataBuffer, &dataRank, dataDimSizes, inputDataType );
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
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
		
	datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
		 temp, outputDataType, datasetName, dataBuffer );
		
	if ( datasetID == EXIT_FAILURE )
	{
		fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
		return (EXIT_FAILURE);
	}
	
	
	free(dataBuffer);
	
	return datasetID;
}

char *correct_name(const char* oldname){

  char * cptr; /* temproary pointer that stores the address
                  of the string where the ORI_SLASH is located. */
  char * newname; /* the new name after changing the ORI_SLASH to
                     CHA_SLASH. */


  if(oldname == NULL) {
    fprintf(stderr, "[%s:%s:%d] The name cannot be NULL. \n", __FILE__, __func__,__LINE__ );
    return NULL;
  }

  newname = malloc(strlen(oldname)+1);
  if(newname == NULL) {
    fprintf(stderr, "[%s:%s:%d] No enough memory to allocate the newname. \n", __FILE__, __func__,__LINE__ );
    return NULL;
  }

  memset(newname,0,strlen(oldname)+1);

  newname = strncpy(newname, oldname, strlen(oldname));
  while(strchr(newname,'/')!= NULL){
    cptr = strchr(newname,'/');
    *cptr = '_';
  }

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
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	unsigned short* tir_dataBuffer;
    uint8_t* vsir_dataBuffer;
    float* output_dataBuffer;
    size_t buffer_size = 1;
	hid_t datasetID;
	hid_t outputDataType;

	intn status = -1;

    if(unc < 0) {
		fprintf( stderr, "[%s:%s:%d] There is no valid Unit Conversion Coefficient for this dataset  %s.\n", __FILE__, __func__,__LINE__,  datasetName );
		return (EXIT_FAILURE);

    }
	
    if(DFNT_UINT8 == inputDataType) {
	    status = H4readData( inputFileID, datasetName,
		(void**)&vsir_dataBuffer, &dataRank, dataDimSizes, inputDataType );
    }
    else if(DFNT_UINT16 == inputDataType) {
        status = H4readData( inputFileID, datasetName,
        (void**)&tir_dataBuffer, &dataRank, dataDimSizes, inputDataType );
    }
    else {
	   fprintf( stderr, "[%s:%s:%d] Unsupported datatype. Datatype must be either DFNT_UINT16 or DFNT_UINT8.\n", __FILE__, __func__,__LINE__ );
	   return (EXIT_FAILURE);
    }

	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
		return (EXIT_FAILURE);
	}
	
    {
        float* temp_float_pointer;
        uint8_t* temp_uint8_pointer = vsir_dataBuffer;
        unsigned short* temp_uint16_pointer = tir_dataBuffer;
        /* Now we need to unpack the data */
        for(int i = 0; i <dataRank;i++) 
            buffer_size *=dataDimSizes[i];
        
        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);
        
        temp_float_pointer = output_dataBuffer;

        if(DFNT_UINT8 == inputDataType) {
          for(int i = 0; i<buffer_size;i++){
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
        else if(DFNT_UINT16 == inputDataType) {
          for(int i = 0; i<buffer_size;i++){
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
	datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
                temp, outputDataType, datasetName, output_dataBuffer );

		
	if ( datasetID == EXIT_FAILURE )
	{
		fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
		return (EXIT_FAILURE);
	}
	
	
    if(DFNT_UINT16 == inputDataType)
	    free(tir_dataBuffer);
    else if(DFNT_UINT8 == inputDataType)
            free(vsir_dataBuffer);
	
    free(output_dataBuffer);
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
        3. inputDataType  -- An HDF4 datatype identifier. Must match the data type of the
                             input dataset. Please reference Section 3 of the HDF4
                             Reference Manual for a list of HDF types.
        4. inputFileID    -- The HDF4 input file identifier
        5. scale_factor   -- Value of "scale" in the unpacking equation
    
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
hid_t readThenWrite_MISR_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType, 
				  int32 inputFileID,float scale_factor )
{
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	unsigned short* input_dataBuffer;
        float * output_dataBuffer;
	hid_t datasetID;
    hid_t outputDataType;	
	intn status;
	
    if(scale_factor < 0) {
		fprintf( stderr, "[%s:%s:%d] The scale_factor of %s is less than 0.\n", __FILE__, __func__,__LINE__,  datasetName );
		return (EXIT_FAILURE);

    }
	status = H4readData( inputFileID, datasetName,
		(void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
		return (EXIT_FAILURE);
	}

        /* Before unpacking the data, we want to re-arrange the name */
	char* newdatasetName = NULL;
    char* RDQIName = "/RDQI";
    char* temp_sub_dsetname = strstr(datasetName,RDQIName);
        
    if(temp_sub_dsetname!=NULL && strncmp(RDQIName,temp_sub_dsetname,strlen(temp_sub_dsetname)) == 0){ 
        newdatasetName = malloc(strlen(datasetName)-strlen(RDQIName)+1);
        memset(newdatasetName,'\0',strlen(datasetName)-strlen(RDQIName)+1);

        strncpy(newdatasetName,datasetName,strlen(datasetName)-strlen(RDQIName));
    }
    else {// We will fail here.
        fprintf( stderr, "[%s:%s:%d] Error: The dataset name doesn't end with /RDQI\n", __FILE__, __func__,__LINE__);
        return EXIT_FAILURE;
    }


    /* Data Unpack */
    {

        float* temp_float_pointer;
        unsigned short* temp_uint16_pointer = input_dataBuffer;
        size_t buffer_size = 1;
        unsigned short  rdqi = 0;
        unsigned short  rdqi_mask = 3;
        size_t num_la_data = 0;
        uint8_t has_la_data = 0;

        unsigned int* la_data_pos = NULL;
        unsigned int* temp_la_data_pos_pointer = NULL;

        for(int i = 0; i <dataRank;i++)
            buffer_size *=dataDimSizes[i];


        for(int i = 0; i<buffer_size;i++){
            rdqi = (*temp_uint16_pointer)&rdqi_mask;
            if(rdqi == 1)
                num_la_data++;
        }

        if(num_la_data>0) {
            has_la_data = 1;
            la_data_pos = malloc(sizeof la_data_pos*num_la_data);
            temp_la_data_pos_pointer = la_data_pos;
        }
 
        /* We need to check if there are any the low accuracy data */
        

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);
        temp_float_pointer = output_dataBuffer;

        unsigned short temp_input_val;

        if(has_la_data == 1) {
            for(int i = 0; i<buffer_size;i++){

                rdqi = (*temp_uint16_pointer)&rdqi_mask;
                if(rdqi == 2 || rdqi == 3)
                    *temp_float_pointer = -999.0;
                else {
                    if(rdqi == 1) {
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
        else {
            for(int i = 0; i<buffer_size;i++){
                rdqi = (*temp_uint16_pointer)&rdqi_mask;
                if(rdqi == 2 || rdqi == 3)
                    *temp_float_pointer = -999.0;
                else {
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
        if(has_la_data ==1) {

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
            la_pos_dsetid = insertDataset( &outputFile, &outputGroupID, 1, la_pos_dset_rank ,
             la_pos_dset_dims, H5T_NATIVE_INT, la_pos_dset_name, la_data_pos );
    
            if ( la_pos_dsetid == EXIT_FAILURE )
            {
                fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, newdatasetName );
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
	datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
		 temp, outputDataType, newdatasetName, output_dataBuffer );
		
	if ( datasetID == EXIT_FAILURE )
	{
		fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
		return (EXIT_FAILURE);
	}
	
	
    free(newdatasetName);
	free(input_dataBuffer);
    free(output_dataBuffer);
	
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
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	unsigned short* input_dataBuffer;
    float* output_dataBuffer;
	hid_t datasetID;
    hid_t outputDataType;


    unsigned short special_values_start = 65535;
    unsigned short special_values_stop = 65500;

    float special_values_packed_start = -999.0;
	
	intn status = -1;

	status = H4readData( inputFileID, datasetName,
		(void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
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
		    fprintf( stderr, "[%s:%s:%d]: SDnametoindex: Failed to get index of dataset.\n", __FILE__, __func__, __LINE__);
		    return EXIT_FAILURE;
	    }
	
	    sds_id = SDselect( inputFileID, sds_index );
	    if ( sds_id < 0 )
	    {
		    printf("SDselect\n");
		    return EXIT_FAILURE;
	    }

        radi_sc_index = SDfindattr(sds_id,radi_scales);
        if(radi_sc_index < 0) {
            fprintf( stderr, "[%s:%s:%d] Cannot find attribute %s of variable %s\n", __FILE__, __func__, __LINE__,radi_scales,datasetName);
		    return EXIT_FAILURE;
        }

        if(SDattrinfo (sds_id, radi_sc_index, temp_attr_name, &radi_sc_type, &num_radi_sc_values)<0) {
            fprintf( stderr, "[%s:%s:%d] Cannot obtain SDS attribute %s of variable %s\n", __FILE__, __func__, __LINE__,radi_scales,datasetName);
		    return EXIT_FAILURE;
        }

        radi_off_index = SDfindattr(sds_id,radi_offset);
        if(radi_off_index < 0) {
            fprintf( stderr, "[%s:%s:%d] Cannot find attribute %s of variable %s\n", __FILE__, __func__, __LINE__,radi_offset,datasetName);
		    return EXIT_FAILURE;
        }


        if(SDattrinfo (sds_id, radi_off_index, temp_attr_name, &radi_off_type, &num_radi_off_values)<0) {
            fprintf( stderr, "[%s:%s:%d] Cannot obtain SDS attribute %s of variable %s\n", __FILE__, __func__, __LINE__,radi_offset,datasetName);
		    return EXIT_FAILURE;
        }

        if(radi_sc_type != DFNT_FLOAT32 || radi_sc_type != radi_off_type || num_radi_sc_values != num_radi_off_values) {
            fprintf( stderr, "[%s:%s:%d] Error: ", __FILE__, __func__, __LINE__);
		    fprintf(stderr, "Either the scale/offset datatype is not 32-bit floating-point type\n\tor there is inconsistency between scale and offset datatype or number of values\n");
            fprintf(stderr, "\tThis is for the variable %s\n",datasetName);
		    return EXIT_FAILURE;
        }


        radi_sc_values = calloc((size_t)num_radi_sc_values,sizeof radi_sc_values);
        radi_off_values= calloc((size_t)num_radi_off_values,sizeof radi_off_values);

        if(SDreadattr(sds_id,radi_sc_index,radi_sc_values) <0) {
            fprintf( stderr, "[%s:%s:%d] Cannot obtain SDS attribute value %s of variable %s\n", __FILE__, __func__, __LINE__,radi_scales,datasetName);
		    return EXIT_FAILURE;
        }

 
        if(SDreadattr(sds_id,radi_off_index,radi_off_values) <0) {
            fprintf( stderr, "[%s:%s:%d] Cannot obtain SDS attribute value %s of variable %s\n", __FILE__, __func__, __LINE__,radi_scales,datasetName);
		    return EXIT_FAILURE;
        }
        
        SDendaccess(sds_id);


        float* temp_float_pointer;
        unsigned short* temp_uint16_pointer = input_dataBuffer;
        size_t buffer_size = 1;
        size_t band_buffer_size = 1;
        int num_bands = dataDimSizes[0];

        if(num_bands != num_radi_off_values) {
            fprintf( stderr, "[%s:%s:%d] Error: Number of band (the first dimension size) of the variable %s\n\tis not the same as the number of scale/offset values\n", __FILE__, __func__, __LINE__,datasetName);
		    return EXIT_FAILURE;
        }

        assert(dataRank>1);
        for(int i = 1; i <dataRank;i++)
            band_buffer_size *=dataDimSizes[i];

        buffer_size = band_buffer_size*num_bands;

        output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

        temp_float_pointer = output_dataBuffer;


        for(int i = 0; i<num_bands;i++){
            float temp_scale_offset = radi_sc_values[i]*radi_off_values[i];
                for(int j = 0; j<band_buffer_size;j++) {
                    /* Check special values  , here I may need to make it a little clear.*/
                    if(((*temp_uint16_pointer)<=special_values_start) && ((*temp_uint16_pointer)>=special_values_stop))
                        *temp_float_pointer = special_values_packed_start +(special_values_start-(*temp_uint16_pointer));
                    else {
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
	datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
		 temp, outputDataType, datasetName, output_dataBuffer );
		
	if ( datasetID == EXIT_FAILURE )
	{
		fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
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
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	uint8_t* input_dataBuffer;
    float* output_dataBuffer;
	hid_t datasetID;
    hid_t outputDataType;

    //Ideally the following 3 values should be retrieved from SDS APIs
    uint8_t fvalue = 255;
    uint8_t valid_min= 0;
    uint8_t valid_max = 15;
    float fvalue_packed = -999.0;
	
	intn status = -1;

	
	status = H4readData( inputFileID, datasetName,
		(void**)&input_dataBuffer, &dataRank, dataDimSizes, inputDataType );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
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
		        fprintf( stderr, "[%s:%s:%d] SDnametoindex -- Failed to get index of dataset.\n", __FILE__, __func__, __LINE__);
		        return EXIT_FAILURE;
	        }
	
	        sds_id = SDselect( inputFileID, sds_index );
	        if ( sds_id < 0 )
	        {
                fprintf( stderr, "[%s:%s:%d] SDselect -- Failed to get ID of dataset.\n", __FILE__, __func__, __LINE__);
		        return EXIT_FAILURE;
	        }

            sc_index = SDfindattr(sds_id,scaling_factor);
            if(sc_index < 0) {
                fprintf( stderr, "[%s:%s:%d] SDfindattr -- Cannot find attribute %s of variable %s\n", __FILE__, __func__, __LINE__,scaling_factor,datasetName);
		        return EXIT_FAILURE;
            }

            if(SDattrinfo (sds_id, sc_index, temp_attr_name, &sc_type, &num_sc_values)<0) {
                fprintf( stderr, "[%s:%s:%d] SDattrinfo -- Cannot obtain SDS attribute %s of variable %s\n", __FILE__, __func__, __LINE__,scaling_factor,datasetName);
		        return EXIT_FAILURE;
            }

            uncert_index = SDfindattr(sds_id,specified_uncert);
            if(uncert_index < 0) {
                fprintf( stderr, "[%s:%s:%d] SDfindattr -- Cannot find attribute %s of variable %s\n", __FILE__, __func__, __LINE__,specified_uncert,datasetName);
		        return EXIT_FAILURE;
            }


            if(SDattrinfo (sds_id, uncert_index, temp_attr_name, &uncert_type, &num_uncert_values)<0) {
                fprintf( stderr, "[%s:%s:%d] SDattrinfo -- Cannot obtain attribute %s of variable %s\n", __FILE__, __func__, __LINE__,specified_uncert,datasetName);
		        return EXIT_FAILURE;
            }

            if(sc_type != DFNT_FLOAT32 || num_sc_values != num_uncert_values) {
                fprintf( stderr, "[%s:%s:%d] Error: Either the scale datatype is not 32-bit floating-point type or there is \n\tinconsistency of number of values between scale and specified uncertainty.\n", __FILE__, __func__, __LINE__);
                fprintf(stderr, "\tThis is for the variable %s\n",datasetName);
		        return EXIT_FAILURE;
            }


            sc_values = calloc((size_t)num_sc_values,sizeof sc_values);
            uncert_values= calloc((size_t)num_uncert_values,sizeof uncert_values);

            if(SDreadattr(sds_id,sc_index,sc_values) <0) {
                fprintf( stderr, "[%s:%s:%d] SDreadattr -- Cannot obtain SDS attribute value %s of variable %s\n", __FILE__, __func__, __LINE__,scaling_factor,datasetName);
		        return EXIT_FAILURE;
            }
            
            if(SDreadattr(sds_id,uncert_index,uncert_values) <0) {
                fprintf( stderr, "[%s:%s:%d] SDattrinfo -- Cannot obtain SDS attribute value %s of variable %s\n", __FILE__, __func__, __LINE__,specified_uncert,datasetName);
		        return EXIT_FAILURE;
            }

            SDendaccess(sds_id);


            float* temp_float_pointer;
            uint8_t* temp_uint8_pointer = input_dataBuffer;
            size_t buffer_size = 1;
            size_t band_buffer_size = 1;
            int num_bands = dataDimSizes[0];

            if(num_bands != num_uncert_values) {
                fprintf( stderr, "[%s:%s:%d] Error: Number of band (the first dimension size) of the variable %s is not\n\tthe same as the number of scale/offset values\n", __FILE__, __func__, __LINE__,datasetName);
		        return EXIT_FAILURE;
            }

            assert(dataRank>1);
            for(int i = 1; i <dataRank;i++)
                band_buffer_size *=dataDimSizes[i];

            buffer_size = band_buffer_size*num_bands;

            output_dataBuffer = malloc(sizeof output_dataBuffer *buffer_size);

            temp_float_pointer = output_dataBuffer;


            for(int i = 0; i<num_bands;i++){
                for(int j = 0; j<band_buffer_size;j++) {
                    /* Check special values  , here I may need to make it a little clear.*/
                    if((*temp_uint8_pointer)== fvalue)
                        *temp_float_pointer = fvalue_packed; 
                    else if(((*temp_uint8_pointer)<valid_min) && ((*temp_uint8_pointer)>valid_max)){
                        /* currently set any invalid value to fill values */
                        *temp_float_pointer = fvalue_packed;
                    }
                    else {
                        *temp_float_pointer = uncert_values[i]*exp((*temp_uint8_pointer)/sc_values[i]);
//printf("i,j,SI and radiance are %d,%d,%d,%f\n",i,j,*temp_uint16_pointer,*temp_float_pointer);
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
	datasetID = insertDataset( &outputFile, &outputGroupID, 1, dataRank ,
		 temp, outputDataType, datasetName, output_dataBuffer );
		
	if ( datasetID == EXIT_FAILURE )
	{
		fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
		return (EXIT_FAILURE);
	}
	
	
	free(input_dataBuffer);
        free(output_dataBuffer);
	

	return datasetID;
}


