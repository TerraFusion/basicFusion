
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
#include <string.h>
#include <hdf5.h>
#include <hdf.h>
#include <mfhdf.h>
#define DIM_MAX 10

/*
	TODO
		I recommend replacing all error checks with the assert() function. It will
		make the code look much neater and makes debugging simpler.
*/

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
		8. data_out		   -- A single dimensional array containing information of the
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
	
	memspace = H5Screate_simple( rank, datasetDims, NULL );
	//printf("\n%d\n", rank);
	dataset = H5Dcreate( *datasetGroup_ID, datasetName, dataType, memspace, 
						 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
						 
	status = H5Dwrite( dataset, dataType, H5S_ALL, H5S_ALL, H5S_ALL, (VOIDP)data_out );
	if ( status < 0 )
	{
		fprintf( stderr, "\n[%s:%s:%d]: H5DWrite: Unable to write to dataset \"%s\". Exiting program.\n", __FILE__,__func__ ,__LINE__ , datasetName );
		
		return (EXIT_FAILURE);
	}
	
	/* Free all remaining memory */
	
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
		fprintf( stderr, "\n[%s]: H5Dopen: Could not open \"%s\". Exiting program.\n\n", __func__, inDatasetPath );
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
		fprintf( stderr, "\n[%s]: H5S_get_simple_extent_ndims: Unable to get rank. Exiting program.\n\n", __func__ );
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
		fprintf( stderr, "\n[%s]: H5S_get_simple_extent_dims: Unable to get dimensions. Exiting program.\n\n", __func__ );
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
		fprintf( stderr, "\n[%s]: H5Dread: Unable to read from dataset \"%s\". Exiting program.\n\n", __func__, inDatasetPath );
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
		fprintf( stderr, "\n[%s]: H5DWrite: Unable to write to dataset \"%s\". Exiting program.\n\n", __func__, outDatasetPath );
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
		H5Dclose(dataset);
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
		fprintf( stderr, "\n[%s:%s:%d]: H5Fopen -- Could not open HDF5 file. Exiting program.\n\n", __FILE__, __func__, __LINE__ );
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
		fprintf( stderr, "\n[%s]: H5Fcreate -- Could not create HDF5 file. Does it already exist? If so, delete or don't\n\t\t    call this function.\n\n", __func__ );
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
	*newGroup = H5Gcreate( *referenceGroup, newGroupName, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
	if ( *newGroup < 0 )
	{
		fprintf( stderr, "\n[%s]: H5Gcreate -- Could not create '%s' root group. Exiting program.\n\n", __func__ , newGroupName);
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
		fprintf( stderr, "\n[%s]: H5Acreate -- Could not create attribute \"%s\". Exiting program.\n\n", __func__, attrName );
		return EXIT_FAILURE;
	}
	
	// close dataspace
	if ( H5Sclose(attrDataspace) < 0 )
		fprintf( stderr, "\n[%s]: H5Sclose -- Noncritical error. Could not close attribute dataset for \"%s\". Memory leak possible.\n\n", __func__, attrName );
	
	return attrID;
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
		5. dimsizes	   -- The caller will pass in an array (stored on the stack) which
						  will be updated to contain the sizes of each dimension in the
						  dataset that was read. This array MUST be of size DIM_MAX.
		6. dataType	   -- An HDF4 datatype describing the type of data in the desired
						  dataset. Please refer to Sectino 3 of the HDF4 Reference Manual,
						  "HDF Constant Definition List."
*/

int32 H4readData( int32 fileID, char* datasetName, void** data, int32 *rank, int32* dimsizes, int32 dataType )
{
	int32 sds_id, sds_index;
	int32 ntype;					// number type for the data stored in the data set
	int32 num_attrs;				// number of attributes
	intn status;
	int32 start[DIM_MAX] = {0};
	int32 stride[DIM_MAX] = {0};
	
	/* get the index of the dataset from the dataset's name */
	sds_index = SDnametoindex( fileID, datasetName );
	if( sds_index < 0 )
	{
		printf("SDnametoindex\n");
		return EXIT_FAILURE;
	}
	
	sds_id = SDselect( fileID, sds_index );
	if ( sds_id < 0 )
	{
		printf("SDselect\n");
		return EXIT_FAILURE;
	}
	
	/* Initialize dimsizes elements to 1 */
	for ( int i = 0; i < DIM_MAX; i++ )
	{
		dimsizes[i] = 1;
	}
	
	/* get info about dataset (rank, dim size, number type, num attributes ) */
	status = SDgetinfo( sds_id, NULL, rank, dimsizes, &ntype, &num_attrs);
	if ( status < 0 )
	{
		printf("SDgetinfo\n");
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
		fprintf( stderr, "[%s:%s:%d]: Invalid data type.\nIt may be the case that your datatype has not been accounted for in the %s function.\nIf that is the case, simply add your datatype to the function in the else-if tree.\n", __FILE__, __func__, __LINE__, __func__ );
		SDendaccess(sds_id);
		return EXIT_FAILURE;
	}
	
	for ( int i = 0; i < *rank; i++ )
		stride[i] = 1;
	
	status = SDreaddata( sds_id, start, stride, dimsizes, *data );		
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: SDreaddata: Failed to read data.\n", __func__);
		SDendaccess(sds_id);
		return EXIT_FAILURE;
	}
	
	
	SDendaccess(sds_id);
	
	return 0;
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
	
	RETURN;
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
		fprintf( stderr, "[%s]: H5Aclose -- Unable to create %s attribute. Exiting program.\n", __func__, name);
		return EXIT_FAILURE;
	}
	
	status = H5Awrite( attrID, stringType, value );
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: H5Awrite -- Unable to write %s attribute. Exiting program.\n", __func__, name);
		return EXIT_FAILURE;
	}
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close %s attribute. Exiting program.\n", __func__, name);
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
		fprintf( stderr, "[%s:%s:%d]: Unable to read %s data.\n", __FILE__, __func__,__LINE__,  datasetName );
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
		fprintf(stderr, "[%s:%s:%d]: Error writing %s dataset.\n", __FILE__, __func__,__LINE__, datasetName );
		return (EXIT_FAILURE);
	}
	
	
	free(dataBuffer);
	
	return datasetID;
}


