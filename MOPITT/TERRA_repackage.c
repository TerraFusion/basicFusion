/*
	DESCRIPTION:
	
	This program takes an input MOPITT HDF file as argv[1] and writes out the radiance, longitude,
	latitude, and time datasets into a new file at argv[2]. This program will eventually become a 
	MOPITT library. Suggestions on how to improve this program, or even more elegant solutions to 
	it, will be greatly appreciated.
	
	AUTHOR:
		Landon Clipp
		
	EMAIL:
		clipp2@illinois.edu
		
*/


#include <stdio.h>
#include <hdf5.h>		// default HDF5 library
#include <stdlib.h>
#include <string.h>


#define RADIANCE "HDFEOS/SWATHS/MOP01/Data Fields/MOPITTRadiances"
#define LATITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Latitude"
#define LONGITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Longitude"
#define TIME "HDFEOS/SWATHS/MOP01/Geolocation Fields/Time"
#define RADIANCE_UNIT "Watts meter-2 Sr-1"
#define LAT_LON_UNIT "degree"
#define TIME_UNIT "seconds since 12 AM 1-1-13 UTC"

/*
	NOTE: argv[1] = INPUT_FILE
		  argv[2] = OUTPUT_FILE
*/

/*********************
 *FUNCTION PROTOTYPES*
 *********************/
 
float getElement5D( float *array, hsize_t dimSize[5], int *position );	// note, this function is used for testing. It's temporary

hid_t MOPITTinsertDataset( hid_t const *inputFileID, hid_t *datasetGroup_ID, 
							char * inDatasetPath, char* outDatasetPath, int returnDatasetID);
herr_t openFile(hid_t *file, char* inputFileName, unsigned flags );
herr_t createOutputFile( hid_t *outputFile, char* outputFileName);
herr_t MOPITTrootGroup( hid_t const *outputFile, hid_t *MOPITTroot);
herr_t MOPITTradianceGroup( hid_t const *MOPITTroot, hid_t *radianceGroup );
herr_t MOPITTgeolocationGroup ( hid_t const *MOPITTroot, hid_t *geolocationGroup );
hid_t attributeCreate( hid_t objectID, const char* attrName, hid_t datatypeID );


/* TODO:		
		I'm considering combining the three group functions into one becaue they are all so similar. 
		It will only take a bit of tinkering to get it to work. The way I have it now is bloated and
		unneccessary. 
		
		Create an attributeWrite function. Currently, main is taking care of all the writing but we can
		make a function to make the code less cluttered.
	
*/


int main( int argc, char* argv[] )
{
	if ( argc != 3 )
	{
		
		fprintf( stderr, "\nError! Program expects format: %s [input HDF5 file] [output HDF5 file]\n\n", argv[0] );
		return EXIT_FAILURE;
	
	}
	
	hid_t file;
	hid_t outputFile;
	hid_t MOPITTroot;
	hid_t radianceGroup;
	hid_t geolocationGroup;
	hid_t radianceDataset;
	hid_t latitudeDataset;
	hid_t longitudeDataset;
	hid_t timeDataset;
	hid_t attrID;		// attribute ID
	hid_t stringType;
	
	herr_t status;
	
	int tempInt;
	float tempFloat;
	
	
	/* remove output file if it already exists. Note that no conditional statements are used. If file does not exist,
	 * this function will throw an error but we do not care.
	 */
	 
	remove( argv[2] );
	
	// open the input file
	if ( openFile( &file, argv[1], H5F_ACC_RDONLY ) ) return EXIT_FAILURE;
	
	// create the output file or open it if it exists
	if ( createOutputFile( &outputFile, argv[2] )) return EXIT_FAILURE;
	
	// create the root group
	if ( MOPITTrootGroup( &outputFile, &MOPITTroot ) ) return EXIT_FAILURE;
	
	// create the radiance group
	if ( MOPITTradianceGroup ( &MOPITTroot, &radianceGroup ) ) return EXIT_FAILURE;
	
	// create the geolocation group
	if ( MOPITTgeolocationGroup ( &MOPITTroot, &geolocationGroup ) ) return EXIT_FAILURE;
	
	// insert the radiance dataset
	radianceDataset = MOPITTinsertDataset( &file, &radianceGroup, RADIANCE, "Radiance", 1 );
	if ( radianceDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the longitude dataset
	longitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LONGITUDE, "Longitude", 1 );
	if ( longitudeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the latitude dataset
	latitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LATITUDE, "Latitude", 1 );
	if ( latitudeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the time dataset
	timeDataset = MOPITTinsertDataset( &file, &geolocationGroup, TIME, "Time", 1);
	if ( timeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	/***************************************************************
	 * Add the appropriate attributes to the groups we just created*
	 ***************************************************************/
	 
						/* !!! FOR RADIANCE FIELD !!! */
	
	/* TrackCount.  */
	
	attrID = attributeCreate( radianceGroup, "TrackCount", H5T_NATIVE_UINT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempInt = 6350;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempInt );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close TrackCount attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* missing_invalid */
	attrID = attributeCreate( radianceGroup, "missing_invalid", H5T_NATIVE_FLOAT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempFloat = -8888.0;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempFloat );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close missing_invalid attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* missing_nodata */
	attrID = attributeCreate( radianceGroup, "missing_nodata", H5T_NATIVE_FLOAT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempFloat = -9999.0;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempFloat );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close missing_nodata attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* radiance_unit */
	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(RADIANCE_UNIT));
	
	
	attrID = attributeCreate( radianceGroup, "radiance_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, RADIANCE_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close radiance_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	H5Tclose(stringType);
	
								/* ### END OF RADIANCE FIELD ATTRIBUTE CREATION ### */
								
								/* !!! FOR GEOLOCATION FIELD !!! */
	/* Latitude_unit */
	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(LAT_LON_UNIT) );
	
	
	attrID = attributeCreate( geolocationGroup, "Latitude_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Latitude_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* Longitude_unit */
	
	attrID = attributeCreate( geolocationGroup, "Longitude_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Longitude_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	H5Tclose(stringType);
	
	/* Time_unit */
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(TIME_UNIT) );
	
	
	attrID = attributeCreate( geolocationGroup, "Time_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, TIME_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Time_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
					/* ### END OF GEOLOCATION FIELD ATTRIBUTE CREATION ### */
	
	/*
	 * Free all associated memory
	 */
	 
	H5Dclose(radianceDataset);
	H5Dclose(latitudeDataset);
	H5Dclose(longitudeDataset);
	H5Dclose(timeDataset);
	H5Fclose(file);
	H5Fclose(outputFile);
	H5Gclose(MOPITTroot);
	H5Gclose(radianceGroup);
	H5Gclose(geolocationGroup);
	H5Tclose(stringType);
	
	
	
	
	return 0;

}

/*
	DESCRIPTION:
		This function returns the value specified by the dimensions given to it in the array
	ARGUMENTS:
		1. float array
		2. Size of each dimension (given by a 5 element 1D array. Elements 0-4 specify size of dimensions 0-4).
		3. Requested position in the array (given by a 5 element 1D array. Elements 0-4 specify which position in each dimension)
	EFFECTS:
		None. Does not write anything.
	RETURN:
		Value stored at specified dimension
		
	array[dim0][dim1][dim2][dim3][dim4]
*/
		
float getElement5D( float *array, hsize_t dimSize[5], int position[5] )
{
	if ( position[0] < 0 || position[0] >= dimSize[0] ||
		 position[1] < 0 || position[1] >= dimSize[1] ||
		 position[2] < 0 || position[2] >= dimSize[2] ||
		 position[3] < 0 || position[3] >= dimSize[3] ||
		 position[4] < 0 || position[4] >= dimSize[3] )
	{
		fprintf( stderr, "\ngetElement5D: Error! Invalid position. Check that dimSize and position arguments are correct.\n");
		exit (-1);
	}
	
	return array[ (position[0] * dimSize[1] * dimSize[2] * dimSize[3] * dimSize[4]) + 
				  (position[1] * dimSize[2] * dimSize[3] * dimSize[4]) +
				  (position[2] * dimSize[3] * dimSize[4]) +
				  (position[3] * dimSize[4]) +
				   position[4] ];

}

/*
	DESCRIPTION:
		This function reads the radiance dataset from the input File ID, and writes the dataset into a new, already opened file.
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
						   char * inDatasetPath, char* outDatasetPath, int returnDatasetID )
{
	
	hid_t dataset;								// dataset ID
	hid_t dataspace;							// filespace ID
	hid_t memspace;								// memoryspace ID
	
	hsize_t *datasetDims; 						// size of each dimension
	hsize_t *datasetMaxSize;					// maximum allowed size of each dimension in datasetDims
	hsize_t *datasetChunkSize;
	
	herr_t status_n, status;
	
	float * data_out;
	
	int rank;
	
	/*
	 * open radiance dataset and do error checking
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
	
	data_out = malloc( sizeof(float ) * datasetDims[0] * datasetDims[1] * datasetDims[2] *  datasetDims[3] * datasetDims[4]);
	
	/*
	 * Now read dataset into data_out buffer
	 * H5T_NATIVE_FLOAT is specifying that we are reading floating points
	 * H5S_ALL specifies that we are reading the entire dataset instead of just a portion
	 */

	status = H5Dread( dataset, H5T_NATIVE_FLOAT, dataspace, H5S_ALL, H5P_DEFAULT, data_out );
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
	
	dataset = H5Dcreate( *datasetGroup_ID, outDatasetPath, H5T_NATIVE_FLOAT, memspace, 
						 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
						 
	/* Now that dataset has been created, read into this data set our data_out array */
	
	status = H5Dwrite( dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5S_ALL, data_out );
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
	DESCRIPTION:
		This function take a pointer to a file identifier and opens the file specified by the inputFileName.
		The main purpose of this is to update the value of the file identifier so that access to the file
		can proceed. This function allows for READ ONLY and R/W access.
	ARGUMENTS:
		1. pointer to file identifier
		2. input file name string
		3. File access flag. Allowable values are:
			H5F_ACC_RDWR
				For Read/Write
			H5F_ACC_RDONLY
				For read only.
	EFFECTS:
		Opens the specified file and update the file identifier with the necessary information to access the 
		file.
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
		fprintf( stderr, "\n[%s]: H5Fopen -- Could not open HDF5 file. Exiting program.\n\n", __func__ );
		return EXIT_FAILURE;
	}
	
	return EXIT_SUCCESS;
	
}

/*
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
	FIXME
	
	I want to eventually take the following group creation functions and combine them into one.
	It's unnecessary to have three separate functions. All that is needed in addition to what
	is written is that the function accept a dataset name string as an argument.
*/

/*
	DESCRIPTION:
		This function creates the root group (aka directory) for the MOPITT datasets in our HDF5 file. It updates the group
		identifier given to it in the arguments.
	ARGUMENTS:
		1. A pointer to the file identifier
		2. A pointer to the group identifier (to be modified)
	EFFECTS:
		Creates a new directory in the HDF file, updates the group pointer.
	RETURN:
		EXIT_FAILURE on failure (equivalent to non-zero number in most environments)
		EXIT_SUCCESS on success (equivalent to 0 in most systems)
*/

herr_t MOPITTrootGroup( hid_t const *outputFile, hid_t *MOPITTroot)
{
	*MOPITTroot = H5Gcreate( *outputFile, "MOPITT", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
	if ( *MOPITTrootGroup < 0 )
	{
		fprintf( stderr, "\n[%s]: H5Gcreate -- Could not create MOPITT root group. Exiting program.\n\n", __func__ );
		return EXIT_FAILURE;
	}
	
	return EXIT_SUCCESS;
}

/* This function is nearly identical to the previous one, except it creates the radiance group inside the MOPITT
   group.
*/

herr_t MOPITTradianceGroup( hid_t const *MOPITTroot, hid_t *radianceGroup )
{
	*radianceGroup = H5Gcreate( *MOPITTroot, "Radiance", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
	
	if ( *radianceGroup < 0 )
	{
		fprintf( stderr, "\n[%s]: H5Gcreate -- Could not create MOPITT radiance group. Exiting program.\n\n", __func__ );
		return EXIT_FAILURE;
	}
	
	return EXIT_SUCCESS;
}

herr_t MOPITTgeolocationGroup ( hid_t const *MOPITTroot, hid_t *geolocationGroup )
{
	*geolocationGroup = H5Gcreate( *MOPITTroot, "Geolocation", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT );
	
	if ( *geolocationGroup < 0 )
	{
		fprintf( stderr, "\n[%s]: H5Gcreate -- Could not create MOPITT geolocation group. Exiting program.\n\n", __func__ );
		return EXIT_FAILURE;
	}
	
	return EXIT_SUCCESS;
}

/*
	DESCRIPTION:
		This function creates one single attribute for the object specified by objectID.
		It is the duty of the caller to free the attribute identifier using H5Aclose().
	ARGUMENTS:
		1. ObjectID: identifier for object to create attribute for
		2. attrName: string for the attribute
		3. datatypeID: datatype identifier. Google: "HDF5 Predefined Datatypes"
	EFFECTS:
		Creates uninitialized attribute at object specified by objectID
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
