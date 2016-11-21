#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>	// hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>	// hdf5
#include <h5toh5.h>
#include "libTERRA.h"
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10

int MODISmain( int argc, char* argv[] )
{
	/*************
	 * VARIABLES *
	 *************/
	int32 fileID;
	
	intn status;
	
	int dimSize;
	
	hid_t MODISrootGroupID;
	hid_t MODISdataFieldsGroupID;
	hid_t MODISgeolocationGroupID;
	
	
	hsize_t temp[DIM_MAX];
	
	/* 1KM data variables */
	int32 _1KMDataRank;
	int32 _1KMDataDimSizes[DIM_MAX];
	unsigned int* _1KMDataBuffer;
	hid_t _1KMDatasetID;
	hid_t _1KMAttrID;
	
	/* latitude data variables */
	int32 latitudeDataRank;
	int32 latitudeDataDimSizes[DIM_MAX];
	float* latitudeDataBuffer;
	hid_t latitudeDatasetID;
	hid_t latitudeAttrID;
	
	/* longitude data variables */
	int32 longitudeDataRank;
	int32 longitudeDataDimSizes[DIM_MAX];
	float* longitudeDataBuffer;
	hid_t longitudeDatasetID;
	hid_t longitudeAttrID;
	
	
	
	/*****************
	 * END VARIABLES *
	 *****************/
	 
	
	
	/* open the input file */
	fileID = SDstart( argv[1], DFACC_READ );
	if ( fileID < 0 )
	{
		fprintf( stderr, "[%s]: Unable to open input file.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	
	/* read EV_1KM_RefSB data */
	
	
	#if 0
	status = H4readData( fileID, argv[1], "EV_1KM_RefSB",
		(void**)&_1KMDataBuffer, &_1KMDataRank, _1KMDataDimSizes, DFNT_UINT16 );
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: Unable to read 1KM_RefSB data.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	/* read Latitude data */
	status = H4readData( fileID, argv[1], 
		"Latitude", (void**)&latitudeDataBuffer,
		&latitudeDataRank, latitudeDataDimSizes, DFNT_FLOAT32 );
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: Unable to read Latitude data.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	/* read longitude data */
	status = H4readData( fileID, argv[1], 
		"Longitude", (void**)&longitudeDataBuffer,
		&longitudeDataRank, longitudeDataDimSizes, DFNT_FLOAT32 );
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: Unable to read Longitude data.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	/* outputfile already exists (created by MOPITT). Create the group directories */
	//create root MODIS group
	if ( createGroup( &outputFile, &MODISrootGroupID, "MODIS" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS root group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	// create the data fields group
	if ( createGroup ( &MODISrootGroupID, &MODISdataFieldsGroupID, "Data Fields" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS data fields group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	// create geolocation fields group
	if ( createGroup( &MODISrootGroupID, &MODISgeolocationGroupID, "Geolocation" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS geolocation group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	/* INSERT DATASETS */
	
	//insert EV_1KM_RefSB data
	/* Because we are converting from HDF4 to HDF5, there are a few type mismatches
	 * that need to be resolved. Thus, we need to take the DimSizes array, which is
	 * of type int32 (an HDF4 type) and put it into an array of type hsize_t.
	 */
	 
	for ( int i = 0; i < DIM_MAX; i++ )
		temp[i] = (hsize_t) _1KMDataDimSizes[i];
		
	_1KMDatasetID = insertDataset( &outputFile, &MODISdataFieldsGroupID, 1, _1KMDataRank ,
		 temp, H5T_NATIVE_USHORT, "EV_1KM_RefSB", _1KMDataBuffer );
		
	if ( _1KMDatasetID < 0 )
	{
		fprintf(stderr, "[%s]: Error writing 1EV_1KM_RefSB dataset.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	
	
	//insert latitude data
	
	for ( int i = 0; i < DIM_MAX; i++ )
		temp[i] = (hsize_t) latitudeDataDimSizes[i];
		
	latitudeDatasetID = insertDataset( &outputFile, &MODISgeolocationGroupID, 1,
					latitudeDataRank, temp, H5T_NATIVE_FLOAT,
					"Latitude", latitudeDataBuffer );
		
	if ( latitudeDatasetID < 0 )
	{
		fprintf(stderr, "[%s]: Error writing latitude dataset.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	//insert longitude data
	
	for ( int i = 0; i < DIM_MAX; i++ )
		temp[i] = (hsize_t) longitudeDataDimSizes[i];
		
	longitudeDatasetID = insertDataset( &outputFile, &MODISgeolocationGroupID, 1,
					longitudeDataRank, temp, H5T_NATIVE_FLOAT,
					"Longitude", longitudeDataBuffer );
		
	if ( longitudeDatasetID < 0 )
	{
		fprintf(stderr, "[%s]: Error writing longitude dataset.\n", __func__ );
		exit (EXIT_FAILURE);
	}
	
	/*********************
	 * INSERT ATTRIBUTES *
	 *********************/
	 
	/*-------------------- EV_1KM_RefSB --------------------------*/
	MODIS_1KM_RefSBAttr( _1KMDatasetID );
	
	
	
	/* release associated identifiers */
	SDend(fileID);
	H5Gclose(MODISrootGroupID);
	H5Gclose(MODISgeolocationGroupID);
	H5Gclose(MODISdataFieldsGroupID);
	H5Dclose(_1KMDatasetID);
	H5Dclose(latitudeDatasetID);
	H5Dclose(longitudeDatasetID);
	H5Aclose(_1KMAttrID);
	H5Aclose(longitudeAttrID);
	H5Aclose(latitudeAttrID);
	
		
	/* release allocated memory (malloc) */
	free(_1KMDataBuffer);
	free( longitudeDataBuffer );
	free( latitudeDataBuffer );
	
	
	return 0;
	#endif
}


herr_t MODIS_1KM_RefSBAttr( hid_t objectID )
{
	hid_t dataspaceID;
	hid_t attrID;
	/* units */
	attrCreateString( objectID, "units", "degrees" );
	
	/* valid_range */
	dimSize = 2;
	dataspaceID = H5Screate_simple( 1, &dimSize, NULL );
	attrID = H5Acreate( objectID, 
}


