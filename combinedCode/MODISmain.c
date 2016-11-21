#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>	// hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>	// hdf5
#include "libTERRA.h"
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10

/*
	[0] = main program name
	[1] = 1km file
	[2] = 500m file
	[3] = 250m file
	[4] = MOD03 file
	[5] = output file (already exists);
*/

//herr_t MODIS_1KM_RefSBAttr( hid_t objectID );
hid_t MODISInsertData( hid_t outputGroupID, char* datasetName, int32 inputDataType, 
					   hid_t outputDataType, int32 inputFile, char* inputFileName );

int MODISmain( int argc, char* argv[] )
{
	/*************
	 * VARIABLES *
	 *************/
	
	
	intn status;
	
	hid_t MODISrootGroupID;
	hid_t MODISdataFieldsGroupID;
	hid_t MODISgeolocationGroupID;

	/**********************
	 * 1KM data variables *
	 **********************/
		/* File IDs */
	int32 _1KMFileID;
		/* Dataset IDs */
	hid_t _1KMDatasetID;
	hid_t _1KMUncertID;
	hid_t _1KMEmissive;
	hid_t _1KMEmissiveUncert;
	hid_t _250Aggr1km;
	hid_t _250Aggr1kmUncert;
	hid_t _500Aggr1km;
	hid_t _500Aggr1kmUncert;
		/* Attribute IDs */
	hid_t _1KMAttrID;
		/* Group IDs */
	hid_t MODIS1KMGroupID;
	
	/***********************
	 * 500m data variables *
	 ***********************/
		/* File IDs */
	int32 _500mFileID;
		/* Dataset IDs */
	hid_t _250Aggr500;	
	hid_t _250Aggr500Uncert;
	hid_t _500RefSB;
	hid_t _500RefSBUncert;
		/* Attribute IDs */
		
		/* Group IDs */
	hid_t MODIS500mGroupID;
	
	/***********************
	 * 250m data variables *
	 ***********************/
		/* File IDs */
	int32 _250mFileID;
	
		/* Dataset IDs */
	hid_t _250RefSB;
	hid_t _250RefSBUncert;
		/* Attribute IDs */
		
		/* Group IDs */
	hid_t MODIS250mGroupID;
	
		
	/************************
	 * MOD03 data variables *
	 ************************/
	 	/* File IDs */
	int32 MOD03FileID;
	
		/* Dataset IDs */
		
		/* Attribute IDs */
		
		/* Group IDs */
		
	/* latitude data variables */
	
	hid_t latitudeDatasetID;
	hid_t latitudeAttrID;
	
	/* longitude data variables */
	
	hid_t longitudeDatasetID;
	hid_t longitudeAttrID;
	
	
	
	/*****************
	 * END VARIABLES *
	 *****************/
	 
	
	
	/* open the input files */
	_1KMFileID = SDstart( argv[1], DFACC_READ );
	if ( _1KMFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 1KM file.\n", __FILE__, __func__,
				 __LINE__ );
		exit (EXIT_FAILURE);
	}
	
	_500mFileID = SDstart( argv[2], DFACC_READ );
	if ( _500mFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
				 __LINE__ );
		exit (EXIT_FAILURE);
	}
	
	_250mFileID = SDstart( argv[3], DFACC_READ );
	if ( _250mFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
				 __LINE__ );
		exit (EXIT_FAILURE);
	}
	
	MOD03FileID = SDstart( argv[4], DFACC_READ );
	if ( MOD03FileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open MOD03 file.\n", __FILE__, __func__,
				 __LINE__ );
		exit (EXIT_FAILURE);
	}
	
	
	/********************************************************************************
	 *                                GROUP CREATION                                *
	 ********************************************************************************/
	 
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
	
	/* create the 1 kilometer product group */
	if ( createGroup ( &MODISdataFieldsGroupID, &MODIS1KMGroupID, "1KM" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 1KM group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	/* create the 500m product group */
	if ( createGroup ( &MODISdataFieldsGroupID, &MODIS500mGroupID, "500m" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 500m group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	/* create the 250m product group */
	if ( createGroup ( &MODISdataFieldsGroupID, &MODIS250mGroupID, "250m" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 250m group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	// create geolocation fields group
	if ( createGroup( &MODISrootGroupID, &MODISgeolocationGroupID, "Geolocation" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS geolocation group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	/******************************************************************************* 
	 *                              INSERT DATASETS                                *
	 *******************************************************************************/
	/* Note: Macros that start with "H5T" or "DFNT" are HDF provided number types.
	 * To see a listing of HDF4 or HDF5 number types, please refer to Section 3 of the
	 * HDF4 reference manual (HDF Constant Definition List) or the HDF5 API specification
	 * under the "Predefined Datatypes" link
	 */
	 
	 
	 			/*------------------------------------
	 			  ------------- 1KM File -------------
	 			  ------------------------------------*/
	 			  
	 			  
	 			  
	/*_______________EV_1KM_RefSB data_______________*/
	
	_1KMDatasetID = MODISInsertData( MODIS1KMGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
					H5T_NATIVE_USHORT, _1KMFileID, argv[1] );
					
					
	/*______________EV_1KM_RefSB_Uncert_Indexes______________*/
	/* TODO
		I do not know if H5T_STD_U8LE is the correct data type for the output.
		I could not find an explicit unsigned 8 bit int native type in HDF5 so
		I used one of the predefined types, a standard unsigned 8 bit little endian.
		Note that little endian might be the incorrect one, big endian might need to be
		used.
	*/
	_1KMUncertID = MODISInsertData( MODIS1KMGroupID, "EV_1KM_RefSB_Uncert_Indexes",
					DFNT_UINT8, H5T_STD_U8LE, _1KMFileID, argv[1] );
					
	/*___________EV_1KM_Emissive___________*/
	
	_1KMEmissive = MODISInsertData( MODIS1KMGroupID, "EV_1KM_Emissive",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID, argv[1] );
					
	/*___________EV_1KM_Emissive_Uncert_Indexes*___________*/
	
	_1KMEmissiveUncert = MODISInsertData( MODIS1KMGroupID,
						  "EV_1KM_Emissive_Uncert_Indexes",
						  DFNT_UINT8, H5T_STD_U8LE, _1KMFileID, argv[1] );
						  
	/*__________EV_250_Aggr1km_RefSB_______________*/
	
	_250Aggr1km = MODISInsertData( MODIS1KMGroupID, "EV_250_Aggr1km_RefSB",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID, argv[1] );
					
	/*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/
	
	_250Aggr1kmUncert = MODISInsertData( MODIS1KMGroupID, 
						"EV_250_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID, argv[1] );				
	
	/*__________EV_500_Aggr1km_RefSB____________*/
	
	_500Aggr1km = MODISInsertData( MODIS1KMGroupID, "EV_500_Aggr1km_RefSB",
				  DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID, argv[1] );
				  
	/*__________EV_500_Aggr1km_RefSB_Uncert_Indexes____________*/
	
	_500Aggr1kmUncert = MODISInsertData( MODIS1KMGroupID, 
						"EV_500_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID, argv[1] );
						
						
						
						
				/*-------------------------------------
	 			  ------------- 500m File -------------
	 			  -------------------------------------*/
	 			  
	 			  
	 			  
	 			  
	/*_____________EV_250_Aggr500_RefSB____________*/
	
	_250Aggr500 = MODISInsertData( MODIS500mGroupID, 
						"EV_250_Aggr500_RefSB",
						DFNT_UINT16, H5T_NATIVE_USHORT, _500mFileID, argv[1] );
						
	/*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
	
	_250Aggr500Uncert = MODISInsertData( MODIS500mGroupID, 
						"EV_250_Aggr500_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _500mFileID, argv[1] );
						
	/*____________EV_500_RefSB_____________*/
	
	_500RefSB = MODISInsertData( MODIS500mGroupID, "EV_500_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _500mFileID, argv[1] );
				
	/*____________EV_500_RefSB_Uncert_Indexes_____________*/
	
	_500RefSBUncert = MODISInsertData( MODIS500mGroupID, "EV_500_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _500mFileID, argv[1] );
					  
	
	
				/*-------------------------------------
	 			  ------------- 250m File -------------
	 			  -------------------------------------*/
	 			  
	 			  
	/*____________EV_250_RefSB_____________*/
	
	_250RefSB = MODISInsertData( MODIS250mGroupID, "EV_250_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _250mFileID, argv[1] );
				
	/*____________EV_250_RefSB_Uncert_Indexes_____________*/
	
	_250RefSBUncert = MODISInsertData( MODIS250mGroupID, "EV_250_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _250mFileID, argv[1] ); 			  
	
	
	
				/*-----------------------------------
				 ------------ MOD03 File ------------
				 ------------------------------------*/
				 
						
	/*_______________latitude data_______________*/
	
	latitudeDatasetID = MODISInsertData( MODISgeolocationGroupID,
					  "Latitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID, argv[1] );
	
	/*_______________longitude data______________*/
	longitudeDatasetID = MODISInsertData( MODISgeolocationGroupID,
					  "Longitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID, argv[1] );
	
	/*********************
	 * INSERT ATTRIBUTES *
	 *********************/
	 
	/*-------------------- EV_1KM_RefSB --------------------------*/
	//MODIS_1KM_RefSBAttr( _1KMDatasetID );
	
	
	
	/* release associated identifiers */
	SDend(_1KMFileID);
	SDend(_500mFileID);
	SDend(_250mFileID);
	SDend(MOD03FileID);
	H5Gclose(MODISrootGroupID);
	H5Gclose(MODISgeolocationGroupID);
	H5Gclose(MODISdataFieldsGroupID);
	H5Gclose(MODIS1KMGroupID);
	H5Gclose(MODIS500mGroupID);
	H5Gclose(MODIS250mGroupID);
	H5Dclose(_1KMDatasetID);
	H5Dclose(_1KMUncertID);
	H5Dclose(_1KMEmissive);
	H5Dclose(_1KMEmissiveUncert);
	H5Dclose(_250Aggr1km);
	H5Dclose(_250Aggr1kmUncert);
	H5Dclose(_500Aggr1km);
	H5Dclose(_500Aggr1kmUncert);
	H5Dclose(_500RefSB);
	H5Dclose(_500RefSBUncert);
	H5Dclose(_250Aggr500);
	H5Dclose(_250Aggr500Uncert);
	H5Dclose(_250RefSB);
	H5Dclose(_250RefSBUncert);
	H5Dclose(latitudeDatasetID);
	H5Dclose(longitudeDatasetID);
	H5Aclose(_1KMAttrID);
	H5Aclose(longitudeAttrID);
	H5Aclose(latitudeAttrID);
	
	
	
	return 0;
	
}

#if 0
herr_t MODIS_1KM_RefSBAttr( hid_t objectID )
{
	hid_t dataspaceID;
	int dimSize;
	hid_t attrID;
	/* units */
	attrCreateString( objectID, "units", "degrees" );
	
	/* valid_range */
	dimSize = 2;
	dataspaceID = H5Screate_simple( 1, &dimSize, NULL );
	//attrID = H5Acreate( objectID, 
	return EXIT_SUCCESS;
}
#endif
hid_t MODISInsertData( hid_t outputGroupID, char* datasetName, int32 inputDataType, 
					   hid_t outputDataType, int32 inputFileID, char* inputFileName  )
{
	int32 dataRank;
	int32 dataDimSizes[DIM_MAX];
	unsigned int* dataBuffer;
	hid_t datasetID;
	
	herr_t status;
	
	status = H4readData( inputFileID, inputFileName, datasetName,
		(void**)&dataBuffer, &dataRank, dataDimSizes, inputDataType );
	
	if ( status < 0 )
	{
		fprintf( stderr, "[%s]: Unable to read %s data.\n", __func__, datasetName );
		exit (EXIT_FAILURE);
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
		
	if ( datasetID < 0 )
	{
		fprintf(stderr, "[%s]: Error writing %s dataset.\n", __func__, datasetName );
		exit (EXIT_FAILURE);
	}
	
	
	free(dataBuffer);
	
	return datasetID;
}

