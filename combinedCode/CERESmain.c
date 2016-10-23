#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>	
#include <hdf.h>	// hdf4
#include <hdf5.h>	// hdf5
#include "libTERRA.h"
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10

int CERESmain( int argc, char* argv[] )
{
	/*************
	 * VARIABLES *
	 *************/
	int32 fileID;
	
	intn status;
	
	double* timeDatasetBuffer = malloc( 2 * 13091 * sizeof(double));
	float* SWFilteredBuffer = malloc( 660 * 13091 * sizeof(float) );
	float* WNFilteredBuffer = malloc( 660 * 13091 * sizeof(float) );
	float* TOTFilteredBuffer = malloc( 660 * 13091 * sizeof(float) );
	float* ColatitudeBuffer = malloc( 660 * 13091 * sizeof(float) );
	float* LongitudeBuffer = malloc( 660 * 13091 * sizeof(float) );
	
	hid_t CERESrootID;
	hid_t CERESdataFieldsID;
	hid_t CERESgeolocationID;
	hid_t SWFilteredDatasetID;
	hid_t WNFilteredDatasetID;
	hid_t TOTFilteredDatasetID;
	hid_t timeDatasetID;
	hid_t colatitudeDatasetID;
	hid_t longitudeDatasetID;
	hsize_t datasetDims[2];
	
	/*****************
	 * END VARIABLES *
	 *****************/
	
	/* open the input file */
	fileID = SDstart( argv[1], DFACC_READ );
	
	/* read time data */
	status = CERESreadData( fileID, argv[1], "Julian Date and Time", timeDatasetBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData Time\n");
	}
	
	/* read CERES SW Filtered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES SW Filtered Radiances Upwards", SWFilteredBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData CERES SW Filtered Radiances Upwards\n");
	}
	
	/* read CERES WN FIltered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES WN Filtered Radiances Upwards", WNFilteredBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData CERES WN Filtered Radiances Upwards\n");
	}
	
	/* read CERES TOT FIltered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES TOT Filtered Radiances Upwards", TOTFilteredBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData CERES TOT Filtered Radiances Upwards\n");
	}
	
	/* read Colatitude of CERES FOV at TOA */
	status = CERESreadData( fileID, argv[1], "Colatitude of CERES FOV at TOA", ColatitudeBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData Colatitude of CERES FOV at TOA\n");
	}
	
	/* read Longitude of CERES FOV at TOA */
	status = CERESreadData( fileID, argv[1], "Longitude of CERES FOV at TOA", LongitudeBuffer );
	if ( status < 0 )
	{
		printf("CERESreadData Longitude of CERES FOV at TOA\n");
	}
	
	/* outputfile already exists (created by MOPITT). Create the group directories */
	//create root CERES group
	if ( createGroup( &outputFile, &CERESrootID, "CERES" ) )
	{
		printf("CERES create root group failure.\n");
	}
	
	// create the data fields group
	if ( createGroup ( &CERESrootID, &CERESdataFieldsID, "Data Fields" ) )
	{
		printf("CERES create data fields group failure.\n");
	}
	
	// create geolocation fields
	if ( createGroup( &CERESrootID, &CERESgeolocationID, "Geolocation" ) )
	{
		fprintf( stderr, "Failed to create CERES geolocation group.\n");
	}
	
	/* INSERT DATASETS */
	//insert SWfiltered
	datasetDims[0] = 13091;
	datasetDims[1] = 660;
	SWFilteredDatasetID = insertDataset( &outputFile, &CERESdataFieldsID, 1, 2, datasetDims,
						"CERES SW Filtered Radiances Upwards", SWFilteredBuffer );
	if ( SWFilteredDatasetID < 0 )
	{
		fprintf(stderr, "Error writing SWFiltered dataset.\n" );
	}
	
	//insert WNfiltered
	datasetDims[0] = 13091;
	datasetDims[1] = 660;
	WNFilteredDatasetID = insertDataset( &outputFile, &CERESdataFieldsID, 1, 2, datasetDims,
						"CERES WN Filtered Radiances Upwards", WNFilteredBuffer );
	if ( WNFilteredDatasetID < 0 )
	{
		fprintf(stderr, "Error writing WNFiltered dataset.\n" );
	}
	
	//insert TOTfiltered
	datasetDims[0] = 13091;
	datasetDims[1] = 660;
	TOTFilteredDatasetID = insertDataset( &outputFile, &CERESdataFieldsID, 1, 2, datasetDims,
						"CERES TOT Filtered Radiances Upwards", TOTFilteredBuffer );
	if ( TOTFilteredDatasetID < 0 )
	{
		fprintf(stderr, "Error writing TOTFiltered dataset.\n" );
	}
	
	//insert time dataset
	datasetDims[0] = 13091;
	datasetDims[1] = 2;
	timeDatasetID = insertDataset( &outputFile, &CERESgeolocationID, 1, 2, datasetDims,
						"Julian Date and Time", timeDatasetBuffer );
	if (  timeDatasetID < 0 )
	{
		fprintf(stderr, "Error writing time dataset.\n" );
	}
	
	//insert colatitude
	datasetDims[0] = 13091;
	datasetDims[1] = 660;
	colatitudeDatasetID = insertDataset( &outputFile, &CERESgeolocationID, 1, 2, datasetDims,
						"Colatitude of CERES FOV at TOA", ColatitudeBuffer );
	if ( colatitudeDatasetID < 0 )
	{
		fprintf(stderr, "Error writing colatitude dataset.\n" );
	}
	
	//insert longitude
	datasetDims[0] = 13091;
	datasetDims[1] = 660;
	longitudeDatasetID = insertDataset( &outputFile, &CERESgeolocationID, 1, 2, datasetDims,
						"Longitude of CERES FOV at TOA", LongitudeBuffer );
	if ( longitudeDatasetID < 0 )
	{
		fprintf(stderr, "Error writing longitude dataset.\n" );
	}
	
	
	/* release associated identifiers */
	SDend(fileID);
	H5Gclose(CERESrootID);
	H5Gclose(CERESgeolocationID);
	H5Gclose(CERESdataFieldsID);
	H5Dclose(SWFilteredDatasetID);
	H5Dclose(WNFilteredDatasetID);
	H5Dclose(TOTFilteredDatasetID);
	H5Dclose(timeDatasetID);
	H5Dclose(colatitudeDatasetID);
	H5Dclose(longitudeDatasetID);
		
	/* release allocated memory (malloc) */
	free(timeDatasetBuffer);
	free(SWFilteredBuffer);
	free(WNFilteredBuffer);
	free(TOTFilteredBuffer);
	free(ColatitudeBuffer);
	free(LongitudeBuffer);
	
	return 0;
	
}


