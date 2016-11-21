#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>
#include <hdf.h>
#include <hdf5.h>
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10

int32 CERESreadData( int32 fileID, char* fileName, char* datasetName, void* data );

int main( int argc, char* argv[] )
{
	/*************
	 * VARIABLES *
	 *************/
	int32 fileID;
	
	intn status;
	
	double* timeDataset = malloc( 2 * 13091 * sizeof(double));
	float* SWFiltered = malloc( 660 * 13091 * sizeof(float) );
	float* WNFiltered = malloc( 660 * 13091 * sizeof(float) );
	float* TOTFiltered = malloc( 660 * 13091 * sizeof(float) );
	float* Colatitude = malloc( 660 * 13091 * sizeof(float) );
	float* Longitude = malloc( 660 * 13091 * sizeof(float) );
	
	/*****************
	 * END VARIABLES *
	 *****************/
	
	/* open the input file */
	fileID = SDstart( argv[1], DFACC_READ );
	
	/* read time data */
	status = CERESreadData( fileID, argv[1], "Julian Date and Time", timeDataset );
	if ( status < 0 )
	{
		printf("CERESreadData Time\n");
	}
	
	/* read CERES SW Filtered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES SW Filtered Radiances Upwards", SWFiltered );
	if ( status < 0 )
	{
		printf("CERESreadData CERES SW Filtered Radiances Upwards\n");
	}
	
	/* read CERES WN FIltered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES WN Filtered Radiances Upwards", WNFiltered );
	if ( status < 0 )
	{
		printf("CERESreadData CERES WN Filtered Radiances Upwards\n");
	}
	
	/* read CERES TOT FIltered Radiances Upwards */
	status = CERESreadData( fileID, argv[1], "CERES TOT Filtered Radiances Upwards", TOTFiltered );
	if ( status < 0 )
	{
		printf("CERESreadData CERES TOT Filtered Radiances Upwards\n");
	}
	
	/* read Colatitude of CERES FOV at TOA */
	status = CERESreadData( fileID, argv[1], "Colatitude of CERES FOV at TOA", Colatitude );
	if ( status < 0 )
	{
		printf("CERESreadData Colatitude of CERES FOV at TOA\n");
	}
	
	/* read Longitude of CERES FOV at TOA */
	status = CERESreadData( fileID, argv[1], "Longitude of CERES FOV at TOA", Longitude );
	if ( status < 0 )
	{
		printf("CERESreadData Longitude of CERES FOV at TOA\n");
	}
	
	
	
	/* release associated identifiers */
	SDend(fileID);
	
	/* release allocated memory (malloc) */
	free(timeDataset);
	free(SWFiltered);
	free(WNFiltered);
	free(TOTFiltered);
	free(Colatitude);
	free(Longitude);
	
	return 0;
	
}

int32 CERESreadData( int32 fileID, char* fileName, char* datasetName, void* data )
{
	int32 sds_id, sds_index;
	int32 rank;
	int32 ntype;					// number type for the data stored in the data set
	int32 num_attrs;				// number of attributes
	int32 dimsizes[DIM_MAX];
	intn status;
	int32 start[DIM_MAX] = {0};
	int32 stride[DIM_MAX] = {0};
	int j;
	
	/* get the index of the dataset from the dataset's name */
	sds_index = SDnametoindex( fileID, datasetName );
	if( sds_index < 0 )
	{
		printf("SDnametoindex\n");
	}
	
	sds_id = SDselect( fileID, sds_index );
	if ( sds_id < 0 )
	{
		printf("SDselect\n");
	}
	
	/* get info about dataset (rank, dim size, number type, num attributes ) */
	status = SDgetinfo( sds_id, NULL, &rank, dimsizes, &ntype, &num_attrs);
	if ( status < 0 )
	{
		printf("SDgetinfo\n");
		return -1;
	}
	
	start[0] = 0;
	start[1] = 0;
	
	for ( int i = 0; i < rank; i++ )
		stride[i] = 1;
	
	status = SDreaddata( sds_id, start, stride, dimsizes, data );
	if ( status < 0 )
	{
		printf("SDreaddata\n");
		return -1;
	}
	
	SDendaccess(sds_id);
	
	return 0;
}
