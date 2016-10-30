#ifndef H_TERRA_FUNCS
#define H_TERRA_FUNCS

#include <stdio.h>
#include <hdf5.h>		// default HDF5 library
#include <hdf.h>
#include <stdlib.h>
#include <string.h>

#define RADIANCE "HDFEOS/SWATHS/MOP01/Data Fields/MOPITTRadiances"
#define LATITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Latitude"
#define LONGITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Longitude"
#define TIME "HDFEOS/SWATHS/MOP01/Geolocation Fields/Time"
#define RADIANCE_UNIT "Watts meter-2 Sr-1"
#define LAT_LON_UNIT "degree"
#define TIME_UNIT "seconds since 12 AM 1-1-13 UTC"

/*********************
 *FUNCTION PROTOTYPES*
 *********************/
extern hid_t outputFile;

int MOPITTmain( int argc, char* argv[] );
int CERESmain( int argc, char* argv[] );
 
float getElement5D( float *array, hsize_t dimSize[5], int *position );	// note, this function is used for testing. It's temporary

hid_t insertDataset( hid_t const *outputFileID, hid_t *datasetGroup_ID, 
					 int returnDatasetID, int rank, hsize_t* datasetDims, 
					 hid_t dataType, char* datasetName, void* data_out);
					 
hid_t MOPITTinsertDataset( hid_t const *inputFileID, hid_t *datasetGroup_ID, 
							char * inDatasetPath, char* outDatasetPath, int returnDatasetID);
herr_t openFile(hid_t *file, char* inputFileName, unsigned flags );
herr_t createOutputFile( hid_t *outputFile, char* outputFileName);
herr_t createGroup( hid_t const *referenceGroup, hid_t *newGroup, char* newGroupName);

hid_t attributeCreate( hid_t objectID, const char* attrName, hid_t datatypeID );

int32 CERESreadData( int32 fileID, char* fileName, char* datasetName, void* data );

#endif
