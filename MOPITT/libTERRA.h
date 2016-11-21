#ifndef H_TERRA_FUNCS
#define H_TERRA_FUNCS

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

extern hid_t outputFile;

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

#endif
