#ifndef H_TERRA
#define H_TERRA
#include <mfhdf.h>
#include <hdf5.h>

#define DIM_MAX 10

/*********************
 *FUNCTION PROTOTYPES*
 *********************/
extern hid_t outputFile;

int MOPITT( char* argv[] );
int CERES( char* argv[] );
int MODIS( char* argv[] );
int ASTER( char* argv[] );
int MISR( char* argv[] );
 
float getElement5D( float *array, hsize_t dimSize[5], int *position );	// note, this function is used for testing. It's temporary

hid_t insertDataset( hid_t const *outputFileID, hid_t *datasetGroup_ID, 
					 int returnDatasetID, int rank, hsize_t* datasetDims, 
					 hid_t dataType, char* datasetName, void* data_out);
					 
hid_t MOPITTinsertDataset( hid_t const *inputFileID, hid_t *datasetGroup_ID, 
							char * inDatasetPath, char* outDatasetPath, hid_t dataType, int returnDatasetID);
herr_t openFile(hid_t *file, char* inputFileName, unsigned flags );
herr_t createOutputFile( hid_t *outputFile, char* outputFileName);
herr_t createGroup( hid_t const *referenceGroup, hid_t *newGroup, char* newGroupName);
/* general type attribute creation */
hid_t attributeCreate( hid_t objectID, const char* attrName, hid_t datatypeID );
/* creates and writes a string attribute */
hid_t attrCreateString( hid_t objectID, char* name, char* value );

int32 H4readData( int32 fileID, char* datasetName, void** data,
				  int32 *rank, int32* dimsizes, int32 dataType );
hid_t readThenWrite( hid_t outputGroupID, char* datasetName, int32 inputDataType, 
					   hid_t outputDataType, int32 inputFile);	  

#endif
