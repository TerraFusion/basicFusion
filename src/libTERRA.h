#ifndef TERRA_H
#define TERRA_H
#include <time.h>
#include <hdf.h>
#include <mfhdf.h>
#include <hdf5.h>
#include <hdf5_hl.h>
#define DEBUG 0
#define DIM_MAX 10
#define FATAL_MSG( ... ) \
do { \
    fprintf(stderr,"[%s:%d] Fatal error: ",__FILE__,__LINE__); \
    fprintf(stderr, __VA_ARGS__); \
    } while(0)
#define WARN_MSG( ... ) \
do { \
    fprintf(stderr,"[%s:%d] Warning: ",__FILE__,__LINE__); \
    fprintf(stderr, __VA_ARGS__); \
    } while(0)

#ifndef STR_LEN
#define STR_LEN 500
#endif

typedef struct OInfo
{
    unsigned int orbit_number;
    unsigned short start_year;
    unsigned char  start_month;
    unsigned char start_day;
    unsigned char start_hour;
    unsigned char start_minute;
    unsigned char start_second;
    unsigned short end_year;
    unsigned char  end_month;
    unsigned char end_day;
    unsigned char end_hour;
    unsigned char end_minute;
    unsigned char end_second;

} OInfo_t;

typedef struct GDateInfo
{
    unsigned short year;
    unsigned char  month;
    unsigned char day;
    unsigned char hour;
    unsigned char minute;
    double second;

} GDateInfo_t;

/*********************
 *FUNCTION PROTOTYPES*
 *********************/
extern hid_t outputFile;
extern double* TAI93toUTCoffset; // The array containing the TAI93 to UTC offset values
int numDigits(int digit);

int MOPITT( char* argv[], OInfo_t cur_orbit_info, int* granuleNum );
//int CERES( char* argv[] ,int index,int ceres_fm_count);
int CERES( char* argv[],int index,int ceres_fm_count,int32*,int32*,int32*);
//int CERES( char* argv[] ,int index);
int CERES_OrbitInfo(char*argv[],int* start_index_ptr,int* end_index_ptr,OInfo_t orbit_info);
int MODIS( char* argv[],int modis_count,int unpack );
int ASTER( char* argv[],int aster_count,int unpack );
int MISR( char* argv[],int unpack );

hid_t insertDataset( hid_t const *outputFileID, hid_t *datasetGroup_ID,
                     int returnDatasetID, int rank, hsize_t* datasetDims,
                     hid_t dataType, const char* datasetName, const void* data_out);

hid_t insertDataset_comp( hid_t const *outputFileID, hid_t *datasetGroup_ID,
                          int returnDatasetID, int rank, hsize_t* datasetDims,
                          hid_t dataType, char* datasetName, void* data_out);

herr_t openFile(hid_t *file, char* inputFileName, unsigned flags );
herr_t createOutputFile( hid_t *outputFile, char* outputFileName);
herr_t createGroup( hid_t const *referenceGroup, hid_t *newGroup, char* newGroupName);
/* general type attribute creation */
hid_t attributeCreate( hid_t objectID, const char* attrName, hid_t datatypeID );
/* creates and writes a string attribute */
hid_t attrCreateString( hid_t objectID, char* name, char* value );

int32 H4ObtainLoneVgroupRef(int32 file_id, char *groupname);

int32 H4readData( int32 fileID, const char* datasetName, void** data,
                  int32 *rank, int32* dimsizes, int32 dataType,int32 *start,int32 *stride,int32 *count);
hid_t readThenWrite( const char* outDatasetName, hid_t outputGroupID, const char* inDatasetName, int32 inputDataType,
                     hid_t outputDataType, int32 inputFileID );
hid_t readThenWriteSubset( const char* outDatasetName, hid_t outputGroupID, const char* inDatasetName, int32 inputDataType,
                           hid_t outputDataType, int32 inputFileID,int32*start,int32*stride,int32*count );

char *correct_name(const char* oldname);

/* MOPITT functions */
hid_t MOPITTaddDimension ( hid_t h5dimGroupID, const char* dimName, hsize_t dimSize, const void* scaleBuffer, hid_t dimScaleNumType );
hid_t MOPITTinsertDataset( hid_t const *inputFileID, hid_t *datasetGroup_ID, char * inDatasetPath, char* outDatasetName, hid_t dataType, int returnDatasetID, unsigned int bound[2] );
herr_t MOPITT_OrbitInfo( const hid_t inputFile, OInfo_t cur_orbit_info, const char* timePath, unsigned int* start_indx_ptr,
                         unsigned int* end_indx_ptr );
/* ASTER functions */

hid_t readThenWrite_ASTER_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
                                  int32 inputFile, float unc);

/* MISR funcions */
hid_t readThenWrite_MISR_Unpack( hid_t outputGroupID, char* datasetName, char** correctedNameptr,int32 inputDataType,
                                 int32 inputFile, float scale_factor);
hid_t readThenWrite_MODIS_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
                                  int32 inputFileID);
hid_t readThenWrite_MODIS_Uncert_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
        int32 inputFileID);
hid_t readThenWrite_MODIS_GeoMetry_Unpack( hid_t outputGroupID, char* datasetName, int32 inputDataType,
        int32 inputFileID);


herr_t convert_SD_Attrs(int32 sd_id,hid_t h5grp_id,char*h5dset_name,char*sds_name);
herr_t copy_h5_attrs(int32 h4_type,int32 n_values,char* attr_name,char* attr_value,hid_t grp, char* dset_name);
herr_t H4readSDSAttr( int32 h4FileID, char* datasetName, char* attrName, void* buffer );

/* general utility functions */
char* getTime( char* pathname, int instrument );
int isLeapYear(int year);
int  h4type_to_h5type( const int32 h4type, hid_t* h5memtype);
int change_dim_attr_NAME_value(hid_t h5dset_id);
herr_t copyDimension( char* outDimName, int32 h4fileID, char* h4datasetName, hid_t h5dimGroupID, hid_t h5dsetID );
herr_t copyDimensionSubset( int32 h4fileID, char* h4datasetName, hid_t h5dimGroupID, hid_t h5dsetID,int32 s_size,char*,int );
herr_t attachDimension(hid_t h5fileID, char* dimname, hid_t h5dsetID, int dim_index);
size_t obtainDimSize(hid_t dsetID);
herr_t Generate2D_Dataset(hid_t h5_group,char* dsetname,hid_t h5_type,void* databuffer,hid_t dim0_id,hid_t dim1_id,size_t dim0_size,size_t dim1_size);

herr_t TAItoUTCconvert ( double* buffer, unsigned int size );
herr_t TAItoUTCconvert ( double* buffer, unsigned int size );
herr_t binarySearchDouble ( const double* array, double target, hsize_t size, short unsigned int firstGreater, long int* targetIndex );
herr_t getTAI93 ( GDateInfo_t date, double* TAI93timestamp );
herr_t H5allocateMemDouble ( hid_t inputFile, const char* datasetPath, void** buffer, long int* size );
herr_t initializeTimeOffset();

int comp_greg(GDateInfo_t j1, GDateInfo_t j2);
int comp_greg_utc(double greg, GDateInfo_t utc);
void get_greg(double julian, int*yearp,int*monthp,int*dayp,int*hourp,int*mmp,double*ssp);
int binarySearchUTC ( const double* array, GDateInfo_t target, int start_index, int end_index );
int utc_time_diff(struct tm start_date, struct tm end_date);

#if 0
float unc[5][15] =
{
    {0.676,0.708,0.423,0.423,0.1087,0.0348,0.0313,0.0299,0.0209,0.0159,-1,-1,-1,-1,-1},
    {1.688,1.415,0.862,0.862,0.2174,0.0696,0.0625,0.0597,0.0417,0.0318,0.006822,0.006780,0.006590,0.005693,0.005225},
    {2.25,1.89,1.15,1.15,0.290,0.0925,0.0830,0.0795,0.0556,0.0424,-1,-1,-1,-1,-1},
    {-1,-1,-1,-1,0.290,0.409,0.390,0.332,0.245,0.265,-1,-1,-1,-1,-1},
    {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1},
};

const char* metadata_gain="productmetadata.0";
const char *band_index_str_list[10] = {"01","02","3N","3B","04","05","06","07","08","09"};
const char *gain_stat_str_list[5]  = {"HGH","NOR","LO1","LO2","OFF"};


char* obtain_gain_info(char *whole_string);
short get_band_index(char *band_index_str);
short get_gain_stat(char *gain_stat_str);
#endif



#endif // TERRA_H
