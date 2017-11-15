#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mfhdf.h>  // hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>   // hdf5
#include "libTERRA.h"
#include "interp/modis/MODISLatLon.h"
#ifndef DIM_MAX
#define DIM_MAX 10
#endif

/* MY 2016-12-20, handling the MODIS files with and without MOD02HKM and MOD02QKM. */

int readThenWrite_MODIS_HR_LatLon(hid_t MODIS500mgeoGroupID,hid_t MODIS250mgeoGroupID,char* latname,char* lonname,int32 h4_type,hid_t h5_type,int32 MOD03FileID,hid_t outputFile,int modis_special_dims);

int check_MODIS_special_dimension(int32 SD_FileID);
/*      MODIS()

 DESCRIPTION:
    This function handles transferring the MODIS data from the input HDF4 granules to the
    output HDF5 granule.

 ARGUMENTS:
    argv[0]     = main program name
    argv[1]     = 1km filename
    argv[2]     = 500m filename
    argv[3]     = 250m filename
    argv[4]     = MOD03 filename
    argv[5]     = NOT USED
    argv[6]     = output filename (already exists);
    modis_count = The granule's index
    unpack      = A boolean value that specifies if unpacking should be performed

 EFFECTS:
    Modifies the output HDF5 file (already exists, the identifier is a global variable) to contain the proper MODIS data.
    Allocates memory as needed.

 RETURN:
    RET_SUCCESS     -- Successful completion
    FATAL_ERR       -- Upon a generic fatal error
    FAIL_OPEN       -- Some input file failed to open
*/

int MODIS( char* argv[],int modis_count, int unpack)
{
    /*************
     * VARIABLES *
     *************/

    const char* emissiveDim        =   "Band_1KM_Emissive_MODIS_SWATH_Type_L1B";
    const char* refSBDim           =   "Band_1KM_RefSB_MODIS_SWATH_Type_L1B";
    const char* _250M_MOD_SWATHDim =   "Band_250M_MODIS_SWATH_Type_L1B";
    const char* _500M_MOD_SWATHDim =   "Band_500M_MODIS_SWATH_Type_L1B";
    hid_t MODISrootGroupID = 0;
    hid_t MODISgranuleGroupID = 0;
    hid_t MODIS1KMdataFieldsGroupID = 0;
    hid_t MODIS500mdataFieldsGroupID = 0;
    hid_t MODIS250mdataFieldsGroupID = 0;
    hid_t MODIS1KMgeolocationGroupID = 0;
    hid_t MODIS500mgeolocationGroupID = 0;
    hid_t MODIS250mgeolocationGroupID = 0;
    herr_t status = RET_SUCCESS;
    float fltTemp = 0.0;
    intn statusn = 0;
    int retVal = RET_SUCCESS;
    char* correctedName = NULL;
    char* fileTime = NULL;
    int32 dsetID = 0;
    herr_t errStatus = 0;
    char* dimName = NULL;
    void* dimBuffer = NULL;
    char* tmpCharPtr = NULL;
    char* _1KMlatPath = NULL;
    char* _1KMlonPath = NULL;
    char* _1KMcoordEmissivePath = NULL;
    char* _1KMcoordRefSBPath = NULL;
    char* _1KMcoord_250M_Path = NULL;
    char* _1KMcoord_500M_Path = NULL;
    char* HKMlatPath = NULL;
    char* HKMlonPath = NULL;
    char* HKMcoord_250M_Path = NULL;
    char* HKMcoord_500M_Path = NULL;
    char* QKMlatPath = NULL;
    char* QKMlonPath = NULL;  
    char* QKMcoord_250M_Path = NULL;
    char granuleName[50] = {'\0'};
    char* granuleNameCorrect = NULL;
    ssize_t pathSize = 0;

    /**********************
     * 1KM data variables *
     **********************/
    /* File IDs */
    int32 _1KMFileID = 0;
    /* Dataset IDs */
    hid_t _1KMDatasetID = 0;
    hid_t _1KMUncertID = 0;
    hid_t _1KMEmissive = 0;
    hid_t _1KMEmissiveUncert = 0;
    hid_t _250Aggr1km = 0;
    hid_t _250Aggr1kmUncert = 0;
    hid_t _500Aggr1km = 0;
    hid_t _500Aggr1kmUncert = 0;
    /* Attribute IDs */
    hid_t _1KMAttrID = 0;
    /* Group IDs */
    hid_t MODIS1KMGroupID = 0;

    /***********************
     * 500m data variables *
     ***********************/
    /* File IDs */
    int32 _500mFileID = 0;
    /* Dataset IDs */
    hid_t _250Aggr500 = 0;
    hid_t _250Aggr500Uncert = 0;
    hid_t _500RefSB = 0;
    hid_t _500RefSBUncert = 0;
    /* Attribute IDs */

    /* Group IDs */
    hid_t MODIS500mGroupID = 0;

    /***********************
     * 250m data variables *
     ***********************/
    /* File IDs */
    int32 _250mFileID = 0;

    /* Dataset IDs */
    hid_t _250RefSB = 0;
    hid_t _250RefSBUncert = 0;
    /* Attribute IDs */

    /* Group IDs */
    hid_t MODIS250mGroupID = 0;


    /************************
     * MOD03 data variables *
     ************************/
    /* File IDs */
    int32 MOD03FileID = 0;

    /* Dataset IDs */

    /* Attribute IDs */

    /* Group IDs */

    /* latitude data variables */

    hid_t latitudeDatasetID = 0;
    hid_t latitudeAttrID = 0;

    /* longitude data variables */

    hid_t longitudeDatasetID = 0;
    hid_t longitudeAttrID = 0;

    /* SD sun angles */

    hid_t SDSunzenithDatasetID = 0;
    hid_t SDSunazimuthDatasetID = 0;

    /* Geometry */
    hid_t SensorZenithDatasetID = 0;
    hid_t SensorAzimuthDatasetID = 0;
    hid_t SolarAzimuthDatasetID = 0;
    hid_t SolarZenithDatasetID = 0;

    /*****************
     * END VARIABLES *
     *****************/


    /* open the input files */

    short openFailed = 0;
    /* The program will skip this granule if any of the files failed to open */
    _1KMFileID = SDstart( argv[1], DFACC_READ );
    if ( _1KMFileID < 0 )
    {
        WARN_MSG( "Unable to open 1KM file.\n\t%s\n", argv[1] );
        _1KMFileID = 0;
        openFailed = 1;
    }

    if (argv[2]!= NULL)
    {
        _500mFileID = SDstart( argv[2], DFACC_READ );
        if ( _500mFileID < 0 )
        {
            WARN_MSG("Unable to open 500m file.\n\t%s\n", argv[2]);
            _500mFileID = 0;
            openFailed = 1;
        }
    }

    if (argv[3]!= NULL)
    {
        _250mFileID = SDstart( argv[3], DFACC_READ );
        if ( _250mFileID < 0 )
        {
            WARN_MSG("Unable to open 250m file.\n\t%s\n", argv[3]);
            _250mFileID = 0;
            openFailed = 1;
        }
    }

    MOD03FileID = SDstart( argv[4], DFACC_READ );
    if ( MOD03FileID < 0 )
    {
        WARN_MSG("Unable to open MOD03 file.\n\t%s\n", argv[4]);
        MOD03FileID = 0;
        openFailed = 1;
    }

    if ( openFailed ) goto cleanupFO;

    // Some MODIS has dimension size 2040 rather than 2030, we need to use a different dimension name.
    short has_MODIS_special_dimension = check_MODIS_special_dimension(_1KMFileID);
    if(check_MODIS_special_dimension(_1KMFileID) <0) {
            
            FATAL_MSG("Failed the check MODIS special dimension check.\n");
            FATAL_MSG("The granule name is %s\n",argv[1]);

            goto cleanupFail;
    }
#if 0
if(has_MODIS_special_dimension == 1)
printf("The MODIS %s has 2040 \n",argv[1]);
else if(has_MODIS_special_dimension == 0) 
printf("The MODIS %s has 2030 \n",argv[1]);
#endif
    
    /********************************************************************************
     *                                GROUP CREATION                                *
     ********************************************************************************/

    /* outputfile already exists (created by main). Create the group directories */
    //create root MODIS group

    // Check if MODIS group exists yet
    htri_t exists = H5Lexists( outputFile, "MODIS", H5P_DEFAULT );
    if ( exists <= 0 )
    {
        if ( createGroup( &outputFile, &MODISrootGroupID, "MODIS" ) == FATAL_ERR )
        {
            FATAL_MSG("Failed to create MODIS root group.\n");
            MODISrootGroupID = 0;
            goto cleanupFail;
        }
        char *comment_name ="comment";
        char *comment_value = "The reserved dn values for uncalibrated data ranging between 65501 and "
                        "65535, as listed in Table 5.6.1 of MODIS Level 1B Product User's Guide(MOD_PR02 V6.1.12(TERRA)), "
                        "are proportionally mapped to the floating point numbers between -964.0 and -999.0, when being"
                        " converted to radiance.";

        char *time_comment_name="comment_for_GranuleTime";
        char *time_comment_value = "Under each MODIS granule group, the integer portion "
                                   "of the GranuleTime attribute value represents the Julian Date of acquisition "
                                   "in YYYYDDD form. The fractional portion represents the UTC hours and minutes of "
                                   "the Julian Date. For example, 2007184.1610 indicates the data acquisition time is at "
                                   "the 16th hour and the 10th minute (UTC) on July 3rd, 2007.";
                          

        if(H5LTset_attribute_string(outputFile,"MODIS",comment_name,comment_value) <0){
            FATAL_MSG("Failed to add the MODIS comment attribute.\n");
            goto cleanupFail;
        }

        if(H5LTset_attribute_string(outputFile,"MODIS",time_comment_name,time_comment_value) <0){
            FATAL_MSG("Failed to add the MODIS time comment attribute.\n");
            goto cleanupFail;
        }
        
    }
    else
    {
        MODISrootGroupID = H5Gopen2(outputFile, "/MODIS",H5P_DEFAULT);

        if (MODISrootGroupID <0)
        {
            FATAL_MSG("Failed to open MODIS root group.\n");
            MODISrootGroupID = 0;
            goto cleanupFail;
        }
    }


    // Create the granule group under MODIS group
    // First extract the time substring from the file path

    fileTime = getTime( argv[1], 2 );
    if ( fileTime == NULL )
    {
        FATAL_MSG("Failed to extract the time.\n");
        goto cleanupFail;
    }

    // Now create the granuleName, the name of the granule group 
    sprintf( granuleName, "granule_%s", fileTime );
    
    /* granuleName has invalid characters. Correct this. */
    granuleNameCorrect = correct_name(granuleName);

    // We can now add granuleName as a group
    if ( createGroup( &MODISrootGroupID, &MODISgranuleGroupID,granuleName ) )
    {
        FATAL_MSG("Failed to create MODIS root group.\n");
        MODISgranuleGroupID = 0;
        goto cleanupFail;
    }

    /* Add 1KM granule name */ 
    tmpCharPtr = strrchr(argv[1], '/');
    if ( tmpCharPtr == NULL )
    {
        FATAL_MSG("Failed to find a specific character within the string.\n");
        goto cleanupFail;
    }

    if (H5LTset_attribute_string(MODISrootGroupID,granuleNameCorrect,"GranuleName_1KM", tmpCharPtr+1) < 0 )
    {
        FATAL_MSG("Cannot add the file path attribute.\n");
        goto cleanupFail;
    }


    /* Add HKM granule name */
    if ( argv[2] != NULL )
    {
        tmpCharPtr = strrchr(argv[2], '/');
        if ( tmpCharPtr == NULL )
        {
            FATAL_MSG("Failed to find a specific character within the string.\n");
            goto cleanupFail;
        }

        if (H5LTset_attribute_string(MODISrootGroupID,granuleNameCorrect,"GranuleName_HKM", tmpCharPtr+1) < 0 )
        {
            FATAL_MSG("Cannot add the file path attribute.\n");
            goto cleanupFail;
        }
    }

    /* add QKM granule name */
    
    if ( argv[3] != NULL )
    {
        tmpCharPtr = strrchr(argv[3], '/');
        if ( tmpCharPtr == NULL )
        {
            FATAL_MSG("Failed to find a specific character within the string.\n");
            goto cleanupFail;
        }

        if (H5LTset_attribute_string(MODISrootGroupID,granuleNameCorrect,"GranuleName_QKM", tmpCharPtr+1)<0)
        {
            FATAL_MSG("Cannot add the file path attribute.\n");
            goto cleanupFail;
        }
    }

    /* Add MOD03 granule name */

    tmpCharPtr = strrchr(argv[4], '/');
    if ( tmpCharPtr == NULL )
    {
        FATAL_MSG("Failed to find a specific character within the string.\n");
        goto cleanupFail;
    }

    if (H5LTset_attribute_string(MODISrootGroupID,granuleNameCorrect,"GranuleName_MOD03", tmpCharPtr+1)<0)
    {
        FATAL_MSG("Cannot add the file path attribute.\n");
        goto cleanupFail;
    }


    // Add the fileTime as an attribute to the MODIS root group
    if (H5LTset_attribute_string(MODISrootGroupID,granuleNameCorrect,"GranuleTime",fileTime)<0)
    {
        FATAL_MSG("Cannot add the time stamp.\n");
        goto cleanupFail;
    }

    free(fileTime);
    fileTime = NULL;

    /* create the 1 kilometer product group */
    if ( createGroup ( &MODISgranuleGroupID, &MODIS1KMGroupID, "1KM" ) )
    {
        FATAL_MSG("Failed to create MODIS 1KM group.\n");
        MODIS1KMGroupID = 0;
        goto cleanupFail;
    }

    // create the data fields group
    if ( createGroup ( &MODIS1KMGroupID, &MODIS1KMdataFieldsGroupID, "Data Fields" ) )
    {
        FATAL_MSG("Failed to create MODIS 1KM data fields group.\n");
        MODIS1KMdataFieldsGroupID = 0;
        goto cleanupFail;
    }

    // create 1KMgeolocation fields group
    if ( createGroup( &MODIS1KMGroupID, &MODIS1KMgeolocationGroupID, "Geolocation" ) )
    {
        FATAL_MSG("Failed to create MODIS 1KMgeolocation group.\n");
        MODIS1KMgeolocationGroupID = 0;
        goto cleanupFail;
    }

    if (argv[2] !=NULL)
    {
        /* create the 500m product group */
        if ( createGroup ( &MODISgranuleGroupID, &MODIS500mGroupID, "500m" ) )
        {
            FATAL_MSG("Failed to create MODIS 500m group.\n");
            MODIS500mGroupID = 0;
            goto cleanupFail;
        }

        if ( createGroup ( &MODIS500mGroupID, &MODIS500mdataFieldsGroupID, "Data Fields" ) )
        {
            FATAL_MSG("Failed to create MODIS 500m data fields group.\n");
            MODIS500mdataFieldsGroupID = 0;
            goto cleanupFail;
        }


    }

    if (argv[3] !=NULL)
    {
        /* create the 250m product group */
        if ( createGroup ( &MODISgranuleGroupID, &MODIS250mGroupID, "250m" ) )
        {
            FATAL_MSG("Failed to create MODIS 250m group.\n");
            MODIS250mGroupID = 0;
            goto cleanupFail;
        }

        if ( createGroup ( &MODIS250mGroupID, &MODIS250mdataFieldsGroupID, "Data Fields" ) )
        {
            FATAL_MSG("Failed to create MODIS 250m data fields group.\n");
            MODIS250mdataFieldsGroupID = 0;
            goto cleanupFail;
        }
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


    /*_______________latitude data under geolocation_______________*/

    latitudeDatasetID = readThenWrite( NULL, MODIS1KMgeolocationGroupID,
                                       "Latitude",
                                       DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID,1);
    if ( latitudeDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer latitude data.\n");
        latitudeDatasetID = 0;
        goto cleanupFail;
    }

    /* save the latitude HDF5 path */
    pathSize = H5Iget_name( latitudeDatasetID, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the latitude path name.\n");
        goto cleanupFail;
    } 
    _1KMlatPath = calloc(pathSize+1, 1 );
    if ( _1KMlatPath == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(latitudeDatasetID, _1KMlatPath, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve latitude path name.\n");
        goto cleanupFail;
    } 

    /* Set latitude units */
    status = H5LTset_attribute_string(MODIS1KMgeolocationGroupID,"Latitude","units","degrees_north");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set latitude units attribute.\n");
        goto cleanupFail;
    }

    // copy dimensions over
    if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "Latitude", outputFile, latitudeDatasetID);
    else 
        errStatus = copyDimension( NULL, MOD03FileID, "Latitude", outputFile, latitudeDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");

        goto cleanupFail;
    }

    if ( latitudeDatasetID) status = H5Dclose( latitudeDatasetID );
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    latitudeDatasetID = 0;

    /*_______________longitude data under geolocation______________*/
    longitudeDatasetID = readThenWrite( NULL, MODIS1KMgeolocationGroupID,
                                        "Longitude",
                                        DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID,1);
    if ( longitudeDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer longitude data.\n");
        longitudeDatasetID = 0;
        goto cleanupFail;
    }
    status = H5LTset_attribute_string(MODIS1KMgeolocationGroupID,"Longitude","units","degrees_east");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set longitude units attribute.\n");
        goto cleanupFail;
    }

    /* save the longitude HDF5 path */
    pathSize = H5Iget_name( longitudeDatasetID, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the longitude path name.\n");
        goto cleanupFail;
    }
    _1KMlonPath = calloc(pathSize+1, 1 );
    if ( _1KMlatPath == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(longitudeDatasetID, _1KMlonPath, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve longitude path name.\n");
        goto cleanupFail;
    }

    // Copy dimensions over
    
    if(has_MODIS_special_dimension >0) 
       errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "Longitude", outputFile, longitudeDatasetID);
    else 
       errStatus = copyDimension( NULL, MOD03FileID, "Longitude", outputFile, longitudeDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }
    if ( longitudeDatasetID) status = H5Dclose( longitudeDatasetID);
    longitudeDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");




    /*=========================*/
    /* COORDINATE CREATION 1KM */
    /*=========================*/

    // Concatenate latitude and longitude path into one string
    {

        size_t latlonSize = strlen(_1KMlatPath) + strlen(_1KMlonPath);
        _1KMcoordEmissivePath = calloc( latlonSize+ strlen(emissiveDim)+ 3, 1 );
        if ( _1KMcoordEmissivePath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        _1KMcoordRefSBPath = calloc( latlonSize+ strlen(refSBDim)+ 3, 1 );
        if ( _1KMcoordRefSBPath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        _1KMcoord_250M_Path = calloc( latlonSize + strlen(_250M_MOD_SWATHDim) +3, 1);
        if ( _1KMcoord_250M_Path == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        _1KMcoord_500M_Path = calloc( latlonSize + strlen(_500M_MOD_SWATHDim) +3, 1);
        if ( _1KMcoord_500M_Path == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }


        strcpy(_1KMcoordEmissivePath, _1KMlonPath);
        strcat(_1KMcoordEmissivePath, " ");
        strcat(_1KMcoordEmissivePath, _1KMlatPath);
        strcat(_1KMcoordEmissivePath, " ");     

        /* Copy the lat/lon portion of the path to the other paths */
        strcpy(_1KMcoordRefSBPath, _1KMcoordEmissivePath);
        strcpy(_1KMcoord_250M_Path, _1KMcoordEmissivePath);
        strcpy(_1KMcoord_500M_Path, _1KMcoordEmissivePath);

        strcat(_1KMcoordEmissivePath, emissiveDim);
        strcat(_1KMcoordRefSBPath, refSBDim);
        strcat(_1KMcoord_250M_Path, _250M_MOD_SWATHDim);
        strcat(_1KMcoord_500M_Path, _500M_MOD_SWATHDim);
    }

    free(_1KMlonPath); _1KMlonPath = NULL;
    free(_1KMlatPath); _1KMlatPath = NULL;


    /*_______________EV_1KM_RefSB data_______________*/

    // IF WE ARE UNPACKING DATA
    if (unpack == 1)
    {
        if (argv[2]!=NULL)
        {

            _1KMDatasetID = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16,
                            _1KMFileID);
            if ( _1KMDatasetID == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_1KM_RefSB data.\n");
                _1KMDatasetID = 0; // Done to prevent program from trying to close this ID erroneously
                goto cleanupFail;
            }

            /*______________EV_1KM_RefSB_Uncert_Indexes______________*/
            
            _1KMUncertID = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                           DFNT_UINT8, _1KMFileID );
            if ( _1KMUncertID == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n");
                _1KMUncertID = 0;
                goto cleanupFail;
            }
        }
    }

    // ELSE WE ARE NOT UNPACKING DATA
    else
    {
        _1KMDatasetID = readThenWrite( NULL, MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16,
                                       H5T_NATIVE_USHORT, _1KMFileID,1);
        if ( _1KMDatasetID == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_RefSB data.\n");
            _1KMDatasetID = 0;
            goto cleanupFail;
        }
        /*______________EV_1KM_RefSB_Uncert_Indexes______________*/


        _1KMUncertID = readThenWrite( NULL, MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                                      DFNT_UINT8, H5T_STD_U8LE, _1KMFileID,1 );
        if ( _1KMUncertID == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n");
            _1KMUncertID = 0;
            goto cleanupFail;
        }

    }

    /* Add attributes to the datasets */
    if ( _1KMDatasetID ) // _1KMDatasetID and _1KMUncertID should be mutually inclusive
    {
        /* Add the geolocation HDF5 paths to the 1KM radiance datasets (path has already been saved) */
        errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", "coordinates", _1KMcoordRefSBPath);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }
 
        // ATTRIBUTES
        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }
        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB valid_min attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB valid_max attribute.\n");
            goto cleanupFail;
        }
 
        status = copyAttrFromName( _1KMFileID, "EV_1KM_RefSB", "band_names", _1KMDatasetID );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _1KMFileID, "EV_1KM_RefSB", "radiance_scales", _1KMDatasetID );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_1KM_RefSB", "radiance_offsets", _1KMDatasetID );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_1KM_RefSB", "reflectance_scales", _1KMDatasetID );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_1KM_RefSB", "reflectance_offsets", _1KMDatasetID );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }



        // Copy the dimensions over
        if(has_MODIS_special_dimension >0) 
            errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_1KM_RefSB", outputFile, _1KMDatasetID );
        else
            errStatus = copyDimension( NULL, _1KMFileID, "EV_1KM_RefSB", outputFile, _1KMDatasetID );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }

        H5Dclose(_1KMDatasetID);
        _1KMDatasetID = 0;
        
        errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes", "coordinates", 
                                              _1KMcoordRefSBPath);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }
     
        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_1KM_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }
        
      if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_1KM_RefSB_Uncert_Indexes", outputFile, _1KMUncertID);
      else
        errStatus = copyDimension( NULL, _1KMFileID, "EV_1KM_RefSB_Uncert_Indexes", outputFile, _1KMUncertID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    
        H5Dclose(_1KMUncertID);
        _1KMUncertID = 0;
    }


    /*___________EV_1KM_Emissive___________*/

    if (1==unpack)
    {

        _1KMEmissive = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                       DFNT_UINT16, _1KMFileID);
        if ( _1KMEmissive == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_Emissive data.\n");
            _1KMEmissive = 0;
            goto cleanupFail;
        }



        _1KMEmissiveUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
                             "EV_1KM_Emissive_Uncert_Indexes",
                             DFNT_UINT8, _1KMFileID);
        if ( _1KMEmissiveUncert == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n");
            _1KMEmissiveUncert = 0;
            goto cleanupFail;
        }


    }
    else
    {
        _1KMEmissive = readThenWrite( NULL, MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                                      DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID,1);
        if ( _1KMEmissive == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_Emissive data.\n");
            _1KMEmissive = 0;
            goto cleanupFail;
        }

        _1KMEmissiveUncert = readThenWrite( NULL, MODIS1KMdataFieldsGroupID,
                                            "EV_1KM_Emissive_Uncert_Indexes",
                                            DFNT_UINT8, H5T_STD_U8LE, _1KMFileID,1);
        if ( _1KMEmissiveUncert == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n");
            _1KMEmissiveUncert = 0;
            goto cleanupFail;
        }
    }


    // ATTRIBUTES
//________________________________________________________________________//
    status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive","units","Watts/m^2/micrometer/steradian");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive units attribute.\n");
        goto cleanupFail;
    }
    fltTemp = -999.0;
    status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive","_FillValue",&fltTemp, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive _FillValue attribute.\n");
        goto cleanupFail;
    }
    fltTemp = 0.0;
    status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive","valid_min",&fltTemp, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive valid_min attribute.\n");
        goto cleanupFail;
    }

    fltTemp = 100.0;
    status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive","valid_max",&fltTemp, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive valid_max attribute.\n");
        goto cleanupFail;
    }
   errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive", "coordinates",
                                              _1KMcoordEmissivePath);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set string attribute.\n");
        goto cleanupFail;
    }

    status = copyAttrFromName( _1KMFileID, "EV_1KM_Emissive", "band_names", _1KMEmissive );
    if ( status == FATAL_ERR )
    {
        FATAL_MSG("Failed to copy attribute.\n");
        goto cleanupFail;
    }

//_______________________________________________________________________________//
    status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive_Uncert_Indexes","units","percent");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive_Uncert_Indexes units attribute.\n");
        goto cleanupFail;
    }
    fltTemp = -999.0;
    status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_1KM_Emissive_Uncert_Indexes","_FillValue",&fltTemp, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add EV_1KM_Emissive_Uncert_Indexes _FillValue attribute.\n");
        goto cleanupFail;
    }
    errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive_Uncert_Indexes", "coordinates",
                                              _1KMcoordEmissivePath);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set string attribute.\n");
        goto cleanupFail;
    }


    // Copy the dimensions over
    if(has_MODIS_special_dimension >0) 
      errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_1KM_Emissive", outputFile, _1KMEmissive );
    else 
      errStatus = copyDimension( NULL, _1KMFileID, "EV_1KM_Emissive", outputFile, _1KMEmissive );
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }
  
    if(has_MODIS_special_dimension >0) 
      errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_1KM_Emissive_Uncert_Indexes", outputFile, _1KMEmissiveUncert);
    else 
       errStatus = copyDimension( NULL, _1KMFileID, "EV_1KM_Emissive_Uncert_Indexes", outputFile, _1KMEmissiveUncert);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // Close the identifiers related to these datasets
    if ( _1KMEmissive) status = H5Dclose(_1KMEmissive);
    _1KMEmissive = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if ( _1KMEmissiveUncert) status = H5Dclose(_1KMEmissiveUncert);
    _1KMEmissiveUncert = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    free(_1KMcoordEmissivePath); _1KMcoordEmissivePath = NULL;
    free(_1KMcoordRefSBPath); _1KMcoordRefSBPath = NULL;

    /*__________EV_250_Aggr1km_RefSB_______________*/

    if (unpack == 1)
    {

        if (argv[2]!=NULL)
        {

            _250Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                          DFNT_UINT16, _1KMFileID);
            if ( _250Aggr1km == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr1km_RefSB data.\n");
                _250Aggr1km = 0;
                goto cleanupFail;
            }
            /*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

            _250Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
                                "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                                DFNT_UINT8, _1KMFileID);
            if ( _250Aggr1kmUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n");
                _250Aggr1kmUncert = 0;
                goto cleanupFail;
            }

        }
    }

    else
    {

        _250Aggr1km = readThenWrite( NULL, MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                                     DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID,1);
        if ( _250Aggr1km == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_250_Aggr1km_RefSB data.\n");
            _250Aggr1km = 0;
            goto cleanupFail;
        }

        /*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

        _250Aggr1kmUncert = readThenWrite( NULL, MODIS1KMdataFieldsGroupID,
                                           "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                                           DFNT_UINT8, H5T_STD_U8LE, _1KMFileID,1);
        if ( _250Aggr1kmUncert == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n");
            _250Aggr1kmUncert = 0;
            goto cleanupFail;
        }


    }
    if ( unpack == 0 || argv[2] != NULL )
    {
        // ATTRIBUTES
        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB valid_min attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB valid_max attribute.\n");
            goto cleanupFail;
        }
       errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB", "coordinates",
                                              _1KMcoord_250M_Path);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }

        // This band_names was left over, KY 2017-10-22
        status = copyAttrFromName( _1KMFileID, "EV_250_Aggr1km_RefSB", "band_names", _250Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _1KMFileID, "EV_250_Aggr1km_RefSB", "radiance_scales", _250Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_250_Aggr1km_RefSB", "radiance_offsets", _250Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_250_Aggr1km_RefSB", "reflectance_scales", _250Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_250_Aggr1km_RefSB", "reflectance_offsets", _250Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_250_Aggr1km_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr1km_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }

        errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB_Uncert_Indexes", "coordinates",
                                              _1KMcoord_250M_Path);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }


        // Copy the dimensions over
    if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_250_Aggr1km_RefSB", outputFile, _250Aggr1km );
    else
        errStatus = copyDimension( NULL, _1KMFileID, "EV_250_Aggr1km_RefSB", outputFile, _250Aggr1km );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        // Copy the dimensions over
    if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_250_Aggr1km_RefSB_Uncert_Indexes", outputFile, _250Aggr1kmUncert);
    else
        errStatus = copyDimension( NULL, _1KMFileID, "EV_250_Aggr1km_RefSB_Uncert_Indexes", outputFile, _250Aggr1kmUncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    }
    // Close the identifiers related to these datasets
    if ( _250Aggr1km) status = H5Dclose(_250Aggr1km);
    _250Aggr1km = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if ( _250Aggr1kmUncert) status = H5Dclose(_250Aggr1kmUncert);
    _250Aggr1kmUncert = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    /*__________EV_500_Aggr1km_RefSB____________*/
    if (unpack == 1)
    {

        if (argv[2]!=NULL)
        {

            _500Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
                          DFNT_UINT16, _1KMFileID );
            if ( _500Aggr1km == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_Aggr1km_RefSB data.\n");
                _500Aggr1km = 0;
                goto cleanupFail;
            }

            /*__________EV_500_Aggr1km_RefSB_Uncert_Indexes____________*/

            _500Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
                                "EV_500_Aggr1km_RefSB_Uncert_Indexes",
                                DFNT_UINT8, _1KMFileID );
            if ( _500Aggr1kmUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n");
                _500Aggr1kmUncert = 0;
                goto cleanupFail;
            }


        }

    }

    else
    {
        _500Aggr1km = readThenWrite( NULL, MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
                                     DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID,1 );
        if ( _500Aggr1km == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_500_Aggr1km_RefSB data.\n");
            _500Aggr1km = 0;
            goto cleanupFail;
        }

        _500Aggr1kmUncert = readThenWrite( NULL, MODIS1KMdataFieldsGroupID,
                                           "EV_500_Aggr1km_RefSB_Uncert_Indexes",
                                           DFNT_UINT8, H5T_STD_U8LE, _1KMFileID,1 );
        if ( _500Aggr1kmUncert == FATAL_ERR )
        {
            FATAL_MSG("Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n");
            _500Aggr1kmUncert = 0;
            goto cleanupFail;
        }
    }

    if ( unpack == 0 || argv[2] != NULL )
    {
        // ATTRIBUTES
        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }
        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB valid_min attribute.\n");
            goto cleanupFail;
        }
        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB valid_max attribute.\n");
            goto cleanupFail;
        }

        errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB", "coordinates",
                                              _1KMcoord_500M_Path);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }

        // This band_names was left over, KY 2017-10-22
        status = copyAttrFromName( _1KMFileID, "EV_500_Aggr1km_RefSB", "band_names", _500Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _1KMFileID, "EV_500_Aggr1km_RefSB", "radiance_scales", _500Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_500_Aggr1km_RefSB", "radiance_offsets", _500Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_500_Aggr1km_RefSB", "reflectance_scales", _500Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _1KMFileID, "EV_500_Aggr1km_RefSB", "reflectance_offsets", _500Aggr1km );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }


        status = H5LTset_attribute_string(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS1KMdataFieldsGroupID,"EV_500_Aggr1km_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_Aggr1km_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }
        errStatus = H5LTset_attribute_string( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB_Uncert_Indexes", "coordinates",
                                              _1KMcoord_500M_Path);
        if ( errStatus < 0 )
        {
            FATAL_MSG("Failed to set string attribute.\n");
            goto cleanupFail;
        }

        // Copy the dimensions over
    if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_500_Aggr1km_RefSB", outputFile, _500Aggr1km );
    else
        errStatus = copyDimension( NULL, _1KMFileID, "EV_500_Aggr1km_RefSB", outputFile, _500Aggr1km );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _1KMFileID, "EV_500_Aggr1km_RefSB_Uncert_Indexes", outputFile, _500Aggr1kmUncert);
    else
        errStatus = copyDimension( NULL, _1KMFileID, "EV_500_Aggr1km_RefSB_Uncert_Indexes", outputFile, _500Aggr1kmUncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    }

    // Release identifiers associated with these datasets
    if ( _500Aggr1km) status = H5Dclose(_500Aggr1km);
    _500Aggr1km = 0;

    if ( _500Aggr1kmUncert) status = H5Dclose(_500Aggr1kmUncert);
    _500Aggr1kmUncert = 0;


    /*_______________Sensor Zenith under the granule group______________*/
    SensorZenithDatasetID = readThenWrite_MODIS_GeoMetry_Unpack(MODISgranuleGroupID,"SensorZenith",
                            DFNT_FLOAT32,  MOD03FileID);
    if ( SensorZenithDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer Sensor zenith data.\n");
        SensorZenithDatasetID = 0;
        goto cleanupFail;
    }

    if(has_MODIS_special_dimension>0 )
      errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "SensorZenith", outputFile, SensorZenithDatasetID);
    else 
      errStatus = copyDimension( NULL, MOD03FileID, "SensorZenith", outputFile, SensorZenithDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // Kind of hard-coded value(from user's guide) because of not enough working hours MY 2017-03-04
    status = H5LTset_attribute_string(MODISgranuleGroupID,"SensorZenith","units","degree");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SensorZenith units attribute.\n");
        goto cleanupFail;
    }


    /*_______________Sensor Azimuth under the granule group______________*/
    SensorAzimuthDatasetID = readThenWrite_MODIS_GeoMetry_Unpack(MODISgranuleGroupID,"SensorAzimuth",
                             DFNT_FLOAT32,  MOD03FileID);
    if ( SensorAzimuthDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer Sensor zenith data.\n");
        SensorAzimuthDatasetID = 0;
        goto cleanupFail;
    }

   
    if(has_MODIS_special_dimension >0) 
      errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "SensorAzimuth", outputFile, SensorAzimuthDatasetID);
    else 
      errStatus = copyDimension( NULL, MOD03FileID, "SensorAzimuth", outputFile, SensorAzimuthDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // Kind of hard-coded value(from user's guide) because of not enough working hours MY 2017-03-04
    status = H5LTset_attribute_string(MODISgranuleGroupID,"SensorAzimuth","units","degree");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SensorAzimuth units attribute.\n");
        goto cleanupFail;
    }

    /*_______________Solar Zenith under the granule group______________*/
    SolarZenithDatasetID = readThenWrite_MODIS_GeoMetry_Unpack(MODISgranuleGroupID,"SolarZenith",
                           DFNT_FLOAT32,  MOD03FileID);
    if ( SolarZenithDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer Solar zenith data.\n");
        SolarZenithDatasetID = 0;
        goto cleanupFail;
    }

    if(has_MODIS_special_dimension >0)
      errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "SolarZenith", outputFile, SolarZenithDatasetID);
    else 
      errStatus = copyDimension( NULL, MOD03FileID, "SolarZenith", outputFile, SolarZenithDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // Kind of hard-coded value(from user's guide) because of not enough working hours MY 2017-03-04
    status = H5LTset_attribute_string(MODISgranuleGroupID,"SolarZenith","units","degree");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SolarZenith units attribute.\n");
        goto cleanupFail;
    }


    /*_______________Solar Azimuth under the granule group______________*/
    SolarAzimuthDatasetID = readThenWrite_MODIS_GeoMetry_Unpack(MODISgranuleGroupID,"SolarAzimuth",
                            DFNT_FLOAT32,  MOD03FileID);
    if ( SolarAzimuthDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer Solar zenith data.\n");
        SolarAzimuthDatasetID = 0;
        goto cleanupFail;
    }

    
    if(has_MODIS_special_dimension >0) 
       errStatus = copyDimension_MODIS_Special( NULL, MOD03FileID, "SolarAzimuth", outputFile, SolarAzimuthDatasetID);
    else 
       errStatus = copyDimension( NULL, MOD03FileID, "SolarAzimuth", outputFile, SolarAzimuthDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // Kind of hard-coded value(from user's guide) because of not enough working hours MY 2017-03-04
    status = H5LTset_attribute_string(MODISgranuleGroupID,"SolarAzimuth","units","degree");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SolarAzimuth units attribute.\n");
        goto cleanupFail;
    }

    // The SD Sun fields are not needed. Still leave it for the time being in case they are needed.
#if 0
    /*_______________Sun angle under the granule group______________*/
    SDSunzenithDatasetID = readThenWrite( NULL,MODISgranuleGroupID,"SD Sun zenith",
                                          DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunzenithDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer SD Sun zenith data.\n");
        SDSunzenithDatasetID = 0;
        goto cleanupFail;
    }

    // ATTRIBUTES
    correctedName = correct_name("SD Sun zenith");
    status = H5LTset_attribute_string(MODISgranuleGroupID,correctedName,"units","radians");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SD Sun zenith units attribute.\n");
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SD Sun zenith", outputFile, SDSunzenithDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }

    // get the attribute "_FillValue" value from SD Sun zenith
    {
        int32 dsetIndex = SDnametoindex(MOD03FileID,"SD Sun zenith");
        if ( dsetIndex < 0 )
        {
            FATAL_MSG("Failed to get dataset index.\n");
            goto cleanupFail;
        }
        int32 dsetID = SDselect(MOD03FileID, dsetIndex);
        if ( dsetID < 0 )
        {
            FATAL_MSG("Failed to get dataset ID.\n");
            goto cleanupFail;
        }
        int32 attrIdx = SDfindattr(dsetID,"_FillValue");
        if ( attrIdx < 0 )
        {
            FATAL_MSG("Failed to get attribute index.\n");
            goto cleanupFail;
        }
        float tempfloat;
        statusn = SDreadattr(dsetID,attrIdx,&tempfloat);
        if ( statusn < 0 )
        {
            FATAL_MSG("Failed to read attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_float(MODISgranuleGroupID,correctedName, "_FillValue",&tempfloat,1);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add SD Sun zenith _FillValue attribute.\n");
            goto cleanupFail;
        }
    }

    free(correctedName);
    correctedName = NULL;
    if ( SDSunzenithDatasetID) status = H5Dclose(SDSunzenithDatasetID);
    SDSunzenithDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    SDSunazimuthDatasetID = readThenWrite( NULL,MODISgranuleGroupID,"SD Sun azimuth",
                                           DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunazimuthDatasetID == FATAL_ERR )
    {
        FATAL_MSG("Failed to transfer SD Sun azimuth data.\n");
        SDSunazimuthDatasetID = 0;
        goto cleanupFail;
    }

    // ATTRIBUTES

    correctedName = correct_name("SD Sun azimuth");
    status = H5LTset_attribute_string(MODISgranuleGroupID,correctedName,"units","radians");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to add SD Sun azimuth units attribute.\n");
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SD Sun azimuth", outputFile, SDSunazimuthDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }
    // get the attribute "_FillValue" value from SD Sun azimuth
    {
        int32 dsetIndex = SDnametoindex(MOD03FileID,"SD Sun azimuth");
        if ( dsetIndex < 0 )
        {
            FATAL_MSG("Failed to get dataset index.\n");
            goto cleanupFail;
        }
        int32 dsetID = SDselect(MOD03FileID, dsetIndex);
        if ( dsetID < 0 )
        {
            FATAL_MSG("Failed to get dataset ID.\n");
            goto cleanupFail;
        }
        int32 attrIdx = SDfindattr(dsetID,"_FillValue");
        if ( attrIdx < 0 )
        {
            FATAL_MSG("Failed to get attribute index.\n");
            goto cleanupFail;
        }
        float tempfloat;
        intn statusn = SDreadattr(dsetID,attrIdx,&tempfloat);
        if ( statusn < 0 )
        {
            FATAL_MSG("Failed to read attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_float(MODISgranuleGroupID,correctedName,"_FillValue",&tempfloat,1);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add SD Sun zenith _FillValue attribute.\n");
            goto cleanupFail;
        }
    }


    free(correctedName);
    correctedName = NULL;
    if ( SDSunazimuthDatasetID) status = H5Dclose(SDSunazimuthDatasetID);
    SDSunazimuthDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

#endif



    /*-------------------------------------
      ------------- 500m File -------------
      -------------------------------------*/




    /*_____________EV_250_Aggr500_RefSB____________*/

    if (argv[2] !=NULL)
    {

        if (unpack == 1)
        {
            _250Aggr500 = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID,
                          "EV_250_Aggr500_RefSB",
                          DFNT_UINT16, _500mFileID );
            if ( _250Aggr500 == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr500_RefSB data.\n");
                _250Aggr500 = 0;
                goto cleanupFail;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/

            _250Aggr500Uncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID,
                                "EV_250_Aggr500_RefSB_Uncert_Indexes",
                                DFNT_UINT8, _500mFileID );
            if ( _250Aggr500Uncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n");
                _250Aggr500Uncert = 0;
                goto cleanupFail;
            }


        }
        else
        {
            _250Aggr500 = readThenWrite( NULL, MODIS500mdataFieldsGroupID,
                                         "EV_250_Aggr500_RefSB",
                                         DFNT_UINT16,H5T_NATIVE_USHORT,  _500mFileID,1 );

            if ( _250Aggr500 == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr500_RefSB data.\n");
                _250Aggr500 = 0;
                goto cleanupFail;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/

            _250Aggr500Uncert = readThenWrite( NULL, MODIS500mdataFieldsGroupID,
                                               "EV_250_Aggr500_RefSB_Uncert_Indexes",
                                               DFNT_UINT8,H5T_STD_U8LE, _500mFileID,1 );
            if ( _250Aggr500Uncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n");
                _250Aggr500Uncert = 0;
                goto cleanupFail;
            }


        }

        // ATTRIBUTES
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB units attribute.\n");

            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB valid_min attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB valid_max attribute.\n");
            goto cleanupFail;
        }
 
         // This band_names was left over, KY 2017-10-22
        status = copyAttrFromName( _500mFileID, "EV_250_Aggr500_RefSB", "band_names", _250Aggr500 );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _500mFileID, "EV_250_Aggr500_RefSB", "radiance_scales", _250Aggr500 );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_250_Aggr500_RefSB", "radiance_offsets", _250Aggr500 );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_250_Aggr500_RefSB", "reflectance_scales", _250Aggr500 );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_250_Aggr500_RefSB", "reflectance_offsets", _250Aggr500 );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

       status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }


        // Copy the dimensions over

      if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _500mFileID, "EV_250_Aggr500_RefSB", outputFile, _250Aggr500);
      else
        errStatus = copyDimension( NULL, _500mFileID, "EV_250_Aggr500_RefSB", outputFile, _250Aggr500);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
      if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _500mFileID, "EV_250_Aggr500_RefSB_Uncert_Indexes", outputFile, _250Aggr500Uncert);
      else
        errStatus = copyDimension( NULL, _500mFileID, "EV_250_Aggr500_RefSB_Uncert_Indexes", outputFile, _250Aggr500Uncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }


        // close identifiers associated with these datasets
        if ( _250Aggr500) status = H5Dclose(_250Aggr500);
        _250Aggr500 = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if ( _250Aggr500Uncert) status = H5Dclose(_250Aggr500Uncert);
        _250Aggr500Uncert = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if (unpack == 1)
        {
            /*____________EV_500_RefSB_____________*/

            _500RefSB = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                                                    _500mFileID );
            if ( _500RefSB == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_RefSB data.\n");
                _500RefSB = 0;
                goto cleanupFail;
            }
            /*____________EV_500_RefSB_Uncert_Indexes_____________*/

            _500RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                              DFNT_UINT8, _500mFileID );
            if ( _500RefSBUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n");
                _500RefSBUncert = 0;
                goto cleanupFail;
            }

        }
        else
        {
            _500RefSB = readThenWrite( NULL, MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                                       H5T_NATIVE_USHORT, _500mFileID,1 );
            /*____________EV_500_RefSB_Uncert_Indexes_____________*/
            if ( _500RefSB == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_RefSB data.\n");
                _500RefSB = 0;
                goto cleanupFail;
            }
            _500RefSBUncert = readThenWrite( NULL, MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                                             DFNT_UINT8, H5T_STD_U8LE, _500mFileID,1 );
            if ( _500RefSBUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n");
                _500RefSBUncert = 0;
                goto cleanupFail;
            }
        }

        // ATTRIBUTES
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_500_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_500_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }
        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_500_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB valid_min attribute.\n");
            goto cleanupFail;
        } 
        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_500_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB valid_max attribute.\n");
            goto cleanupFail;
        } 



        status = copyAttrFromName( _500mFileID, "EV_500_RefSB", "band_names", _500RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }
        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _500mFileID, "EV_500_RefSB", "radiance_scales", _500RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_500_RefSB", "radiance_offsets", _500RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_500_RefSB", "reflectance_scales", _500RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _500mFileID, "EV_500_RefSB", "reflectance_offsets", _500RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }


//________________________________________________________________________________________________________//
 
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_500_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS500mdataFieldsGroupID,"EV_500_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }
//________________________________________________________________________________________________________//

        // Copy the dimensions over
        if(has_MODIS_special_dimension >0)
        errStatus = copyDimension_MODIS_Special( NULL, _500mFileID, "EV_500_RefSB", outputFile, _500RefSB);
       else 
        errStatus = copyDimension( NULL, _500mFileID, "EV_500_RefSB", outputFile, _500RefSB);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
       if(has_MODIS_special_dimension > 0)
        errStatus = copyDimension_MODIS_Special( NULL, _500mFileID, "EV_500_RefSB_Uncert_Indexes", outputFile, _500RefSBUncert);
       else
        errStatus = copyDimension( NULL, _500mFileID, "EV_500_RefSB_Uncert_Indexes", outputFile, _500RefSBUncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }

        // Release the identifiers
        if ( _500RefSB) status = H5Dclose(_500RefSB);
        _500RefSB = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if ( _500RefSBUncert ) status = H5Dclose(_500RefSBUncert);
        _500RefSBUncert = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

    } // end if ( argv[2] != NULL )


    /*-------------------------------------
      ------------- 250m File -------------
      -------------------------------------*/


    /*____________EV_250_RefSB_____________*/

    if (argv[3] != NULL)
    {
        if (unpack == 1)
        {
            _250RefSB = readThenWrite_MODIS_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
                                                    _250mFileID );
            if ( _250RefSB == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_RefSB data.\n");
                _250RefSB = 0;
                goto cleanupFail;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                              DFNT_UINT8, _250mFileID);
            if ( _250RefSBUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n");
                _250RefSBUncert = 0;
                goto cleanupFail;
            }

        }
        else
        {
            _250RefSB = readThenWrite( NULL, MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
                                       H5T_NATIVE_USHORT, _250mFileID,1 );
            if ( _250RefSB == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_RefSB data.\n");
                _250RefSB = 0;
                goto cleanupFail;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite( NULL, MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                                             DFNT_UINT8, H5T_STD_U8LE, _250mFileID,1);
            if ( _250RefSBUncert == FATAL_ERR )
            {
                FATAL_MSG("Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n");
                _250RefSBUncert = 0;
                goto cleanupFail;
            }
        }

        // ATTRIBUTES
//__________________________________________________________________________________//
        status = H5LTset_attribute_string(MODIS250mdataFieldsGroupID,"EV_250_RefSB","units","Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_RefSB units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS250mdataFieldsGroupID,"EV_250_RefSB","_FillValue",&fltTemp, 1 );
        if ( status < 0  )
        {
            FATAL_MSG("Failed to add EV_250_RefSB _FillValue attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 0.0;
        status = H5LTset_attribute_float(MODIS250mdataFieldsGroupID,"EV_250_RefSB","valid_min",&fltTemp, 1 );
        if ( status < 0  )
        {
            FATAL_MSG("Failed to add EV_250_RefSB valid_min attribute.\n");
            goto cleanupFail;
        }

        fltTemp = 900.0;
        status = H5LTset_attribute_float(MODIS250mdataFieldsGroupID,"EV_250_RefSB","valid_max",&fltTemp, 1 );
        if ( status < 0  )
        {
            FATAL_MSG("Failed to add EV_250_RefSB valid_max attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _250mFileID, "EV_250_RefSB", "band_names", _250RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        // For some researchers, reflectance should be used. However, we didn't unscale reflectance.
        // So we will copy over the radiance and reflectance scale and offset so that these
        // researchers can calculate reflectances by themselves. Maybe we can add reflectance in the future.
        // KY 2017-10-22

        status = copyAttrFromName( _250mFileID, "EV_250_RefSB", "radiance_scales", _250RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _250mFileID, "EV_250_RefSB", "radiance_offsets", _250RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _250mFileID, "EV_250_RefSB", "reflectance_scales", _250RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }

        status = copyAttrFromName( _250mFileID, "EV_250_RefSB", "reflectance_offsets", _250RefSB );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to copy attribute.\n");
            goto cleanupFail;
        }


//____________________________________________________________________________________//
        status = H5LTset_attribute_string(MODIS250mdataFieldsGroupID,"EV_250_RefSB_Uncert_Indexes","units","percent");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_RefSB_Uncert_Indexes units attribute.\n");
            goto cleanupFail;
        }
        fltTemp = -999.0;
        status = H5LTset_attribute_float(MODIS250mdataFieldsGroupID,"EV_250_RefSB_Uncert_Indexes","_FillValue",&fltTemp, 1 );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB_Uncert_Indexes _FillValue attribute.\n");
            goto cleanupFail;
        }

        // Copy the dimensions over

      if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _250mFileID, "EV_250_RefSB", outputFile, _250RefSB);
      else
        errStatus = copyDimension( NULL, _250mFileID, "EV_250_RefSB", outputFile, _250RefSB);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
       
      if(has_MODIS_special_dimension >0) 
        errStatus = copyDimension_MODIS_Special( NULL, _250mFileID, "EV_250_RefSB_Uncert_Indexes", outputFile, _250RefSBUncert);
      else
        errStatus = copyDimension( NULL, _250mFileID, "EV_250_RefSB_Uncert_Indexes", outputFile, _250RefSBUncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }

        if ( _250RefSB) status = H5Dclose(_250RefSB);
        _250RefSB = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if ( _250RefSBUncert ) status = H5Dclose(_250RefSBUncert);
        _250RefSBUncert = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");
    }



    /*-----------------------------------
     ------------ MOD03 File ------------
     ------------------------------------*/


    // We add the high-resolution lat/lon only when the data is unpacked, This is actually an advanced basic-fusion version.
    if(unpack == 1 && argv[2] != NULL )
    {
        // Add MODIS interpolation data
        if ( createGroup( &MODIS500mGroupID, &MODIS500mgeolocationGroupID, "Geolocation" ) )
        {
            FATAL_MSG("Failed to create MODIS 500m geolocation group.\n");
            MODIS500mgeolocationGroupID = 0;
            goto cleanupFail;
        }
        // Add MODIS interpolation data
        if ( createGroup( &MODIS250mGroupID, &MODIS250mgeolocationGroupID, "Geolocation" ) )
        {
            FATAL_MSG("Failed to create MODIS 250m geolocation group.\n");
            MODIS250mgeolocationGroupID = 0;
            goto cleanupFail;
        }

        if(-1 == readThenWrite_MODIS_HR_LatLon(MODIS500mgeolocationGroupID, MODIS250mgeolocationGroupID,"Latitude","Longitude",DFNT_FLOAT32,H5T_NATIVE_FLOAT,MOD03FileID,outputFile,has_MODIS_special_dimension))
        {
            FATAL_MSG("Failed to generate MODIS 250m and 500m geolocation fields.\n");
            goto cleanupFail;

        }

        /* Save the paths */
        pathSize = H5Iget_name( MODIS500mgeolocationGroupID, NULL, 0 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to get the size of the path name.\n");
            goto cleanupFail;
        }
        pathSize = pathSize+strlen("/Latitude");

        HKMlatPath = calloc(pathSize+1, 1 );
        if ( HKMlatPath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }

        pathSize = H5Iget_name( MODIS500mgeolocationGroupID, HKMlatPath, pathSize+1 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to retrieve latitude path name.\n");
            goto cleanupFail;
        }
        strncat(HKMlatPath, "/Latitude", strlen("/Latitude"));

        pathSize = H5Iget_name( MODIS500mgeolocationGroupID, NULL, 0 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to get the size of the path name.\n");
            goto cleanupFail;
        }
        pathSize = pathSize+strlen("/Longitude");

        HKMlonPath = calloc(pathSize+1, 1 );
        if ( HKMlonPath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }

        pathSize = H5Iget_name( MODIS500mgeolocationGroupID, HKMlonPath, pathSize+1 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to retrieve longitude path name.\n");
            goto cleanupFail;
        }
        strncat(HKMlonPath, "/Longitude", strlen("/Longitude"));


        /* ======================== *
         * 500M COORDINATE CREATION *
         * ======================== */
        
        {
            size_t pathLen = strlen(HKMlatPath) + strlen(HKMlonPath);
            HKMcoord_250M_Path = calloc( pathLen + strlen(_250M_MOD_SWATHDim) + 3, 1 );
            if ( HKMcoord_250M_Path == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            HKMcoord_500M_Path = calloc( pathLen + strlen(_500M_MOD_SWATHDim) + 3, 1 );
            if ( HKMcoord_500M_Path == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }

            strcpy(HKMcoord_250M_Path, HKMlonPath);
            strcat(HKMcoord_250M_Path, " ");
            strcat(HKMcoord_250M_Path, HKMlatPath);
            strcat(HKMcoord_250M_Path, " ");

            /* Copy the current 250M path to the 500M path */
            strcpy(HKMcoord_500M_Path, HKMcoord_250M_Path);

            strcat(HKMcoord_500M_Path, _500M_MOD_SWATHDim);
            strcat(HKMcoord_250M_Path, _250M_MOD_SWATHDim); 
        }
        free(HKMlonPath); HKMlonPath = NULL;
        free(HKMlatPath); HKMlatPath = NULL;
        
        /* Do the same thing for QKM */
        pathSize = H5Iget_name( MODIS250mgeolocationGroupID, NULL, 0 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to get the size of the path name.\n");
            goto cleanupFail;
        }
        pathSize = pathSize+strlen("/Latitude");

        QKMlatPath = calloc(pathSize+1, 1 );
        if ( QKMlatPath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }

        pathSize = H5Iget_name( MODIS250mgeolocationGroupID, QKMlatPath, pathSize+1 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to retrieve latitude path name.\n");
            goto cleanupFail;
        }
        strncat(QKMlatPath, "/Latitude", strlen("/Latitude"));

        pathSize = H5Iget_name( MODIS250mgeolocationGroupID, NULL, 0 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to get the size of the path name.\n");
            goto cleanupFail;
        }
        pathSize = pathSize+strlen("/Longitude");

        QKMlonPath = calloc(pathSize+1, 1 );
        if ( QKMlonPath == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }

        pathSize = H5Iget_name( MODIS250mgeolocationGroupID, QKMlonPath, pathSize+1 );
        if ( pathSize < 0 )
        {
            FATAL_MSG("Failed to retrieve longitude path name.\n");
            goto cleanupFail;
        }
        strncat(QKMlonPath, "/Longitude", strlen("/Longitude"));

        QKMcoord_250M_Path = calloc( strlen(QKMlatPath) + strlen(QKMlonPath) + strlen(_250M_MOD_SWATHDim) + 3, 1 );
        if ( QKMcoord_250M_Path == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }



        /* ======================== *
         * 250M COORDINATE CREATION *
         * ======================== */
        
        strcpy(QKMcoord_250M_Path, QKMlonPath);
        strcat(QKMcoord_250M_Path, " ");
        strcat(QKMcoord_250M_Path, QKMlatPath);
        strcat(QKMcoord_250M_Path, " ");
        strcat(QKMcoord_250M_Path, _250M_MOD_SWATHDim);

        free(QKMlonPath); QKMlonPath = NULL;
        free(QKMlatPath); QKMlatPath = NULL;

        
    }

    /* Need to add coordinates attribute to the physical datasets now that we have the HDF5 paths */

    
    if ( QKMcoord_250M_Path )
    {
        status = H5LTset_attribute_string(MODIS250mdataFieldsGroupID,"EV_250_RefSB","coordinates",QKMcoord_250M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_RefSB coordinates attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_string(MODIS250mdataFieldsGroupID,"EV_250_RefSB_Uncert_Indexes","coordinates",QKMcoord_250M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_RefSB_Uncert_Indexes coordinates attribute.\n");
            goto cleanupFail;
        }

    }
 
    if ( HKMcoord_500M_Path )
    {
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_500_RefSB","coordinates",HKMcoord_500M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB coordinates attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_500_RefSB_Uncert_Indexes","coordinates",HKMcoord_500M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_500_RefSB_Uncert_Indexes coordinates attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB","coordinates",HKMcoord_250M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB coordinates attribute.\n");
            goto cleanupFail;
        }
        status = H5LTset_attribute_string(MODIS500mdataFieldsGroupID,"EV_250_Aggr500_RefSB_Uncert_Indexes","coordinates",HKMcoord_250M_Path);
        if ( status < 0 )
        {
            FATAL_MSG("Failed to add EV_250_Aggr500_RefSB_Uncert_Indexes coordinates attribute.\n");
            goto cleanupFail;
        }


    }

    if ( 0 )
    {
cleanupFail:
        retVal = FATAL_ERR;
    }
    if ( 0 )
    {
cleanupFO:
        retVal = FAIL_OPEN;
    }

    /* release associated identifiers */
    if (latitudeAttrID !=0 ) status = H5Aclose(latitudeAttrID);
    if ( status < 0 ) WARN_MSG("H5Aclose\n");
    if (latitudeDatasetID  !=0 ) status = H5Dclose( latitudeDatasetID );
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (longitudeAttrID !=0 ) status = H5Aclose(longitudeAttrID);
    if ( status < 0 ) WARN_MSG("HADclose\n");
    if (longitudeDatasetID !=0 ) status = H5Dclose( longitudeDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (MOD03FileID !=0 ) statusn = SDend(MOD03FileID);
    if (MODIS1KMdataFieldsGroupID !=0 ) status = H5Gclose(MODIS1KMdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS1KMgeolocationGroupID !=0 ) status = H5Gclose(MODIS1KMgeolocationGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS1KMGroupID !=0 ) status = H5Gclose(MODIS1KMGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS250mdataFieldsGroupID !=0 ) status = H5Gclose(MODIS250mdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS250mgeolocationGroupID !=0 ) status = H5Gclose(MODIS250mgeolocationGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS250mGroupID !=0 ) status = H5Gclose(MODIS250mGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS500mdataFieldsGroupID !=0 ) status = H5Gclose(MODIS500mdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS500mgeolocationGroupID !=0 ) status = H5Gclose(MODIS500mgeolocationGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODIS500mGroupID !=0 ) status = H5Gclose(MODIS500mGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODISgranuleGroupID !=0 ) status = H5Gclose(MODISgranuleGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if (MODISrootGroupID !=0 ) status = H5Gclose(MODISrootGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(SensorZenithDatasetID != 0) status = H5Dclose(SensorZenithDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(SensorAzimuthDatasetID != 0) status = H5Dclose(SensorAzimuthDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(SolarAzimuthDatasetID != 0) status = H5Dclose(SolarAzimuthDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(SolarZenithDatasetID != 0) status = H5Dclose(SolarZenithDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (SDSunazimuthDatasetID !=0 ) status = H5Dclose(SDSunazimuthDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (SDSunzenithDatasetID !=0 ) status = H5Dclose(SDSunzenithDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_1KMAttrID !=0 ) status = H5Aclose(_1KMAttrID);
    if ( status < 0 ) WARN_MSG("H5Aclose\n");
    if (_1KMDatasetID !=0 ) status = H5Dclose(_1KMDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_1KMEmissive !=0 ) status = H5Dclose(_1KMEmissive);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_1KMEmissiveUncert !=0 ) status = H5Dclose(_1KMEmissiveUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_1KMFileID !=0 ) statusn = SDend(_1KMFileID);
    if (_1KMUncertID !=0 ) status = H5Dclose(_1KMUncertID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250Aggr1km !=0 ) status = H5Dclose(_250Aggr1km);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250Aggr1kmUncert !=0 ) status = H5Dclose(_250Aggr1kmUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250Aggr500 !=0 ) status = H5Dclose(_250Aggr500);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250Aggr500Uncert !=0 ) status = H5Dclose(_250Aggr500Uncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250mFileID !=0 ) statusn = SDend(_250mFileID);
    if (_250RefSB !=0 ) status = H5Dclose(_250RefSB);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_250RefSBUncert !=0 ) status = H5Dclose(_250RefSBUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_500Aggr1km !=0 ) status = H5Dclose(_500Aggr1km);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_500Aggr1kmUncert !=0 ) status = H5Dclose(_500Aggr1kmUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_500mFileID !=0 ) statusn = SDend(_500mFileID);
    if (_500RefSB !=0 ) status = H5Dclose(_500RefSB);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if (_500RefSBUncert !=0 ) status = H5Dclose(_500RefSBUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if ( correctedName != NULL ) free(correctedName);
    if ( status ) WARN_MSG("free\n");
    if ( fileTime ) free(fileTime);
    if ( dsetID ) SDendaccess(dsetID);
    if ( dimName != NULL ) free(dimName);
    if ( dimBuffer ) free(dimBuffer);
    if ( _1KMlatPath ) free(_1KMlatPath);
    if ( _1KMlonPath ) free(_1KMlonPath);
    if ( HKMlatPath ) free (HKMlatPath);
    if ( HKMlonPath ) free (HKMlonPath);
    if ( QKMlatPath ) free(QKMlatPath);
    if ( QKMlonPath ) free(QKMlonPath);
    if ( _1KMcoordEmissivePath ) free(_1KMcoordEmissivePath );
    if ( _1KMcoordRefSBPath ) free(_1KMcoordRefSBPath);
    if ( _1KMcoord_250M_Path ) free (_1KMcoord_250M_Path);
    if ( _1KMcoord_500M_Path ) free(_1KMcoord_500M_Path);
    if ( HKMcoord_250M_Path ) free (HKMcoord_250M_Path);
    if ( HKMcoord_500M_Path ) free(HKMcoord_500M_Path );
    if ( QKMcoord_250M_Path ) free (QKMcoord_250M_Path);    
    if ( granuleNameCorrect ) free ( granuleNameCorrect );

    return retVal;
}

int readThenWrite_MODIS_HR_LatLon(hid_t MODIS500mgeoGroupID,hid_t MODIS250mgeoGroupID,char* latname,char* lonname,int32 h4_type,hid_t h5_type,int32 MOD03FileID,hid_t outputFileID,int modis_special_dims)
{

    hid_t dummy_output_file_id = 0;
    int32 latRank,lonRank;
    int32 latDimSizes[DIM_MAX];
    int32 lonDimSizes[DIM_MAX];
    float* latBuffer = NULL;
    float* lonBuffer = NULL;
    hid_t datasetID;
    herr_t status;

    int i = 0;
    int nRow_1km = 0;
    int nCol_1km = 0;
    int nRow_500m = 0;
    int nCol_500m = 0;
    int scanSize  = 10;
    int nRow_250m = 0;
    int nCol_250m = 0;

    double* lat_1km_buffer = NULL;
    double* lon_1km_buffer = NULL;
    double* lat_500m_buffer = NULL;
    double* lon_500m_buffer = NULL;
    double* lat_250m_buffer = NULL;
    double* lon_250m_buffer = NULL;


    float* lat_output_500m_buffer = NULL;
    float* lon_output_500m_buffer = NULL;
    float* lat_output_250m_buffer = NULL;
    float* lon_output_250m_buffer = NULL;

    char* ll_500m_dimnames[2]= {"_20_nscans_MODIS_SWATH_Type_L1B","_2_Max_EV_frames_MODIS_SWATH_Type_L1B"};
    char* ll_250m_dimnames[2]= {"_40_nscans_MODIS_SWATH_Type_L1B","_4_Max_EV_frames_MODIS_SWATH_Type_L1B"};

    if(modis_special_dims == 1) {
        ll_500m_dimnames[0] = "_20_nscans_MODIS_SWATH_Type_L1B_2";
        ll_250m_dimnames[0] ="_40_nscans_MODIS_SWATH_Type_L1B_2";
    }
    else if(modis_special_dims == 2) {
        ll_500m_dimnames[0] = "_20_nscans_MODIS_SWATH_Type_L1B_3";
        ll_250m_dimnames[0] ="_40_nscans_MODIS_SWATH_Type_L1B_3";
    }

    status = H4readData( MOD03FileID, latname,
                         (void**)&latBuffer, &latRank, latDimSizes, h4_type,NULL,NULL,NULL );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read %s data.\n",  latname );
        if ( latBuffer != NULL ) free(latBuffer);
        return -1;
    }

    status = H4readData( MOD03FileID, lonname,
                         (void**)&lonBuffer, &lonRank, lonDimSizes, h4_type,NULL,NULL,NULL );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read %s data.\n",  lonname );
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
    if(latRank !=2 || lonRank!=2)
    {
        FATAL_MSG("The latitude and longitude array rank must be 2.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
    if(latDimSizes[0]!=lonDimSizes[0] || latDimSizes[1]!=lonDimSizes[1])
    {
        FATAL_MSG("The latitude and longitude array rank must share the same dimension sizes.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }

    /* END READ DATA. BEGIN Computing DATA */
    /* first 500 m */
    nRow_1km = latDimSizes[0];
    nCol_1km = latDimSizes[1];

    lat_1km_buffer = (double*)malloc(sizeof(double)*nRow_1km*nCol_1km);
    if(lat_1km_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lat_1km_buffer.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }


    lon_1km_buffer = (double*)malloc(sizeof(double)*nRow_1km*nCol_1km);
    if(lon_1km_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_1km_buffer.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        return -1;
    }

    nRow_500m = 2*nRow_1km;
    nCol_500m = 2*nCol_1km;
    lat_500m_buffer = (double*)malloc(sizeof(double)*nRow_500m*nCol_500m);
    if(lat_500m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lat_500m_buffer.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        return -1;
    }


    lon_500m_buffer = (double*)malloc(sizeof(double)*4*nRow_500m*nCol_500m);
    if(lon_500m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_500m_buffer.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        if (lat_500m_buffer !=NULL) free(lat_500m_buffer);
        return -1;
    }

    // Float to double
    for (i = 0; i <nRow_1km*nCol_1km; i++)
    {
        lat_1km_buffer[i] = (double)latBuffer[i];
        lon_1km_buffer[i] = (double)lonBuffer[i];
    }
    upscaleLatLonSpherical(lat_1km_buffer, lon_1km_buffer, nRow_1km, nCol_1km, scanSize, lat_500m_buffer, lon_500m_buffer);


    lat_output_500m_buffer = (float*)malloc(sizeof(float)*nRow_500m*nCol_500m);
    if(lat_output_500m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_500m_buffer.\n");
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        if (lat_500m_buffer !=NULL) free(lat_500m_buffer);
        if (lon_500m_buffer !=NULL) free(lon_500m_buffer);
        return -1;
    }
    for (i = 0; i <nRow_500m*nCol_500m; i++)
        lat_output_500m_buffer[i] = (float)lat_500m_buffer[i];

    hsize_t temp[DIM_MAX];
    for ( i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) (2*latDimSizes[i]);

    datasetID = insertDataset( &dummy_output_file_id, &MODIS500mgeoGroupID, 1, latRank,
                               temp, h5_type, latname, lat_output_500m_buffer );

    if ( datasetID == FATAL_ERR )
    {
        FATAL_MSG("Error writing %s dataset.\n", latname );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lat_output_500m_buffer);
        return -1;
    }
    // semi-hard-code here.
    if(attachDimension(outputFileID,ll_500m_dimnames[0],datasetID,0) <0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n",ll_500m_dimnames[0] );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lat_output_500m_buffer);
        return -1;
    }
    if(attachDimension(outputFileID,ll_500m_dimnames[1],datasetID,1)<0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n", ll_500m_dimnames[1] );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lat_output_500m_buffer);
        return -1;
    }

    H5Dclose(datasetID);

    lon_output_500m_buffer = (float*)malloc(sizeof(float)*nRow_500m*nCol_500m);
    if(lon_output_500m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_500m_buffer.\n");
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_output_500m_buffer);
        return -1;
    }
    for (i = 0; i <nRow_500m*nCol_500m; i++)
        lon_output_500m_buffer[i] = (float)lon_500m_buffer[i];


    datasetID = insertDataset( &dummy_output_file_id, &MODIS500mgeoGroupID, 1, lonRank,
                               temp, h5_type, lonname, lon_output_500m_buffer );

    if ( datasetID == FATAL_ERR )
    {
        FATAL_MSG("Error writing %s dataset.\n", lonname );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_output_500m_buffer);
        free(lon_output_500m_buffer);
        return -1;
    }
    // semi-hard-code here.
    if(attachDimension(outputFileID,ll_500m_dimnames[0],datasetID,0) <0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n",ll_500m_dimnames[0] );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_output_500m_buffer);
        free(lon_output_500m_buffer);
        return -1;
    }
    if(attachDimension(outputFileID,ll_500m_dimnames[1],datasetID,1)<0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n", ll_500m_dimnames[1] );
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_output_500m_buffer);
        free(lon_output_500m_buffer);
        return -1;
    }

    H5Dclose(datasetID);

    // Nor used anymore, free.
    free(latBuffer);
    free(lonBuffer);
    free(lat_1km_buffer);
    free(lon_1km_buffer);
    free(lat_output_500m_buffer);
    free(lon_output_500m_buffer);


    nRow_250m = 2*nRow_500m;
    nCol_250m = 2*nCol_500m;
    lat_250m_buffer = (double*)malloc(sizeof(double)*nRow_250m*nCol_250m);
    if(lat_250m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lat_250m_buffer.\n");
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        return -1;
    }

    lon_250m_buffer = (double*)malloc(sizeof(double)*nRow_250m*nCol_250m);
    if(lon_250m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_250m_buffer.\n");
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_250m_buffer);
        return -1;
    }

    upscaleLatLonSpherical(lat_500m_buffer, lon_500m_buffer, nRow_500m, nCol_500m, scanSize, lat_250m_buffer, lon_250m_buffer);

    free(lat_500m_buffer);
    free(lon_500m_buffer);
    lat_output_250m_buffer = (float*)malloc(sizeof(float)*nRow_250m*nCol_250m);
    if(lat_output_250m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lat_output_250m_buffer.\n");
        free(lat_250m_buffer);
        free(lon_250m_buffer);
        return -1;
    }

    lon_output_250m_buffer = (float*)malloc(sizeof(float)*nRow_250m*nCol_250m);
    if(lon_output_500m_buffer == NULL)
    {
        FATAL_MSG("Cannot allocate lon_output_250m_buffer.\n");
        free(lat_250m_buffer);
        free(lon_250m_buffer);
        free(lat_output_250m_buffer);
        return -1;
    }

    for (i = 0; i <nRow_250m*nCol_250m; i++)
    {
        lon_output_250m_buffer[i] = (float)lon_250m_buffer[i];
        lat_output_250m_buffer[i] = (float)lat_250m_buffer[i];
    }

    free(lat_250m_buffer);
    free(lon_250m_buffer);

    for ( i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) (4*latDimSizes[i]);

    datasetID = insertDataset( &dummy_output_file_id, &MODIS250mgeoGroupID, 1, latRank,
                               temp, h5_type, latname, lat_output_250m_buffer );

    if ( datasetID == FATAL_ERR )
    {
        FATAL_MSG("Error writing %s dataset.\n", latname );
        free(lat_output_250m_buffer);
        free(lon_output_250m_buffer);
        return -1;
    }

    free(lat_output_250m_buffer);

    if(attachDimension(outputFileID,ll_250m_dimnames[0],datasetID,0) <0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n",ll_250m_dimnames[0] );
        free(lon_250m_buffer);
        return -1;
    }
    if(attachDimension(outputFileID,ll_250m_dimnames[1],datasetID,1)<0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n", ll_250m_dimnames[1] );
        free(lon_250m_buffer);
        return -1;
    }

    H5Dclose(datasetID);

    datasetID = insertDataset( &dummy_output_file_id, &MODIS250mgeoGroupID, 1, lonRank,
                               temp, h5_type, lonname, lon_output_250m_buffer );

    if ( datasetID == FATAL_ERR )
    {
        FATAL_MSG("Error writing %s dataset.\n", latname );
        free(lon_output_250m_buffer);
        return -1;
    }

    free(lon_output_250m_buffer);

    if(attachDimension(outputFileID,ll_250m_dimnames[0],datasetID,0) <0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n",ll_250m_dimnames[0] );
        return -1;
    }
    if(attachDimension(outputFileID,ll_250m_dimnames[1],datasetID,1)<0)
    {
        FATAL_MSG("Error  opening dimension dataset ID %s dataset.\n", ll_250m_dimnames[1] );
        return -1;
    }

    H5Dclose(datasetID);

    //Insert Latitude/Longitude units
    if(H5LTset_attribute_string(MODIS500mgeoGroupID,"Longitude","units","degrees_east")<0) 
    {
        FATAL_MSG("Failed to set longitude units attribute.\n");
        return -1;
    }
    if(H5LTset_attribute_string(MODIS500mgeoGroupID,"Latitude","units","degrees_north")<0) 
    {
        FATAL_MSG("Failed to set latitude units attribute.\n");
        return -1;
    }

    if(H5LTset_attribute_string(MODIS250mgeoGroupID,"Longitude","units","degrees_east")<0) 
    {
        FATAL_MSG("Failed to set longitude units attribute.\n");
        return -1;
    }

    if(H5LTset_attribute_string(MODIS250mgeoGroupID,"Latitude","units","degrees_north")<0) 
    {
        FATAL_MSG("Failed to set latitude units attribute.\n");
        return -1;
    }

    return 0;

}

int check_MODIS_special_dimension(int32 MOD1KMID) {

    int32 emissive_index = SDnametoindex(MOD1KMID,"EV_1KM_Emissive");
    if(emissive_index <0) {
        FATAL_MSG("Error cannot find the SDS %s\n","EV_1KM_Emissive");
        return -1;
    }

    int32 sds_id = SDselect(MOD1KMID,emissive_index);
    if(sds_id <0) {
        FATAL_MSG("Error cannot select the SDS %s\n","EV_1KM_Emissive");
        return -1;
    }

    int32 dimsizes[MAX_VAR_DIMS];
    int32 rank = 0;
    if(SDgetinfo(sds_id,NULL,&rank,dimsizes,NULL,NULL) <0) {
        SDendaccess(sds_id);
        FATAL_MSG("Error cannot select the SDS %s\n","EV_1KM_Emissive");
        return -1;
    }

    SDendaccess(sds_id);
    if(rank <3) {
        FATAL_MSG("SDS %s should have rank >=3. \n","EV_1KM_Emissive");
        return -1;
    }

    if(dimsizes[1] == 2030) 
        return 0;
    else if(dimsizes[1] == 2040)
        return 1;
    else if(dimsizes[1] ==2050)
        return 2;
    else {
        FATAL_MSG("The second dimension of SDS %s should be either 2030 or 2040. \n","EV_1KM_Emissive");
        FATAL_MSG("The dimension size  is %d  \n",dimsizes[1]);
        return -1;
    }

}

