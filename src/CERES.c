#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>
#include <hdf.h>    // hdf4
#include <hdf5.h>   // hdf5
#include "libTERRA.h"
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10
//int get_subset_index(int start_fix,int start_index,int end_index,GDateInfo_t orbit_time,double* jd);
int obtain_start_end_index(int* sindex_ptr,int* endex_ptr,double *jd,int32 size,OInfo_t orbit_info);
herr_t CERESinsertAttrs( hid_t objectID, char* long_nameVal, char* unitsVal, float valid_rangeMin, float valid_rangeMax );
//int CERES_OrbitInfo(char*argv[],int* start_index_ptr,int* end_index_ptr,OInfo_t orbit_info);


int CERES( char* argv[],int index,int ceres_fm_count,int32*c_start,int32*c_stride,int32*c_count)
{

#define NUM_TIME 3
#define NUM_VIEWING 4
#define NUM_FILT 4
#define NUM_UNFILT 3

    /*************
     * VARIABLES *
     *************/
    int32 fileID = 0;
    int32 h4Type;
    hid_t h5Type;
    hid_t rootID_g = 0;
    hid_t granuleID_g = 0;
    hid_t FMID_g = 0;
    hid_t radianceID_g = 0;
    hid_t geolocationID_g = 0;
    hid_t viewingAngleID_g = 0;
    hid_t generalDsetID_d = 0;

    herr_t status = 0;

    char* fileTime = NULL;
    char* correctName = NULL;
    short fail = 0;
    char* tmpCharPtr = NULL;
    const char* UNITS1 = {"Watts/m^2/steradian"};
    const char* UNITS2 = {"Watts/m^2/micrometer/steradian"};
    char* inTimePosName[NUM_TIME] = {"Time of observation", "Colatitude of CERES FOV at surface",
                                     "Longitude of CERES FOV at surface"
                                    };
    char* outTimePosName[NUM_TIME] = {"Time of observation", "Latitude", "Longitude" };

    char* inViewingAngles[NUM_VIEWING] =
    {
        "CERES viewing zenith at surface", "CERES solar zenith at surface", "CERES relative azimuth at surface",
        "CERES viewing azimuth at surface wrt North"
    };
    char* outViewingAngles[NUM_VIEWING] = {"Viewing Zenith", "Solar Zenith", "Relative Azimuth", "Viewing Azimuth" };

    char* inFilteredRadiance[NUM_FILT] =
    {
        "CERES TOT filtered radiance - upwards", "CERES SW filtered radiance - upwards",
        "CERES WN filtered radiance - upwards", "Radiance and Mode flags"
    };
    char* outFilteredRadiance[NUM_FILT] =
    {
        "TOT Filtered Radiance", "SW Filtered Radiance", "WN Filtered Radiance", "Radiance Mode Flags"
    };

    char* inUnfilteredRadiance[NUM_UNFILT] =
    {
        "CERES SW radiance - upwards", "CERES LW radiance - upwards", "CERES WN radiance - upwards"
    };
    char* outUnfilteredRadiance[NUM_UNFILT] =
    {
        "SW Radiance", "LW Radiance", "WN Radiance"
    };

    /* open the input file */
    fileID = SDstart( argv[2], DFACC_READ );
    if ( fileID < 0 )
    {
        FATAL_MSG("Unable to open CERES file.\n");
        return (EXIT_FAILURE);
    }

    /* outputfile already exists (created by main). Create the group directories */
    //create root CERES group
    if (index == 1)
    {
        if(ceres_fm_count == 1)
        {
            if ( createGroup( &outputFile, &rootID_g, "CERES" ) )
            {
                FATAL_MSG("Failed to create CERES root group.\n");
                rootID_g = 0;
                goto cleanupFail;
            }
        }
        else
        {
            rootID_g = H5Gopen2( outputFile, "CERES", H5P_DEFAULT );
            if ( rootID_g < 0 )
            {
                FATAL_MSG("Unable to open CERES root group.\n");
                rootID_g = 0;
                goto cleanupFail;
            }
        }

        

        fileTime = getTime( argv[2], 1 );

        if(H5LTset_attribute_string(outputFile,"CERES","GranuleTime",fileTime)<0)
        {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            goto cleanupFail;
        }
        free(fileTime);
        fileTime = NULL;

        if ( createGroup( &rootID_g, &granuleID_g,argv[3] ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create CERES granule group.\n",__FILE__,__func__,__LINE__);
            granuleID_g = 0;
            goto cleanupFail;
        }

        if ( createGroup( &granuleID_g, &FMID_g, "FM1" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            FMID_g = 0;
            goto cleanupFail;
        }

        
        /* Insert granule name as attribute into FM1 or FM2 group */
        tmpCharPtr = strrchr( argv[2], '/' );
        if ( tmpCharPtr == NULL )
        {
            FATAL_MSG("Failed to find character within a string.\n");
            goto cleanupFail;
        }
        if(H5LTset_attribute_string(granuleID_g,"FM1","GranuleName",tmpCharPtr + 1)<0)
        {
            FATAL_MSG("Failed to add CERES granule name.\n");
            goto cleanupFail;
        }
    }
    else if(index == 2)
    {
        rootID_g = H5Gopen2( outputFile, "CERES", H5P_DEFAULT );
        if ( rootID_g < 0 )
        {
            FATAL_MSG("Unable to open CERES root group.\n");
            rootID_g = 0;
            goto cleanupFail;
        }
        granuleID_g = H5Gopen2( rootID_g, argv[3], H5P_DEFAULT );
        if ( rootID_g < 0 )
        {
            FATAL_MSG("Unable to open CERES root group.\n");
            rootID_g = 0;
            goto cleanupFail;
        }

        if ( createGroup( &granuleID_g, &FMID_g, "FM2" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            goto cleanupFail;
        }

        
        /* Insert granule name as attribute into FM1 or FM2 group */
        tmpCharPtr = strrchr( argv[2], '/' );
        if ( tmpCharPtr == NULL )
        {
            FATAL_MSG("Failed to find character within a string.\n");
            goto cleanupFail;
        }
        if(H5LTset_attribute_string(granuleID_g,"FM2","GranuleName",tmpCharPtr + 1)<0)
        {
            FATAL_MSG("Failed to add CERES granule name.\n");
            goto cleanupFail;
        }
    }

    else
    {
        FATAL_MSG("The CERES granule index should be either 1 or 2.\n");
        goto cleanupFail;
    }



    // create the data fields group
    if ( createGroup ( &FMID_g, &radianceID_g, "Radiances" ) )
    {
        FATAL_MSG("CERES create data fields group failure.\n");
        radianceID_g = 0;
        goto cleanupFail;
    }

    // create geolocation fields
    if ( createGroup( &FMID_g, &geolocationID_g, "Time_and_Position" ) )
    {
        FATAL_MSG("Failed to create CERES geolocation group.\n");
        geolocationID_g = 0;
        goto cleanupFail;
    }

    // create Viewing angle fields
    if ( createGroup( &FMID_g, &viewingAngleID_g, "Viewing_Angles" ) )
    {
        FATAL_MSG("Failed to create CERES ViewingAngle group.\n");
        viewingAngleID_g = 0;
        goto cleanupFail;
    }

    /************************
     * Time and Position *
     ************************/
    int i;

    for ( i = 0; i < NUM_TIME; i++ )
    {

        switch ( i )
        {
        case 0:
            h4Type = DFNT_FLOAT64;
            h5Type = H5T_NATIVE_DOUBLE;
            break;
        case 1:
            h4Type = DFNT_FLOAT32;
            h5Type = H5T_NATIVE_FLOAT;
            break;
        case 2:
            h4Type = DFNT_FLOAT32;
            h5Type = H5T_NATIVE_FLOAT;
            break;
        default:
            FATAL_MSG("Failed to set the appropriate HDF4 and HDF5 types.\n");
            goto cleanupFail;
        }

        generalDsetID_d = readThenWriteSubset( outTimePosName[i], geolocationID_g, inTimePosName[i], h4Type, h5Type,
                                               fileID,c_start,c_stride,c_count);


        if ( generalDsetID_d == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to insert CERES %s dataset.\n", inTimePosName[i]);
            generalDsetID_d = 0;
            goto cleanupFail;
        }
        // copy the dimension scales

        /* NOTE
         * The dataset name in output HDF5 file is not the same as dataset name in input HDF4 file. Name is passed
         * according to the correct_name function.
         */
        if(index == 1)
            status = copyDimensionSubset( fileID, inTimePosName[i], outputFile, generalDsetID_d,*c_count,"_FM1_",ceres_fm_count );
        else
            status = copyDimensionSubset( fileID, inTimePosName[i], outputFile, generalDsetID_d,*c_count,"_FM2_",ceres_fm_count );
        if ( status == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions for %s.\n", inTimePosName[i]);
            goto cleanupFail;
        }

        H5Dclose(generalDsetID_d);
        generalDsetID_d = 0;

        char* NewcorrectName = correct_name(outTimePosName[i]);
        convert_SD_Attrs(fileID,geolocationID_g,NewcorrectName,inTimePosName[i]);
        free(NewcorrectName);

        // Quick way to change the attribute unit of latitude and longitude
        // LEAVE this block of code, we may need this later.
#if 0
        if(i == 1)// latitude
            H5LTset_attribute_string(geolocationID_g,"Latitude","units","degrees_north");
        else if( i==2)
            H5LTset_attribute_string(geolocationID_g,"Longitude","units","degrees_east");
#endif

    }

    /******************
     * VIEWING ANGLES *
     ******************/

    for ( i = 0; i < NUM_VIEWING; i++ )
    {
        h4Type = DFNT_FLOAT32;
        h5Type = H5T_NATIVE_FLOAT;

        generalDsetID_d = readThenWriteSubset( outViewingAngles[i], viewingAngleID_g, inViewingAngles[i], h4Type, h5Type, fileID,c_start,c_stride,c_count);
        if ( generalDsetID_d == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to insert \"%s\" dataset.\n", inViewingAngles[i]);
            generalDsetID_d = 0;
            goto cleanupFail;
        }

        if(index == 1)
            status = copyDimensionSubset( fileID, inViewingAngles[i], outputFile, generalDsetID_d,*c_count,"_FM1_",ceres_fm_count );
        else
            status = copyDimensionSubset( fileID, inViewingAngles[i], outputFile, generalDsetID_d,*c_count,"_FM2_",ceres_fm_count );
        if ( status == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions for %s.\n", inViewingAngles[i] );
            goto cleanupFail;
        }

        H5Dclose(generalDsetID_d);
        generalDsetID_d = 0;
        char* NewcorrectName = correct_name(outViewingAngles[i]);
        convert_SD_Attrs(fileID,viewingAngleID_g,NewcorrectName,inViewingAngles[i]);
        free(NewcorrectName);
    }


    /**********************
     * FILTERED RADIANCES *
     **********************/

    for ( i = 0; i < NUM_FILT; i++ )
    {
        if ( i >=0 && i <=2 )
        {
            h4Type = DFNT_FLOAT32;
            h5Type = H5T_NATIVE_FLOAT;
        }
        else if ( i == 3 )
        {
            h4Type = DFNT_INT32;
            h5Type = H5T_NATIVE_INT;
        }

        generalDsetID_d = readThenWriteSubset( outFilteredRadiance[i], radianceID_g, inFilteredRadiance[i], h4Type, h5Type, fileID,c_start,c_stride,c_count);
        if ( generalDsetID_d == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to insert \"%s\" dataset.\n", inFilteredRadiance[i]);
            generalDsetID_d = 0;
            goto cleanupFail;
        }

        if(index == 1)
            status = copyDimensionSubset( fileID, inFilteredRadiance[i], outputFile, generalDsetID_d,*c_count,"_FM1_",ceres_fm_count );
        else
            status = copyDimensionSubset( fileID, inFilteredRadiance[i], outputFile, generalDsetID_d,*c_count,"_FM2_",ceres_fm_count );
        if ( status == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions for %s.\n", inFilteredRadiance[i] );
            goto cleanupFail;
        }

        H5Dclose(generalDsetID_d);
        generalDsetID_d = 0;
        char* NewcorrectName = correct_name(outFilteredRadiance[i]);
        convert_SD_Attrs(fileID,radianceID_g,NewcorrectName,inFilteredRadiance[i]);
        free(NewcorrectName);
    }

    /************************
     * UNFILTERED RADIANCES *
     ************************/

    for ( i = 0; i < NUM_UNFILT; i++ )
    {
        h4Type = DFNT_FLOAT32;
        h5Type = H5T_NATIVE_FLOAT;

        generalDsetID_d = readThenWriteSubset( outUnfilteredRadiance[i], radianceID_g, inUnfilteredRadiance[i], h4Type, h5Type, fileID,c_start,c_stride,c_count);
        if ( generalDsetID_d == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to insert \"%s\" dataset.\n", inUnfilteredRadiance[i]);
            generalDsetID_d = 0;
            goto cleanupFail;
        }

        if(index == 1)
            status = copyDimensionSubset( fileID, inUnfilteredRadiance[i], outputFile, generalDsetID_d,*c_count,"_FM1_",ceres_fm_count );
        else
            status = copyDimensionSubset( fileID, inUnfilteredRadiance[i], outputFile, generalDsetID_d,*c_count,"_FM2_",ceres_fm_count );

        if ( status == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions for %s.\n", inUnfilteredRadiance[i] );
            goto cleanupFail;
        }

        H5Dclose(generalDsetID_d);
        generalDsetID_d = 0;
        char* NewcorrectName = correct_name(outUnfilteredRadiance[i]);
        convert_SD_Attrs(fileID,radianceID_g,NewcorrectName,inUnfilteredRadiance[i]);
        free(NewcorrectName);
    }

    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( fileID ) SDend(fileID);
    if ( fileTime ) free(fileTime);
    if ( rootID_g ) H5Gclose(rootID_g);
    if ( granuleID_g) H5Gclose(granuleID_g);
    if ( FMID_g) H5Gclose(FMID_g);
    if ( radianceID_g ) H5Gclose(radianceID_g);
    if ( geolocationID_g ) H5Gclose(geolocationID_g);
    if ( viewingAngleID_g ) H5Gclose(viewingAngleID_g);
    if ( generalDsetID_d ) H5Dclose(generalDsetID_d);
    if ( correctName ) free(correctName);

    if ( fail ) return EXIT_FAILURE;

    return EXIT_SUCCESS;




#if 0
    /*************
     * VARIABLES *
     *************/
    int32 fileID = 0;
    hid_t timeDatasetID = 0;
    hid_t SWFilteredDatasetID = 0;
    hid_t WNFilteredDatasetID = 0;
    hid_t TOTFilteredDatasetID = 0;
    hid_t RadianceModeFlagDatasetID = 0;
    hid_t SWUnfilteredDatasetID = 0;
    hid_t WNUnfilteredDatasetID = 0;
    hid_t LWUnfilteredDatasetID = 0;


    hid_t colatitudeDatasetID = 0;
    hid_t longitudeDatasetID = 0;
    hid_t viewZenithDatasetID = 0;
    hid_t solarZenithDatasetID = 0;
    hid_t relativeAzimuthDatasetID = 0;
    hid_t viewAzimuthDatasetID = 0;

    hid_t CERESrootID = 0;
    hid_t CERESgranuleID = 0;
    hid_t CERESdataFieldsID = 0;
    hid_t CERESgeolocationID = 0;
    hid_t CERESViewingangleID = 0;
    herr_t status = EXIT_SUCCESS;
    char* fileTime = NULL;
    short fail = 0;


    /*****************
     * END VARIABLES *
     *****************/

    /* open the input file */
    fileID = SDstart( argv[1], DFACC_READ );
    if ( fileID < 0 )
    {
        FATAL_MSG("Unable to open CERES file.\n");
        return (EXIT_FAILURE);
    }

    /* outputfile already exists (created by main). Create the group directories */
    //create root CERES group
    if ( index == 1 )
    {
        if ( createGroup( &outputFile, &CERESrootID, "CERES" ) )
        {
            FATAL_MSG("Failed to create CERES root group.\n");
            CERESrootID = 0;
            goto cleanupFail;
        }


        if(H5LTset_attribute_string(outputFile,"CERES","FilePath",argv[1])<0)
        {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            goto cleanupFail;
        }

        fileTime = getTime( argv[1], 1 );

        if(H5LTset_attribute_string(outputFile,"CERES","GranuleTime",fileTime)<0)
        {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            goto cleanupFail;
        }
        free(fileTime);
        fileTime = NULL;

        if ( createGroup( &CERESrootID, &CERESgranuleID, "FM1" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            CERESgranuleID = 0;
            goto cleanupFail;
        }
    }
    else if(index == 2)
    {
        CERESrootID = H5Gopen2( outputFile, "CERES", H5P_DEFAULT );
        if ( CERESrootID < 0 )
        {
            FATAL_MSG("Unable to open CERES root group.\n");
            CERESrootID = 0;
            goto cleanupFail;
        }
        if ( createGroup( &CERESrootID, &CERESgranuleID, "FM2" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            goto cleanupFail;
        }
    }

    else
    {
        FATAL_MSG("The CERES granule index should be either 1 or 2.\n");
        goto cleanupFail;
    }


    // create the data fields group
    if ( createGroup ( &CERESgranuleID, &CERESdataFieldsID, "Data Fields" ) )
    {
        FATAL_MSG("CERES create data fields group failure.\n");
        CERESdataFieldsID = 0;
        goto cleanupFail;
    }

    // create geolocation fields
    if ( createGroup( &CERESgranuleID, &CERESgeolocationID, "Geolocation" ) )
    {
        FATAL_MSG("Failed to create CERES geolocation group.\n");
        CERESgeolocationID = 0;
        goto cleanupFail;
    }

    // create Viewing angle fields
    if ( createGroup( &CERESgranuleID, &CERESViewingangleID, "ViewingAngle" ) )
    {
        FATAL_MSG("Failed to create CERES ViewingAngle group.\n");
        CERESgeolocationID = 0;
        goto cleanupFail;
    }

    /************************
     *Time_Of_Observation *
     ************************/
    timeDatasetID = readThenWrite( CERESgeolocationID, "Time_Of_Observation", DFNT_FLOAT64, H5T_NATIVE_DOUBLE,
                                   fileID,c_start,c_stride,c_end );

    if ( timeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES time dataset.\n");
        timeDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "Time_Of_Observation", outputFile, timeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(timeDatasetID);
    timeDatasetID = 0;

    /************************
    *Viewing_Zenith *
    ************************/
    viewZenithDatasetID = readThenWrite( CERESViewingangleID, "CERES viewing zenith at surface", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                         fileID );

    if ( viewZenithDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES viewing zenith at surface dataset.\n");
        viewZenithDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES viewing zenith at surface", outputFile, viewZenithDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(viewZenithDatasetID);
    viewZenithDatasetID = 0;

    /************************
     *Solar_Zenith *
     ************************/
    solarZenithDatasetID = readThenWrite( CERESViewingangleID, "CERES solar zenith at surface", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                          fileID );

    if ( solarZenithDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES solar zenith at surface dataset.\n");
        solarZenithDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES solar zenith at surface", outputFile, solarZenithDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(solarZenithDatasetID);
    solarZenithDatasetID = 0;

    /************************
     *Relative_Azimuth *
     ************************/
    relativeAzimuthDatasetID = readThenWrite( CERESViewingangleID, "CERES relative azimuth at surface", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                               fileID );

    if ( relativeAzimuthDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES relative azimuth at surface dataset.\n");
        relativeAzimuthDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES relative azimuth at surface", outputFile, relativeAzimuthDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(relativeAzimuthDatasetID);
    relativeAzimuthDatasetID = 0;

    /************************
     *Viewing_Azimuth *
     ************************/
    viewAzimuthDatasetID = readThenWrite( CERESViewingangleID, "CERES viewing azimuth at surface wrt North", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                          fileID );

    if ( viewAzimuthDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES viewing azimuth at surface wrt North dataset.\n");
        viewAzimuthDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES viewing azimuth at surface wrt North", outputFile, viewAzimuthDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(viewAzimuthDatasetID);
    viewAzimuthDatasetID = 0;



    /***************************************
     * CERES SW Filtered Radiances Upwards *
     ***************************************/
    SWFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES SW filtered radiance - upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                         fileID );
    if ( SWFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES SW Filtered Radiances Upwards dataset.\n");
        SWFilteredDatasetID = 0;
        goto cleanupFail;
    }

    // The valid_range is hard-coded. This is not good from software point-of-view. Maybe OK if this is from user's guide.
    status = CERESinsertAttrs( SWFilteredDatasetID, "CERES SW filtered radiance - upwards", UNITS1, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW Filtered Radiances Upwards.\n");
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES SW filtered radiance - upwards", outputFile, SWFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(SWFilteredDatasetID);
    SWFilteredDatasetID = 0;


    /*********************************
     * WN Filtered Radiances Upwards *
     *********************************/
    WNFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES WN filtered radiance - upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                         fileID );
    if ( WNFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards dataset.\n");
        WNFilteredDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( WNFilteredDatasetID, "CERES WN filtered radiance - upwards", UNITS2, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( NULL, fileID, "CERES WN filtered radiance - upwards", outputFile, WNFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(WNFilteredDatasetID);
    WNFilteredDatasetID = 0;


    /**********************************
     * TOT Filtered Radiances Upwards *
     **********************************/
    TOTFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES TOT filtered radiance - upwards", DFNT_FLOAT32,
                                          H5T_NATIVE_FLOAT, fileID );
    if ( TOTFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards dataset.\n");
        TOTFilteredDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( TOTFilteredDatasetID, "CERES TOT filtered radiance - upwards", UNITS1, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( NULL, fileID, "CERES TOT filtered radiance - upwards", outputFile, TOTFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(TOTFilteredDatasetID);
    TOTFilteredDatasetID = 0;

    /**********************************
     * Radiance and Mode flags *
     **********************************/
    RadianceModeFlagDatasetID = readThenWrite( CERESdataFieldsID, "Radiance and Mode flags", DFNT_INT32,
                                H5T_NATIVE_INT, fileID );
    if ( RadianceModeFlagDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES radiance and Mode flags.\n");
        RadianceModeFlagDatasetID = 0;
        goto cleanupFail;
    }

    /*
    #if 0
        status = CERESinsertAttrs( RadianceModeFlagDatasetID, "Radiance and Mode flags", "N/A", -10.0f, 510.0f );
        if ( status != EXIT_SUCCESS )
        {
            FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
            goto cleanupFail;
        }
    #endif
    */

    {

        int radiance_flags_fvalue = 2147483647;
        int radiance_flags_valid_range[2] = {0,2147483647};
        if(H5LTset_attribute_string(CERESdataFieldsID,"Radiance_and_Mode_flags","units","N/A")<0)
        {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags units attribute.\n");
            goto cleanupFail;
        }

        if(H5LTset_attribute_int(CERESdataFieldsID,"Radiance_and_Mode_flags","_FillValue",&radiance_flags_fvalue, 1 )<0)
        {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags _FillValue attribute.\n");
            goto cleanupFail;
        }
        if(H5LTset_attribute_int(CERESdataFieldsID,"Radiance_and_Mode_flags","valid_range",radiance_flags_valid_range, 2 )<0)
        {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags valid_range attribute.\n");
            goto cleanupFail;
        }
    }
    status = copyDimension( NULL, fileID, "Radiance and Mode flags", outputFile, RadianceModeFlagDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(RadianceModeFlagDatasetID);
    RadianceModeFlagDatasetID = 0;



    /***************************************
    * CERES SW Unfiltered Radiances Upwards *
    ***************************************/
    SWUnfilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES SW radiance - upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                           fileID );
    if ( SWUnfilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES SW Unfiltered Radiances Upwards dataset.\n");
        SWUnfilteredDatasetID = 0;
        goto cleanupFail;
    }

    // The valid_range is hard-coded. This is not good from software point-of-view. Maybe OK if this is from user's guide.
    status = CERESinsertAttrs( SWUnfilteredDatasetID, "CERES SW radiance - upwards", UNITS1, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW iltered Radiances Upwards.\n");
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( NULL, fileID, "CERES SW radiance - upwards", outputFile, SWUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(SWUnfilteredDatasetID);
    SWUnfilteredDatasetID = 0;


    /**********************************
     * LW Filtered Radiances Upwards *
     **********************************/
    LWUnfilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES LW radiance - upwards", DFNT_FLOAT32,
                                           H5T_NATIVE_FLOAT, fileID );
    if ( LWUnfilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES LW Radiances Upwards dataset.\n");
        LWUnfilteredDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( LWUnfilteredDatasetID, "CERES LW radiance - upwards", UNITS1, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( NULL, fileID, "CERES LW radiance - upwards", outputFile, LWUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(LWUnfilteredDatasetID);
    LWUnfilteredDatasetID = 0;


    /*********************************
     * WN Filtered Radiances Upwards *
     *********************************/
    WNUnfilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES WN radiance - upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                                           fileID );
    if ( WNUnfilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES WN  Radiances Upwards dataset.\n");
        WNUnfilteredDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( WNUnfilteredDatasetID, "CERES WN radiance - upwards", UNITS1, -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Radiances Upwards attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( NULL, fileID, "CERES WN radiance - upwards", outputFile, WNUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(WNUnfilteredDatasetID);
    WNUnfilteredDatasetID = 0;


    /**************
     * colatitude *
     **************/
    colatitudeDatasetID = readThenWrite( CERESgeolocationID, "Colatitude of CERES FOV at TOA", DFNT_FLOAT32,
                                         H5T_NATIVE_FLOAT, fileID );
    if ( colatitudeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES Colatitude of CERES FOV at TOA dataset.\n");
        colatitudeDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( colatitudeDatasetID, "CERES SW Filtered Radiance, Upwards", "deg", 0.0f, 180.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES Colatitude of CERES FOV at TOA attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( NULL, fileID, "Colatitude of CERES FOV at TOA", outputFile, colatitudeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(colatitudeDatasetID);
    colatitudeDatasetID = 0;

    /*************
     * longitude *
     *************/
    longitudeDatasetID = readThenWrite( CERESgeolocationID, "Longitude of CERES FOV at TOA", DFNT_FLOAT32,
                                        H5T_NATIVE_FLOAT, fileID );
    if ( longitudeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES Longitude of CERES FOV at TOA dataset.\n");
        longitudeDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( longitudeDatasetID, "CERES SW Filtered Radiance, Upwards", "deg", 0.0f, 180.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES Longitude of CERES FOV at TOA attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( NULL, fileID, "Longitude of CERES FOV at TOA", outputFile, longitudeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }


    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }
    if ( fileID ) SDend(fileID);
    if ( CERESrootID ) H5Gclose(CERESrootID);
    if ( CERESgranuleID ) H5Gclose(CERESgranuleID);
    if ( CERESdataFieldsID ) H5Gclose(CERESdataFieldsID);
    if ( CERESgeolocationID ) H5Gclose(CERESgeolocationID);
    if ( timeDatasetID ) H5Dclose(timeDatasetID);
    if ( SWFilteredDatasetID ) H5Dclose(SWFilteredDatasetID);
    if ( WNFilteredDatasetID ) H5Dclose(WNFilteredDatasetID);
    if ( TOTFilteredDatasetID ) H5Dclose(TOTFilteredDatasetID);
    if ( colatitudeDatasetID ) H5Dclose(colatitudeDatasetID);
    if ( longitudeDatasetID ) H5Dclose(longitudeDatasetID);

    if ( fail ) return EXIT_FAILURE;
#endif
    //return EXIT_SUCCESS;
}

int CERES_OrbitInfo(char*argv[],int* start_index_ptr,int* end_index_ptr,OInfo_t orbit_info)
{

    /* open the input file */
    int32 sd_id,sds_id, sds_index,status;
    int32 rank;
    int32 dimsizes[DIM_MAX];
    int32 start[DIM_MAX] = {0};
    int32 ntype;                    // number type for the data stored in the data set
    int32 num_attrs;                // number of attributes

    char* datasetName = "Time of observation";
    sd_id = SDstart( argv[2], DFACC_READ );
    if ( sd_id < 0 )
    {
        FATAL_MSG("Unable to open CERES file.\n");
        return (EXIT_FAILURE);
    }

    /* get the index of the dataset from the dataset's name */
    sds_index = SDnametoindex( sd_id, datasetName );
    if( sds_index < 0 )
    {
        printf("SDnametoindex\n");
        SDend(sd_id);
        return EXIT_FAILURE;
    }

    sds_id = SDselect( sd_id, sds_index );
    if ( sds_id < 0 )
    {
        printf("SDselect\n");
        SDend(sd_id);
        return EXIT_FAILURE;
    }


    for ( int i = 0; i < DIM_MAX; i++ )
    {
        dimsizes[i] = 1;
    }

    /* get info about dataset (rank, dim size, number type, num attributes ) */
    status = SDgetinfo( sds_id, NULL, &rank, dimsizes, &ntype, &num_attrs);
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] SDgetinfo: Failed to get info from dataset.\n", __FILE__, __func__, __LINE__);
        SDendaccess(sds_id);
        SDend(sd_id);
        return EXIT_FAILURE;
    }


    if(rank !=1 || ntype !=DFNT_FLOAT64)
    {
        fprintf( stderr, "[%s:%s:%d] the time dimension rank must be 1 and the datatype must be double.\n", __FILE__, __func__, __LINE__);
        SDendaccess(sds_id);
        SDend(sd_id);
        return EXIT_FAILURE;
    }

    double *julian_date = malloc(sizeof julian_date *dimsizes[0]);

    status = SDreaddata( sds_id, start, NULL, dimsizes, (VOIDP)julian_date);

    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] SDreaddata: Failed to read data.\n", __FILE__, __func__, __LINE__);
        SDendaccess(sds_id);
        SDend(sd_id);
        if ( julian_date != NULL ) free(julian_date);
        return EXIT_FAILURE;
    }

//printf("julian_date[0] is %lf \n",julian_date[0]);

    if(obtain_start_end_index(start_index_ptr,end_index_ptr,julian_date,dimsizes[0],orbit_info) == EXIT_FAILURE)
    {
        fprintf( stderr, "[%s:%s:%d] Obtain_start_end_index: Failed to obtain the start and end index.\n", __FILE__, __func__, __LINE__);
        SDendaccess(sds_id);
        SDend(sd_id);
        if ( julian_date != NULL ) free(julian_date);
        return EXIT_FAILURE;


    }
#if DEBUG
    printf("starting index is %d\n",*start_index_ptr);
    printf("ending index is %d\n",*end_index_ptr);
#endif



    SDendaccess(sds_id);
    SDend(sd_id);
    if ( julian_date != NULL ) free(julian_date);
    return 0;

}
herr_t CERESinsertAttrs( hid_t objectID, char* long_nameVal, char* unitsVal, float valid_rangeMin, float valid_rangeMax )
{

    hsize_t dimSize = 0;
    hid_t attrID = 0;
    hid_t dataspaceID = 0;
    float floatBuff2[2] = {0.0};
    herr_t status = 0;
    /********************* long_name*****************************/
    attrID = attrCreateString( objectID, "long_name", long_nameVal );
    if ( attrID == EXIT_FAILURE )
    {
        FATAL_MSG("Cannot create CERES \"long_name\" attribute.\n");
        return EXIT_FAILURE;
    }
    H5Aclose(attrID);
    /********************* units ********************************/
    attrID = attrCreateString( objectID, "units", unitsVal );
    if ( attrID == EXIT_FAILURE )
    {
        FATAL_MSG("Cannot create CERES \"units\" attribute.\n");
        return EXIT_FAILURE;
    }

    H5Aclose(attrID);
    /********************* format *******************************/
    attrID = attrCreateString( objectID, "format", "32-BitFloat" );
    if ( attrID == EXIT_FAILURE )
    {
        FATAL_MSG("Cannot create CERES \"format\" attribute.\n");
        return EXIT_FAILURE;
    }

    H5Aclose(attrID);
    /********************* coordsys *****************************/
    attrID = attrCreateString( objectID, "coordsys", "not used" );
    if ( attrID == EXIT_FAILURE )
    {
        FATAL_MSG("Cannot create CERES \"coordsys\" attribute.\n");
        return EXIT_FAILURE;
    }

    H5Aclose(attrID);
    /********************* valid_range **************************/
    dimSize = 2;
    dataspaceID = H5Screate_simple( 1, &dimSize, NULL );
    if ( dataspaceID < 0 )
    {
        FATAL_MSG("Failed to create simple dataspace.\n");
        return EXIT_FAILURE;
    }
    attrID = H5Acreate(objectID, "valid_range", H5T_NATIVE_FLOAT, dataspaceID, H5P_DEFAULT, H5P_DEFAULT );
    if ( attrID < 0 )
    {
        FATAL_MSG("Failed to create CERES \"valid_range\" attribute\n");
        H5Sclose(dataspaceID);
        return EXIT_FAILURE;
    }

    floatBuff2[0] = valid_rangeMin;
    floatBuff2[1] = valid_rangeMax;
    status = H5Awrite( attrID, H5T_NATIVE_FLOAT, floatBuff2 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to write to CERES attribute.\n");
        H5Sclose(dataspaceID);
        H5Aclose(attrID);
        return EXIT_FAILURE;
    }

    H5Aclose( attrID );
    H5Sclose(dataspaceID);
    /********************* _FillValue****************************/
    dimSize = 1;
    dataspaceID = H5Screate_simple( 1, &dimSize, NULL );
    if ( dataspaceID < 0 )
    {
        FATAL_MSG("Failed to create simple dataspace.\n");
        return EXIT_FAILURE;
    }
    attrID = H5Acreate( objectID, "_FillValue", H5T_NATIVE_FLOAT,
                        dataspaceID, H5P_DEFAULT, H5P_DEFAULT );
    if ( attrID < 0 )
    {
        FATAL_MSG("Failed to create CERES \"valid_range\" attribute\n");
        H5Sclose(dataspaceID);
        return EXIT_FAILURE;
    }

    floatBuff2[0] = 3.4028235e38;
    status = H5Awrite( attrID, H5T_NATIVE_FLOAT, floatBuff2 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to write to CERES attribute.\n");
        H5Sclose(dataspaceID);
        H5Aclose(attrID);
        return EXIT_FAILURE;
    }
    H5Aclose( attrID );
    H5Sclose(dataspaceID);

    return EXIT_SUCCESS;

}

int obtain_start_end_index(int* sindex_ptr,int* eindex_ptr,double *jd,int32 size,OInfo_t orbit_info)
{

    GDateInfo_t orbit_start_time,orbit_end_time;
    GDateInfo_t granule_start_time,granule_end_time;
    int temp_year = 0;
    int temp_month = 0;
    int temp_day = 0;
    int temp_hour = 0;
    int temp_minute = 0;
    double temp_second = 0.0;
    int* temp_yearp =&temp_year;
    int* temp_monthp=&temp_month;
    int* temp_dayp=&temp_day;
    int* temp_hourp=&temp_hour;
    int* temp_minutep=&temp_minute;
    double* temp_secondp=&temp_second;


    orbit_start_time.year = orbit_info.start_year;
    orbit_start_time.month = orbit_info.start_month;
    orbit_start_time.day = orbit_info.start_day;
    orbit_start_time.hour = orbit_info.start_hour;
    orbit_start_time.minute = orbit_info.start_minute;
    orbit_start_time.second = orbit_info.start_second;

#if DEBUG
    printf("S orbit year is %d\n",orbit_start_time.year);
    printf("S orbit month is %d\n",orbit_start_time.month);
    printf("S orbit day is %d\n",orbit_start_time.day);
    printf("S orbit hour is %d\n",orbit_start_time.hour);
    printf("S orbit minute is %d\n",orbit_start_time.minute);
    printf("S orbit second is %lf\n",orbit_start_time.second);
#endif

    orbit_end_time.year = orbit_info.end_year;
    orbit_end_time.month = orbit_info.end_month;
    orbit_end_time.day = orbit_info.end_day;
    orbit_end_time.hour = orbit_info.end_hour;
    orbit_end_time.minute = orbit_info.end_minute;
    orbit_end_time.second = orbit_info.end_second;

#if DEBUG
    printf("E orbit year is %d\n",orbit_end_time.year);
    printf("E orbit month is %d\n",orbit_end_time.month);
    printf("E orbit day is %d\n",orbit_end_time.day);
    printf("E orbit hour is %d\n",orbit_end_time.hour);
    printf("E orbit minute is %d\n",orbit_end_time.minute);
    printf("E orbit second is %lf\n",orbit_end_time.second);
#endif



    get_greg(jd[0],temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);

    //sanity check the return value
    if((*temp_yearp<=0) || ((*temp_monthp)>12) || ((*temp_monthp) <=0) ||((*temp_dayp)>31)
            ||((*temp_dayp)<=0) ||((*temp_hourp)<0) ||((*temp_hourp)>59) ||((*temp_minutep)<0)
            ||((*temp_minutep)>59)||((*temp_secondp)<0) ||((*temp_secondp)>59))
    {
        FATAL_MSG("The UTC time retrieved from CERES array is out of range\n");
        return EXIT_FAILURE;
    }
    granule_start_time.year = *temp_yearp;
    granule_start_time.month = *temp_monthp;
    granule_start_time.day = *temp_dayp;
    granule_start_time.hour = *temp_hourp;
    granule_start_time.minute = *temp_minutep;
    granule_start_time.second = *temp_secondp;


    get_greg(jd[size-1],temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);

    granule_end_time.year = *temp_yearp;
    granule_end_time.month = *temp_monthp;
    granule_end_time.day = *temp_dayp;
    granule_end_time.hour = *temp_hourp;
    granule_end_time.minute = *temp_minutep;
    granule_end_time.second = *temp_secondp;

    // The granule is not falling into the oribit
    if ((comp_greg(granule_end_time,orbit_start_time)<0) || (comp_greg(orbit_end_time,granule_start_time)<0))
    {
        *sindex_ptr = -1;
        *eindex_ptr = -1;
    }
    // The whole granule falls into the orbit
    else if((comp_greg(orbit_start_time,granule_start_time)<0) &&(comp_greg(granule_end_time,orbit_end_time)<0))
    {
//printf("coming to full granule \n");
        *sindex_ptr = 0;
        *eindex_ptr = size-1;
    }
    // Partial - Use efficient algorithm
    else
    {

        // Here, we know that CERES is an hourly date,so use 1 hour
        // now use the binarySearchDouble, later, will use the combination of utc_diff_time and binaerySearchDouble
        // The starting index will always be 0
        if(comp_greg(orbit_start_time,granule_start_time)<=0)
        {
//printf("coming to partial start index is 0\n");
            *sindex_ptr = 0;
            // Find end_index, must use granule_end_time

            //*eindex_ptr = get_subset_index(1,0,size-1,orbit_end_time,jd);
            int temp_index =  binarySearchUTC(jd,orbit_end_time, 0,size-1);
            if(temp_index == -1)
            {
                FATAL_MSG("The CERES subset index is out of range\n");
                return EXIT_FAILURE;
            }

            *eindex_ptr = temp_index;
            // corner case, if the end_index time is exactly the same as orbit_end_time, don't count this point.
            if(comp_greg_utc(jd[*eindex_ptr],orbit_end_time) == 0)
                *eindex_ptr = *eindex_ptr -1;

        }
        else
        {
//printf("coming to partial end index is %d\n",size-1);
            *eindex_ptr = size-1;
            // Find the start index, we need to add 1 because the convention is that the starting time falls in
            // the orbit. The return index is adjcent to the orbit start time but on the smaller side.
            int temp_index = binarySearchUTC(jd,orbit_start_time,0,size-1);
            if(temp_index == -1)
            {
                FATAL_MSG("The CERES subset index is out of range\n");
                return EXIT_FAILURE;
            }

            *sindex_ptr = 1+temp_index;
        }
    }

    return EXIT_SUCCESS;
}

#if 0
// The start_fix will determine which point includes in the subset. Only the larger value one includes in the subset.
int get_subset_index(int start_fix,int start_index,int end_index,GDateInfo_t orbit_time,double* jd)
{

    printf("orbit year is %d\n",orbit_time.year);
    printf("orbit month is %d\n",orbit_time.month);
    printf("orbit day is %d\n",orbit_time.day);
    printf("orbit hour is %d\n",orbit_time.hour);
    printf("orbit minute is %d\n",orbit_time.minute);
    printf("orbit second is %lf\n",orbit_time.second);
    GDateInfo_t gran_mark_index_time,gran_mark_adj_time;
    int temp_year = 0;
    int temp_month = 0;
    int temp_day = 0;
    int temp_hour = 0;
    int temp_minute = 0;
    double temp_second = 0.;
    int* temp_yearp =&temp_year;
    int* temp_monthp=&temp_month;
    int* temp_dayp=&temp_day;
    int* temp_hourp=&temp_hour;
    int* temp_minutep=&temp_minute;
    double* temp_secondp=&temp_second;

    // Find end_index
    int mark_index =end_index/2;
    int mark_adj_index = mark_index+1;

    int find_index = 0;
    short before_orbit = 0;
    int pre_mark_index = 0;

    int loop_end_index = end_index;
    int loop_start_index = 0;
    //int pre_mark_index2 = 0;

    while (find_index == 0)
    {

        get_greg(jd[mark_index],temp_yearp,temp_monthp,temp_dayp,temp_hourp,temp_minutep,temp_secondp);
        gran_mark_index_time.year = *temp_yearp;
        gran_mark_index_time.month = *temp_monthp;
        gran_mark_index_time.day = *temp_dayp;
        gran_mark_index_time.hour = *temp_hourp;
        gran_mark_index_time.minute = *temp_minutep;
        gran_mark_index_time.second = *temp_secondp;

        int cmp_orbit_mark_time = comp_greg(orbit_time,gran_mark_index_time);
        printf("mark_index is %d\n",mark_index);
        printf("cmp_orbit_mark_time is %d\n",cmp_orbit_mark_time);

        int temp_mark_index = mark_index;
        //if(cmp_orbit_mark_time >=0) {
        if(cmp_orbit_mark_time >0)
        {
            if(before_orbit == -1)
            {
                mark_index = (mark_index+pre_mark_index)/2;
            }
            else
                mark_index = (mark_index+loop_end_index)/2;
            loop_start_index = mark_index;
            before_orbit = 1;
        }
        else
        {
            if(before_orbit == 1)
                mark_index = (mark_index+pre_mark_index)/2;
            else
                mark_index =(mark_index+loop_start_index)/2;
            loop_end_index = mark_index;
            before_orbit = -1;
        }
        if(((pre_mark_index - mark_index) == 1) || ((mark_index - pre_mark_index) == 1))
            find_index = 1;
        if(mark_index == 0)
            find_index = -1;

        pre_mark_index = temp_mark_index;

    }
    // Get the index
    return mark_index;

}
#endif
