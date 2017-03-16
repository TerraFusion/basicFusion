#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>  
#include <hdf.h>    // hdf4
#include <hdf5.h>   // hdf5
#include "libTERRA.h"
#define XMAX 2
#define YMAX 720
#define DIM_MAX 10

herr_t CERESinsertAttrs( hid_t objectID, char* long_nameVal, char* unitsVal, float valid_rangeMin, float valid_rangeMax );
//int CERES_OrbitInfo(char*argv[],int* start_index_ptr,int* end_index_ptr,OInfo_t orbit_info);
int CERES_OrbitInfo(char*argv[],int* start_index_ptr,int* end_index_ptr,OInfo_t orbit_info){

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


    if(rank !=1 || ntype !=DFNT_FLOAT64) {
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

 printf("julian_date[0] is %lf \n",julian_date[0]);
    
    obtain_start_end_index(start_index_ptr,end_index_ptr,julian_date,dimsizes[0]);
 

    SDendaccess(sds_id);
    SDend(sd_id);
    if ( julian_date != NULL ) free(julian_date);
    return 0;   

}
int CERES( char* argv[],int index )
{
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
    if ( index == 1 ){
        if ( createGroup( &outputFile, &CERESrootID, "CERES" ) )
        {
            FATAL_MSG("Failed to create CERES root group.\n");
            CERESrootID = 0;
            goto cleanupFail;
        }


        if(H5LTset_attribute_string(outputFile,"CERES","FilePath",argv[1])<0) {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            goto cleanupFail;
        }

        fileTime = getTime( argv[1], 1 );

        if(H5LTset_attribute_string(outputFile,"CERES","GranuleTime",fileTime)<0) {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            goto cleanupFail;
        }
        free(fileTime); fileTime = NULL;

        if ( createGroup( &CERESrootID, &CERESgranuleID, "FM1" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            CERESgranuleID = 0;
            goto cleanupFail;
        }
    }   
    else if(index == 2) {
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

    else {
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
                            fileID );

    if ( timeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES time dataset.\n");
        timeDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( fileID, "Time_Of_Observation", outputFile, timeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(timeDatasetID); timeDatasetID = 0;

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
    status = copyDimension( fileID, "CERES viewing zenith at surface", outputFile, viewZenithDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(viewZenithDatasetID); viewZenithDatasetID = 0;

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
    status = copyDimension( fileID, "CERES solar zenith at surface", outputFile, solarZenithDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(solarZenithDatasetID); solarZenithDatasetID = 0;

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
    status = copyDimension( fileID, "CERES relative azimuth at surface", outputFile, relativeAzimuthDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(relativeAzimuthDatasetID); relativeAzimuthDatasetID = 0;

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
    status = copyDimension( fileID, "CERES viewing azimuth at surface wrt North", outputFile, viewAzimuthDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(viewAzimuthDatasetID); viewAzimuthDatasetID = 0;



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
    status = CERESinsertAttrs( SWFilteredDatasetID, "CERES SW filtered radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW Filtered Radiances Upwards.\n");
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( fileID, "CERES SW filtered radiance - upwards", outputFile, SWFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(SWFilteredDatasetID); SWFilteredDatasetID = 0;


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

    status = CERESinsertAttrs( WNFilteredDatasetID, "CERES WN filtered radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( fileID, "CERES WN filtered radiance - upwards", outputFile, WNFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(WNFilteredDatasetID); WNFilteredDatasetID = 0;


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
    
    status = CERESinsertAttrs( TOTFilteredDatasetID, "CERES TOT filtered radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( fileID, "CERES TOT filtered radiance - upwards", outputFile, TOTFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(TOTFilteredDatasetID); TOTFilteredDatasetID = 0;

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
        if(H5LTset_attribute_string(CERESdataFieldsID,"Radiance_and_Mode_flags","units","N/A")<0) {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags units attribute.\n");
            goto cleanupFail;
        }

        if(H5LTset_attribute_int(CERESdataFieldsID,"Radiance_and_Mode_flags","_FillValue",&radiance_flags_fvalue, 1 )<0) {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags _FillValue attribute.\n");
            goto cleanupFail;
        }
        if(H5LTset_attribute_int(CERESdataFieldsID,"Radiance_and_Mode_flags","valid_range",radiance_flags_valid_range, 2 )<0) {
            FATAL_MSG("Failed to insert the Radiance_and_Mode_flags valid_range attribute.\n");
            goto cleanupFail;
        }
    }
    status = copyDimension( fileID, "Radiance and Mode flags", outputFile, RadianceModeFlagDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(RadianceModeFlagDatasetID); RadianceModeFlagDatasetID = 0;


    
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
    status = CERESinsertAttrs( SWUnfilteredDatasetID, "CERES SW radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW iltered Radiances Upwards.\n");
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( fileID, "CERES SW radiance - upwards", outputFile, SWUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(SWUnfilteredDatasetID); SWUnfilteredDatasetID = 0;


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
    
    status = CERESinsertAttrs( LWUnfilteredDatasetID, "CERES LW radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( fileID, "CERES LW radiance - upwards", outputFile, LWUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(LWUnfilteredDatasetID); LWUnfilteredDatasetID = 0;


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

    status = CERESinsertAttrs( WNUnfilteredDatasetID, "CERES WN radiance - upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Radiances Upwards attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( fileID, "CERES WN radiance - upwards", outputFile, WNUnfilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(WNUnfilteredDatasetID); WNUnfilteredDatasetID = 0;


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
    status = copyDimension( fileID, "Colatitude of CERES FOV at TOA", outputFile, colatitudeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(colatitudeDatasetID); colatitudeDatasetID = 0;

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

    status = copyDimension( fileID, "Longitude of CERES FOV at TOA", outputFile, longitudeDatasetID );
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
    return EXIT_SUCCESS;    
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

void obtain_start_end_index(int* sindex_ptr,int* endex_ptr,double *jd,int32 size){

}
