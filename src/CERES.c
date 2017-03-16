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

int CERES( char* argv[],int index )
{
    /*************
     * VARIABLES *
     *************/
    int32 fileID = 0;
    hid_t timeDatasetID = 0;
    hid_t SWFilteredDatasetID = 0;
    hid_t WNFilteredDatasetID = 0;
    hid_t TOTFilteredDatasetID = 0;
    hid_t colatitudeDatasetID = 0;
    hid_t longitudeDatasetID = 0;
    hid_t CERESrootID = 0;
    hid_t CERESgranuleID = 0;
    hid_t CERESdataFieldsID = 0;
    hid_t CERESgeolocationID = 0;
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

    /************************
     * Julian Date and Time *
     ************************/
    timeDatasetID = readThenWrite( CERESgeolocationID, "Julian Date and Time", DFNT_FLOAT64, H5T_NATIVE_DOUBLE,
                            fileID );

    if ( timeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES time dataset.\n");
        timeDatasetID = 0;
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( fileID, "Julian Date and Time", outputFile, timeDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }
    H5Dclose(timeDatasetID); timeDatasetID = 0;


    /***************************************
     * CERES SW Filtered Radiances Upwards *
     ***************************************/
    SWFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES SW Filtered Radiances Upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                            fileID );
    if ( SWFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES SW Filtered Radiances Upwards dataset.\n");
        SWFilteredDatasetID = 0;
        goto cleanupFail;
    }
 
    status = CERESinsertAttrs( SWFilteredDatasetID, "CERES SW Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW Filtered Radiances Upwards.\n");
        goto cleanupFail;
    }
    // copy the dimension scales
    status = copyDimension( fileID, "CERES SW Filtered Radiances Upwards", outputFile, SWFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(SWFilteredDatasetID); SWFilteredDatasetID = 0;


    /*********************************
     * WN Filtered Radiances Upwards *
     *********************************/
    WNFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES WN Filtered Radiances Upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                            fileID );
    if ( WNFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards dataset.\n");
        WNFilteredDatasetID = 0;
        goto cleanupFail;
    }

    status = CERESinsertAttrs( WNFilteredDatasetID, "CERES WN Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }

    status = copyDimension( fileID, "CERES WN Filtered Radiances Upwards", outputFile, WNFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(WNFilteredDatasetID); WNFilteredDatasetID = 0;


    /**********************************
     * TOT Filtered Radiances Upwards *
     **********************************/
    TOTFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES TOT Filtered Radiances Upwards", DFNT_FLOAT32, 
                                          H5T_NATIVE_FLOAT, fileID );
    if ( TOTFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards dataset.\n");
        TOTFilteredDatasetID = 0;
        goto cleanupFail;
    }
    
    status = CERESinsertAttrs( TOTFilteredDatasetID, "CERES TOT Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        goto cleanupFail;
    }
    status = copyDimension( fileID, "CERES TOT Filtered Radiances Upwards", outputFile, TOTFilteredDatasetID );
    if ( status == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    H5Dclose(TOTFilteredDatasetID); TOTFilteredDatasetID = 0;

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
