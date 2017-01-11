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
            SDend(fileID);
            return EXIT_FAILURE;
        }


        if(H5LTset_attribute_string(outputFile,"CERES","GranuleTime",argv[1])<0) {
            FATAL_MSG("Failed to add CERES time stamp.\n");
            SDend(fileID);
            H5Gclose(CERESrootID);
            return EXIT_FAILURE;
        }

        if ( createGroup( &CERESrootID, &CERESgranuleID, "FM1" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            SDend(fileID);
            H5Gclose(CERESrootID);
            return EXIT_FAILURE;
        }
    }   
    else if(index == 2) {
        CERESrootID = H5Gopen2( outputFile, "CERES", H5P_DEFAULT );
        if ( CERESrootID < 0 )
        {
            FATAL_MSG("Unable to open CERES root group.\n");
            SDend(fileID);
            return EXIT_FAILURE;
        }
        if ( createGroup( &CERESrootID, &CERESgranuleID, "FM2" ) )
        {
            FATAL_MSG("CERES create granule group failure.\n");
            SDend(fileID);
            H5Gclose(CERESrootID);
            return EXIT_FAILURE;
        }
    }   

    else {
        FATAL_MSG("The CERES granule index should be either 1 or 2.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        return EXIT_FAILURE;
    }   


    // create the data fields group
    if ( createGroup ( &CERESgranuleID, &CERESdataFieldsID, "Data Fields" ) )
    {
        FATAL_MSG("CERES create data fields group failure.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        return EXIT_FAILURE;
    }
    
    // create geolocation fields
    if ( createGroup( &CERESgranuleID, &CERESgeolocationID, "Geolocation" ) )
    {
        FATAL_MSG("Failed to create CERES geolocation group.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        return EXIT_FAILURE;
    }

    /************************
     * Julian Date and Time *
     ************************/
    timeDatasetID = readThenWrite( CERESgeolocationID, "Julian Date and Time", DFNT_FLOAT64, H5T_NATIVE_DOUBLE,
                            fileID );

    if ( timeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES time dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }   
    H5Dclose(timeDatasetID);


    /***************************************
     * CERES SW Filtered Radiances Upwards *
     ***************************************/
    SWFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES SW Filtered Radiances Upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                            fileID );
    if ( SWFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES SW Filtered Radiances Upwards dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }
 
    status = CERESinsertAttrs( SWFilteredDatasetID, "CERES SW Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert attributes for SW Filtered Radiances Upwards.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        H5Dclose(SWFilteredDatasetID);
        return EXIT_FAILURE;
    }
    H5Dclose(SWFilteredDatasetID);


    /*********************************
     * WN Filtered Radiances Upwards *
     *********************************/
    WNFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES WN Filtered Radiances Upwards", DFNT_FLOAT32, H5T_NATIVE_FLOAT,
                            fileID );
    if ( WNFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }

    status = CERESinsertAttrs( WNFilteredDatasetID, "CERES WN Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES WN Filtered Radiances Upwards attributes.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        H5Dclose(WNFilteredDatasetID);
        return EXIT_FAILURE;
    }
    H5Dclose(WNFilteredDatasetID);


    /**********************************
     * TOT Filtered Radiances Upwards *
     **********************************/
    TOTFilteredDatasetID = readThenWrite( CERESdataFieldsID, "CERES TOT Filtered Radiances Upwards", DFNT_FLOAT32, 
                                          H5T_NATIVE_FLOAT, fileID );
    if ( TOTFilteredDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }
    
    status = CERESinsertAttrs( WNFilteredDatasetID, "CERES TOT Filtered Radiance, Upwards", "Watts per square meter per steradian", -10.0f, 510.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES TOT Filtered Radiances Upwards attributes.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        H5Dclose(TOTFilteredDatasetID);
        return EXIT_FAILURE;
    }

    H5Dclose(TOTFilteredDatasetID);

    /**************
     * colatitude *
     **************/
    colatitudeDatasetID = readThenWrite( CERESgeolocationID, "Colatitude of CERES FOV at TOA", DFNT_FLOAT32, 
                                          H5T_NATIVE_FLOAT, fileID );
    if ( colatitudeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES Colatitude of CERES FOV at TOA dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }
    
    status = CERESinsertAttrs( colatitudeDatasetID, "CERES SW Filtered Radiance, Upwards", "deg", 0.0f, 180.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES Colatitude of CERES FOV at TOA attributes.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        H5Dclose(colatitudeDatasetID);
        return EXIT_FAILURE;
    }
    H5Dclose(colatitudeDatasetID);

    /*************
     * longitude *
     *************/
    longitudeDatasetID = readThenWrite( CERESgeolocationID, "Longitude of CERES FOV at TOA", DFNT_FLOAT32,
                                          H5T_NATIVE_FLOAT, fileID );
    if ( longitudeDatasetID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to insert CERES Longitude of CERES FOV at TOA dataset.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        return EXIT_FAILURE;
    }
    
    status = CERESinsertAttrs( longitudeDatasetID, "CERES SW Filtered Radiance, Upwards", "deg", 0.0f, 180.0f );
    if ( status != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to insert CERES Longitude of CERES FOV at TOA attributes.\n");
        SDend(fileID);
        H5Gclose(CERESrootID);
        H5Gclose(CERESgranuleID);
        H5Gclose(CERESdataFieldsID);
        H5Gclose(CERESgeolocationID);
        H5Dclose(longitudeDatasetID);
        return EXIT_FAILURE;
    }
    H5Dclose(longitudeDatasetID);

    // Close the groups, we don't need them anymore
    H5Gclose(CERESrootID);
    H5Gclose(CERESgranuleID);
    H5Gclose(CERESdataFieldsID);
    H5Gclose(CERESgeolocationID);

    SDend(fileID);
    
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
