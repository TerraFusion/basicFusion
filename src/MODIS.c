#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <mfhdf.h>  // hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>   // hdf5
#include "libTERRA.h"
#ifndef DIM_MAX
#define DIM_MAX 10
#endif
#ifndef M_PI
#    define M_PI 3.14159265358979323846
#endif


/*
    [0] = main program name
    [1] = 1km filename
    [2] = 500m filename
    [3] = 250m filename
    [4] = MOD03 filename
    [5] = granule name ("granule1","granule2", etc)
    [6] = output filename (already exists);
*/

/* MY 2016-12-20, handling the MODIS files with and without MOD02HKM and MOD02QKM. */

void upscaleLatLonSpherical(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon);
int readThenWrite_MODIS_HR_LatLon(hid_t MODIS500mgeoGroupID,hid_t MODIS250mgeoGroupID,char* latname,char* lonname,int32 h4_type,hid_t h5_type,int32 MOD03FileID);
int MODIS( char* argv[] ,int modis_count, int unpack)
{
    /*************
     * VARIABLES *
     *************/
    
    hid_t MODISrootGroupID = 0;
    hid_t MODISgranuleGroupID = 0;
    hid_t MODIS1KMdataFieldsGroupID = 0;
    hid_t MODIS500mdataFieldsGroupID = 0;
    hid_t MODIS250mdataFieldsGroupID = 0;
    hid_t MODIS1KMgeolocationGroupID = 0;
    hid_t MODIS500mgeolocationGroupID = 0;
    hid_t MODIS250mgeolocationGroupID = 0;
    herr_t status = EXIT_SUCCESS;
    float fltTemp = 0.0;
    intn statusn = 0;
    int fail = 0;
    char* correctedName = NULL;
    char* fileTime = NULL;
    int32 dsetID = 0;
    herr_t errStatus = 0;
    char* dimName = NULL;
    void* dimBuffer = NULL;
    //char netCDF_pureDim_NAME_val[] = "This is a netCDF dimension but not a netCDF variable.";

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
     
// JUST FOR DEBUGGING
//unpack = 0;    
    
    /* open the input files */
    _1KMFileID = SDstart( argv[1], DFACC_READ );
    if ( _1KMFileID < 0 )
    {
        FATAL_MSG( "Unable to open 1KM file.\n" );
        _1KMFileID = 0;
        return (EXIT_FAILURE);
    }
    
    if (argv[2]!= NULL) {
        _500mFileID = SDstart( argv[2], DFACC_READ );
        if ( _500mFileID < 0 )
        {
            fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
                 __LINE__ );
            _500mFileID = 0;
            goto cleanupFail;
        }
    }
    
    if (argv[3]!= NULL) {
        _250mFileID = SDstart( argv[3], DFACC_READ );
        if ( _250mFileID < 0 )
        {
            fprintf( stderr, "[%s:%s:%d]: Unable to open 250m file.\n", __FILE__, __func__,
                 __LINE__ );
            _250mFileID = 0;
            goto cleanupFail;
        }   
    }
    
    MOD03FileID = SDstart( argv[4], DFACC_READ );
    if ( MOD03FileID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d]: Unable to open MOD03 file.\n", __FILE__, __func__,
                 __LINE__ );
        MOD03FileID = 0;
        goto cleanupFail;
    }
    
    
    /********************************************************************************
     *                                GROUP CREATION                                *
     ********************************************************************************/
     
    /* outputfile already exists (created by main). Create the group directories */
    //create root MODIS group
    
    if (modis_count == 1) {
        if ( createGroup( &outputFile, &MODISrootGroupID, "MODIS" ) == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS root group.\n",__FILE__,__func__,__LINE__);
            MODISrootGroupID = 0;
            goto cleanupFail;
        }
    }
    else {
        MODISrootGroupID = H5Gopen2(outputFile, "/MODIS",H5P_DEFAULT);

        if (MODISrootGroupID <0) {
            fprintf( stderr, "[%s:%s:%d] Failed to open MODIS root group.\n",__FILE__,__func__,__LINE__);
            MODISrootGroupID = 0;
            goto cleanupFail;
        }
    }


        // Create the granule group under MODIS group
    if ( createGroup( &MODISrootGroupID, &MODISgranuleGroupID,argv[5] ) )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create MODIS root group.\n",__FILE__,__func__,__LINE__);
        MODISgranuleGroupID = 0;
        goto cleanupFail;
    }

        
    if (H5LTset_attribute_string(MODISrootGroupID,argv[5],"FilePath",argv[1])<0) 
    {
        fprintf(stderr, "[%s:%s:%d] Cannot add the file path attribute.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }

    // Extract the time substring from the file path
    fileTime = getTime( argv[1], 2 );
    if (H5LTset_attribute_string(MODISrootGroupID,argv[5],"GranuleTime",fileTime)<0)
    {
        fprintf(stderr, "[%s:%s:%d] Cannot add the time stamp.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }
    free(fileTime); fileTime = NULL;

    /* create the 1 kilometer product group */
    if ( createGroup ( &MODISgranuleGroupID, &MODIS1KMGroupID, "1KM" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KM group.\n",__FILE__,__func__,__LINE__);
        MODIS1KMGroupID = 0;
        goto cleanupFail;
    }
    
    // create the data fields group
    if ( createGroup ( &MODIS1KMGroupID, &MODIS1KMdataFieldsGroupID, "Data Fields" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KM data fields group.\n",__FILE__,__func__,__LINE__);
        MODIS1KMdataFieldsGroupID = 0;
        goto cleanupFail;
    }
    
    // create 1KMgeolocation fields group
    if ( createGroup( &MODIS1KMGroupID, &MODIS1KMgeolocationGroupID, "Geolocation" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KMgeolocation group.\n",__FILE__,__func__,__LINE__);
        MODIS1KMgeolocationGroupID = 0;
        goto cleanupFail;
    }

    if (argv[2] !=NULL) {
        /* create the 500m product group */
        if ( createGroup ( &MODISgranuleGroupID, &MODIS500mGroupID, "500m" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 500m group.\n",__FILE__, __func__,__LINE__);
            MODIS500mGroupID = 0;
            goto cleanupFail;
        }

        if ( createGroup ( &MODIS500mGroupID, &MODIS500mdataFieldsGroupID, "Data Fields" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 500m data fields group.\n",__FILE__, __func__,__LINE__);
            MODIS500mdataFieldsGroupID = 0;
            goto cleanupFail;
        }

    
    }

    if (argv[3] !=NULL) {
        /* create the 250m product group */
        if ( createGroup ( &MODISgranuleGroupID, &MODIS250mGroupID, "250m" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 250m group.\n",__FILE__, __func__,__LINE__);
            MODIS250mGroupID = 0;
            goto cleanupFail;
        }

        if ( createGroup ( &MODIS250mGroupID, &MODIS250mdataFieldsGroupID, "Data Fields" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 250m data fields group.\n",__FILE__,__func__,__LINE__);
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
                  
                  
                  
    /*_______________EV_1KM_RefSB data_______________*/

    // IF WE ARE UNPACKING DATA
    if (unpack == 1) {
        if (argv[2]!=NULL) {

            /* Forgive me oh mighty programming gods for using the insidious goto.
             * I believe if you look carefully, goto makes a lot of sense in this context.
             */

            _1KMDatasetID = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
                 _1KMFileID);
            if ( _1KMDatasetID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer EV_1KM_RefSB data.\n");
                _1KMDatasetID = 0; // Done to prevent program from trying to close this ID erroneously
                goto cleanupFail;
            }
    
            _1KMUncertID = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                DFNT_UINT8, _1KMFileID );
            if ( _1KMUncertID == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _1KMUncertID = 0;
                goto cleanupFail;
            }
            

        }
    }
        
    // ELSE WE ARE NOT UNPACKING DATA
    else {
        _1KMDatasetID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
                H5T_NATIVE_USHORT, _1KMFileID);
        if ( _1KMDatasetID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB data.\n",__FILE__,__func__,__LINE__);
            _1KMDatasetID = 0;
            goto cleanupFail;
        }
    /*______________EV_1KM_RefSB_Uncert_Indexes______________*/


        _1KMUncertID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
        if ( _1KMUncertID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            _1KMUncertID = 0;
            goto cleanupFail;
        }

    }

    if ( unpack == 0 || argv[2] != NULL )
    {
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


        // Copy the dimensions over
        errStatus = copyDimension( _1KMFileID, "EV_1KM_RefSB", outputFile, _1KMDatasetID );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _1KMFileID, "EV_1KM_RefSB_Uncert_Indexes", outputFile, _1KMUncertID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    }



    // Close the identifiers related to these datasets
    if ( _1KMDatasetID ) status = H5Dclose(_1KMDatasetID); 
    _1KMDatasetID = 0;
    if ( status < 0 ) { WARN_MSG("H5Dclose\n");}

    if ( _1KMUncertID) status = H5Dclose(_1KMUncertID); 
    _1KMUncertID = 0;
    if ( status < 0 ) {WARN_MSG("H5Dclose\n");}

                
/*___________EV_1KM_Emissive___________*/

    if (1==unpack) {

        _1KMEmissive = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                DFNT_UINT16, _1KMFileID);
        if ( _1KMEmissive == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive data.\n",__FILE__,__func__,__LINE__);
            _1KMEmissive = 0;
            goto cleanupFail;
        }
        

        
        _1KMEmissiveUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
                      "EV_1KM_Emissive_Uncert_Indexes",
                      DFNT_UINT8, _1KMFileID);
        if ( _1KMEmissiveUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            _1KMEmissiveUncert = 0;
            goto cleanupFail;
        }
        

    }
    else {
        _1KMEmissive = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
        if ( _1KMEmissive == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive data.\n",__FILE__,__func__,__LINE__);
            _1KMEmissive = 0;
            goto cleanupFail;
        }

        _1KMEmissiveUncert = readThenWrite( MODIS1KMdataFieldsGroupID,
                      "EV_1KM_Emissive_Uncert_Indexes",
                      DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
        if ( _1KMEmissiveUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            _1KMEmissiveUncert = 0;
            goto cleanupFail;
        }
    }

     
    // ATTRIBUTES
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

    // Copy the dimensions over
    errStatus = copyDimension( _1KMFileID, "EV_1KM_Emissive", outputFile, _1KMEmissive );
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }
    errStatus = copyDimension( _1KMFileID, "EV_1KM_Emissive_Uncert_Indexes", outputFile, _1KMEmissiveUncert);
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

                          
    /*__________EV_250_Aggr1km_RefSB_______________*/

    if (unpack == 1) {

        if (argv[2]!=NULL) {

            _250Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                DFNT_UINT16, _1KMFileID);
            if ( _250Aggr1km == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
                _250Aggr1km = 0;
                goto cleanupFail;
            }
            /*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

            _250Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
                    "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                    DFNT_UINT8, _1KMFileID);
            if ( _250Aggr1kmUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _250Aggr1kmUncert = 0;
                goto cleanupFail;
            }
        
        }
    }

    else {

        _250Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
        if ( _250Aggr1km == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
            _250Aggr1km = 0;
            goto cleanupFail;
        }

/*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

        _250Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
                    "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                    DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
        if ( _250Aggr1kmUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
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


        // Copy the dimensions over
        errStatus = copyDimension( _1KMFileID, "EV_250_Aggr1km_RefSB", outputFile, _250Aggr1km );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _1KMFileID, "EV_250_Aggr1km_RefSB_Uncert_Indexes", outputFile, _250Aggr1kmUncert);
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
    if (unpack == 1) {
    
        if (argv[2]!=NULL) {
    
            _500Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
                  DFNT_UINT16, _1KMFileID );
            if ( _500Aggr1km == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
                 _500Aggr1km = 0;
                goto cleanupFail;
            }

        /*__________EV_500_Aggr1km_RefSB_Uncert_Indexes____________*/
    
            _500Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
                        "EV_500_Aggr1km_RefSB_Uncert_Indexes",
                        DFNT_UINT8, _1KMFileID );
            if ( _500Aggr1kmUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _500Aggr1kmUncert = 0;
                goto cleanupFail;
            }
                  

        }

    }

    else {
        _500Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
                  DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID );
        if ( _500Aggr1km == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
            _500Aggr1km = 0;
            goto cleanupFail;
        }

        _500Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
                        "EV_500_Aggr1km_RefSB_Uncert_Indexes",
                        DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
        if ( _500Aggr1kmUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            _500Aggr1kmUncert = 0;
            goto cleanupFail;
        }   
    }

    if ( unpack == 0 || argv[2] != NULL ) {
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

        // Copy the dimensions over
        errStatus = copyDimension( _1KMFileID, "EV_500_Aggr1km_RefSB", outputFile, _500Aggr1km );
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _1KMFileID, "EV_500_Aggr1km_RefSB_Uncert_Indexes", outputFile, _500Aggr1kmUncert);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
    }
    
    // Release identifiers associated with these datasets
    if ( _500Aggr1km) status = H5Dclose(_500Aggr1km); 
    _500Aggr1km = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if ( _500Aggr1kmUncert) status = H5Dclose(_500Aggr1kmUncert); 
    _500Aggr1kmUncert = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
                            
    /*_______________latitude data under geolocation_______________*/
    
    latitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
                      "Latitude",
                      DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( latitudeDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer latitude data.\n",__FILE__,__func__,__LINE__);
        latitudeDatasetID = 0;
        goto cleanupFail;
    }
    status = H5LTset_attribute_string(MODIS1KMgeolocationGroupID,"Latitude","units","degrees_north");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set latitude units attribute.\n");
        goto cleanupFail;
    }
    
    // copy dimensions over
    errStatus = copyDimension( MOD03FileID, "Latitude", outputFile, latitudeDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        
        goto cleanupFail;
    }

    if ( latitudeDatasetID) status = H5Dclose( latitudeDatasetID ); 
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    latitudeDatasetID = 0;


    
    /*_______________longitude data under geolocation______________*/
    longitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
                      "Longitude",
                      DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( longitudeDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer longitude data.\n",__FILE__,__func__,__LINE__);
        longitudeDatasetID = 0;
        goto cleanupFail;
    }
    status = H5LTset_attribute_string(MODIS1KMgeolocationGroupID,"Longitude","units","degrees_east");
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set longitude units attribute.\n");
        goto cleanupFail;
    }

    // Copy dimensions over
    errStatus = copyDimension( MOD03FileID, "Longitude", outputFile, longitudeDatasetID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimension.\n");
        goto cleanupFail;
    }
    if ( longitudeDatasetID) status = H5Dclose( longitudeDatasetID); 
    longitudeDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");


     // We add the high-resolution lat/lon only when the data is unpacked, This is actually an advanced basic-fusion version.
     if(unpack == 1) {
        if(argv[2]!=NULL) {
            // Add MODIS interpolation data
            if ( createGroup( &MODIS500mGroupID, &MODIS500mgeolocationGroupID, "Geolocation" ) )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 500m geolocation group.\n",__FILE__,__func__,__LINE__);
                MODIS500mgeolocationGroupID = 0;
                goto cleanupFail;
            }
            // Add MODIS interpolation data
            if ( createGroup( &MODIS250mGroupID, &MODIS250mgeolocationGroupID, "Geolocation" ) )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 250m geolocation group.\n",__FILE__,__func__,__LINE__);
                MODIS250mgeolocationGroupID = 0;
                goto cleanupFail;
            }

            if(-1 == readThenWrite_MODIS_HR_LatLon(MODIS500mgeolocationGroupID, MODIS250mgeolocationGroupID,"Latitude","Longitude",DFNT_FLOAT32,H5T_NATIVE_FLOAT,MOD03FileID)){
                fprintf( stderr, "[%s:%s:%d] Failed to generate MODIS 250m and 500m geolocation fields.\n",__FILE__,__func__,__LINE__);
                goto cleanupFail;

            }
        }
    }

    /*_______________Sensor Zenith under the granule group______________*/
    SensorZenithDatasetID = readThenWrite_MODIS_GeoMetry_Unpack(MODISgranuleGroupID,"SensorZenith",
                                             DFNT_FLOAT32,  MOD03FileID);
    if ( SensorZenithDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer Sensor zenith data.\n",__FILE__,__func__,__LINE__);
        SensorZenithDatasetID = 0;
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SensorZenith", outputFile, SensorZenithDatasetID);
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
    if ( SensorAzimuthDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer Sensor zenith data.\n",__FILE__,__func__,__LINE__);
        SensorAzimuthDatasetID = 0;
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SensorAzimuth", outputFile, SensorAzimuthDatasetID);
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
    if ( SolarZenithDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer Solar zenith data.\n",__FILE__,__func__,__LINE__);
        SolarZenithDatasetID = 0;
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SolarZenith", outputFile, SolarZenithDatasetID);
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
    if ( SolarAzimuthDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer Solar zenith data.\n",__FILE__,__func__,__LINE__);
        SolarAzimuthDatasetID = 0;
        goto cleanupFail;
    }

    errStatus = copyDimension( MOD03FileID, "SolarAzimuth", outputFile, SolarAzimuthDatasetID);
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


    /*_______________Sun angle under the granule group______________*/
    SDSunzenithDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun zenith",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunzenithDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer SD Sun zenith data.\n",__FILE__,__func__,__LINE__);
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

    free(correctedName); correctedName = NULL;
    if ( SDSunzenithDatasetID) status = H5Dclose(SDSunzenithDatasetID); 
    SDSunzenithDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    SDSunazimuthDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun azimuth",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunazimuthDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer SD Sun azimuth data.\n",__FILE__,__func__,__LINE__);
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


    free(correctedName); correctedName = NULL;   
    if ( SDSunazimuthDatasetID) status = H5Dclose(SDSunazimuthDatasetID); 
    SDSunazimuthDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");               
                        

    
                /*-------------------------------------
                  ------------- 500m File -------------
                  -------------------------------------*/
                  
                  
                  
                  
    /*_____________EV_250_Aggr500_RefSB____________*/
    
    if (argv[2] !=NULL) {

        if (unpack == 1) {
            _250Aggr500 = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, 
                        "EV_250_Aggr500_RefSB",
                        DFNT_UINT16, _500mFileID );
            if ( _250Aggr500 == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB data.\n",__FILE__,__func__,__LINE__);
                _250Aggr500 = 0;
                goto cleanupFail;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
    
            _250Aggr500Uncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, 
                        "EV_250_Aggr500_RefSB_Uncert_Indexes",
                        DFNT_UINT8, _500mFileID );
            if ( _250Aggr500Uncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _250Aggr500Uncert = 0;
                goto cleanupFail;
            }
           

        }
        else {
            _250Aggr500 = readThenWrite( MODIS500mdataFieldsGroupID, 
                            "EV_250_Aggr500_RefSB",
                            DFNT_UINT16,H5T_NATIVE_USHORT,  _500mFileID );

            if ( _250Aggr500 == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB data.\n",__FILE__,__func__,__LINE__);
                _250Aggr500 = 0;
                goto cleanupFail;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
        
            _250Aggr500Uncert = readThenWrite( MODIS500mdataFieldsGroupID, 
                            "EV_250_Aggr500_RefSB_Uncert_Indexes",
                            DFNT_UINT8,H5T_STD_U8LE, _500mFileID );
            if ( _250Aggr500Uncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
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
        errStatus = copyDimension( _500mFileID, "EV_250_Aggr500_RefSB", outputFile, _250Aggr500);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _500mFileID, "EV_250_Aggr500_RefSB_Uncert_Indexes", outputFile, _250Aggr500Uncert);
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
                            
        if (unpack == 1) {
            /*____________EV_500_RefSB_____________*/
        
            _500RefSB = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                     _500mFileID );
            if ( _500RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB data.\n",__FILE__,__func__,__LINE__);
                _500RefSB = 0;
                goto cleanupFail;
            }
                /*____________EV_500_RefSB_Uncert_Indexes_____________*/
        
            _500RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                          DFNT_UINT8, _500mFileID );
            if ( _500RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _500RefSBUncert = 0;
                goto cleanupFail;
            }

        }
        else {
            _500RefSB = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                    H5T_NATIVE_USHORT, _500mFileID );
            /*____________EV_500_RefSB_Uncert_Indexes_____________*/
            if ( _500RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB data.\n",__FILE__,__func__,__LINE__);
                _500RefSB = 0;
                goto cleanupFail;
            }
            _500RefSBUncert = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                          DFNT_UINT8, H5T_STD_U8LE, _500mFileID );
            if ( _500RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
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

        // Copy the dimensions over
        errStatus = copyDimension( _500mFileID, "EV_500_RefSB", outputFile, _500RefSB);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _500mFileID, "EV_500_RefSB_Uncert_Indexes", outputFile, _500RefSBUncert);
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
    
    if (argv[3] != NULL) {
        if (unpack == 1) {
            _250RefSB = readThenWrite_MODIS_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
             _250mFileID );
            if ( _250RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB data.\n",__FILE__,__func__,__LINE__);
                _250RefSB = 0;
                goto cleanupFail;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                  DFNT_UINT8, _250mFileID);
            if ( _250RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _250RefSBUncert = 0;
                goto cleanupFail;
            }

        }
        else {
            _250RefSB = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
            H5T_NATIVE_USHORT, _250mFileID );
            if ( _250RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB data.\n",__FILE__,__func__,__LINE__);
                _250RefSB = 0;
                goto cleanupFail;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                  DFNT_UINT8, H5T_STD_U8LE, _250mFileID);             
            if ( _250RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                _250RefSBUncert = 0;
                goto cleanupFail;
            }
        }

        // ATTRIBUTES
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
        errStatus = copyDimension( _250mFileID, "EV_250_RefSB", outputFile, _250RefSB);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimension.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension( _250mFileID, "EV_250_RefSB_Uncert_Indexes", outputFile, _250RefSBUncert);
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
                 
#if 0                       
    /*_______________latitude data_______________*/
    
    latitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
                      "Latitude",
                      DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    
    /*_______________longitude data______________*/
    longitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
                      "Longitude",
                      DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    
    /*_______________Sun angle under the granule group______________*/
        SDSunzenithDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun zenith",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);

        SDSunazimuthDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun azimuth",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
#endif

       
    if ( 0 )
    {
        cleanupFail: 
        fail = 1;
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
    if ( fail ) return EXIT_FAILURE;

    return EXIT_SUCCESS;
    
}

int readThenWrite_MODIS_HR_LatLon(hid_t MODIS500mgeoGroupID,hid_t MODIS250mgeoGroupID,char* latname,char* lonname,int32 h4_type,hid_t h5_type,int32 MOD03FileID) {

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
    

    status = H4readData( MOD03FileID, latname,
        (void**)&latBuffer, &latRank, latDimSizes, h4_type );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  latname );
        if ( latBuffer != NULL ) free(latBuffer);
        return -1;
    }

    status = H4readData( MOD03FileID, lonname,
        (void**)&lonBuffer, &lonRank, lonDimSizes, h4_type );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read %s data.\n", __FILE__, __func__,__LINE__,  lonname );
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
    if(latRank !=2 || lonRank!=2) {
        fprintf( stderr, "[%s:%s:%d] The latitude and longitude array rank must be 2.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
    if(latDimSizes[0]!=lonDimSizes[0] || latDimSizes[1]!=lonDimSizes[1]) {
        fprintf( stderr, "[%s:%s:%d] The latitude and longitude array rank must share the same dimension sizes.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
   
    /* END READ DATA. BEGIN Computing DATA */
    /* first 500 m */
    nRow_1km = latDimSizes[0];
    nCol_1km = latDimSizes[1];

    lat_1km_buffer = (double*)malloc(sizeof(double)*nRow_1km*nCol_1km);
    if(lat_1km_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lat_1km_buffer.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        return -1;
    }
    
    
    lon_1km_buffer = (double*)malloc(sizeof(double)*nRow_1km*nCol_1km);
    if(lon_1km_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_1km_buffer.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        return -1;
    }

    nRow_500m = 2*nRow_1km;
    nCol_500m = 2*nCol_1km;
    lat_500m_buffer = (double*)malloc(sizeof(double)*nRow_500m*nCol_500m);
    if(lat_500m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lat_500m_buffer.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        return -1;
    }
    
    
    lon_500m_buffer = (double*)malloc(sizeof(double)*4*nRow_500m*nCol_500m);
    if(lon_500m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_500m_buffer.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        if (lat_500m_buffer !=NULL) free(lat_500m_buffer);
        return -1;
    }

    // Float to double
    for (i = 0; i <nRow_1km*nCol_1km;i++){
        lat_1km_buffer[i] = (double)latBuffer[i];
        lon_1km_buffer[i] = (double)lonBuffer[i];
    }
    upscaleLatLonSpherical(lat_1km_buffer, lon_1km_buffer, nRow_1km, nCol_1km, scanSize, lat_500m_buffer, lon_500m_buffer);


    lat_output_500m_buffer = (float*)malloc(sizeof(float)*nRow_500m*nCol_500m);
    if(lat_output_500m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_500m_buffer.\n", __FILE__, __func__,__LINE__);
        if ( latBuffer != NULL ) free(latBuffer);
        if ( lonBuffer != NULL ) free(lonBuffer);
        if (lat_1km_buffer !=NULL) free(lat_1km_buffer);
        if (lon_1km_buffer !=NULL) free(lon_1km_buffer);
        if (lat_500m_buffer !=NULL) free(lat_500m_buffer);
        if (lon_500m_buffer !=NULL) free(lon_500m_buffer);
        return -1;
    }
    for (i = 0; i <nRow_500m*nCol_500m;i++)
        lat_output_500m_buffer[i] = (float)lat_500m_buffer[i];

    hsize_t temp[DIM_MAX];
    for ( i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) (2*latDimSizes[i]);

    datasetID = insertDataset( &dummy_output_file_id, &MODIS500mgeoGroupID, 1, latRank ,
         temp, h5_type, latname, lat_output_500m_buffer );

    if ( datasetID == EXIT_FAILURE )
    {
        fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, latname );
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
    if(lon_output_500m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_500m_buffer.\n", __FILE__, __func__,__LINE__);
        free(latBuffer);
        free(lonBuffer);
        free(lat_1km_buffer);
        free(lon_1km_buffer);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_output_500m_buffer);
        return -1;
    }
    for (i = 0; i <nRow_500m*nCol_500m;i++)
        lon_output_500m_buffer[i] = (float)lon_500m_buffer[i];

   
    datasetID = insertDataset( &dummy_output_file_id, &MODIS500mgeoGroupID, 1, lonRank ,
         temp, h5_type, lonname, lon_output_500m_buffer );

    if ( datasetID == EXIT_FAILURE )
    {
        fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, lonname );
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
    if(lat_250m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lat_250m_buffer.\n", __FILE__, __func__,__LINE__);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        return -1;
    }
 
    lon_250m_buffer = (double*)malloc(sizeof(double)*nRow_250m*nCol_250m);
    if(lon_250m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_250m_buffer.\n", __FILE__, __func__,__LINE__);
        free(lat_500m_buffer);
        free(lon_500m_buffer);
        free(lat_250m_buffer);
        return -1;
    }
 
    upscaleLatLonSpherical(lat_500m_buffer, lon_500m_buffer, nRow_500m, nCol_500m, scanSize, lat_250m_buffer, lon_250m_buffer);

    free(lat_500m_buffer);
    free(lon_500m_buffer);
    lat_output_250m_buffer = (float*)malloc(sizeof(float)*nRow_250m*nCol_250m);
    if(lat_output_250m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lat_output_250m_buffer.\n", __FILE__, __func__,__LINE__);
        free(lat_250m_buffer);
        free(lon_250m_buffer);
        return -1;
    }

    lon_output_250m_buffer = (float*)malloc(sizeof(float)*nRow_250m*nCol_250m);
    if(lon_output_500m_buffer == NULL) {
        fprintf( stderr, "[%s:%s:%d] Cannot allocate lon_output_250m_buffer.\n", __FILE__, __func__,__LINE__);
        free(lat_250m_buffer);
        free(lon_250m_buffer);
        free(lat_output_250m_buffer);
        return -1;
    }

    for (i = 0; i <nRow_250m*nCol_250m;i++){
        lon_output_250m_buffer[i] = (float)lon_250m_buffer[i];
        lat_output_250m_buffer[i] = (float)lat_250m_buffer[i];
    }

    free(lat_250m_buffer);
    free(lon_250m_buffer);

    for ( i = 0; i < DIM_MAX; i++ )
        temp[i] = (hsize_t) (4*latDimSizes[i]);

    datasetID = insertDataset( &dummy_output_file_id, &MODIS250mgeoGroupID, 1, latRank ,
         temp, h5_type, latname, lat_output_250m_buffer );

    if ( datasetID == EXIT_FAILURE )
    {
        fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, latname );
        free(lat_output_250m_buffer);
        free(lon_output_250m_buffer);
        return -1;
    }
    H5Dclose(datasetID);

    free(lat_output_250m_buffer);
    datasetID = insertDataset( &dummy_output_file_id, &MODIS250mgeoGroupID, 1, lonRank ,
         temp, h5_type, lonname, lon_output_250m_buffer );

    if ( datasetID == EXIT_FAILURE )
    {
        fprintf(stderr, "[%s:%s:%d] Error writing %s dataset.\n", __FILE__, __func__,__LINE__, latname );
        free(lon_output_250m_buffer);
        return -1;
    }
    
    free(lon_output_250m_buffer);
 
    
    H5Dclose(datasetID);
 
   return 0;

}

/** The following two routines were written by Yizhao Gao <ygao29@illinois.edu>. */
#if 0
/*
 * NAME:	upscaleLatLonPlanar
 * DESCRIPTION:	calculate the latitude and longitude of MODIS cell centers at finner resolution based on coarser resolution cell locations, using a planer cooridnate system
 * PARAMETERS:
 * 	double * oriLat:	the latitudes of input cells at coarser resolution
 * 	double * oriLon:	the longitudes of input cells at coarser resolution
 * 	int nRow:		the number of rows of input raster
 * 	int nRol:		the number of columns of input raster
 * 	int scanSize:		the number of cells in a scan
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * RETURN:
 * 	TYPE:	float *
 * 	VALUE:	An array of density value at each cell
 */

void upscaleLatLonPlanar(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon) {

	if(0 != nRow % scanSize) {
		printf("nRows:%d is not a multiple of scanSize: %d\n", nRow, scanSize);
		exit(1);
	}

	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);
	}

	if(NULL == (step1Lon = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);
	}

	int i, j;

	// First step for bilinear interpolation
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {

			step1Lat[i * 2 * nCol + 2 * j] = oriLat[i * nCol + j];
			step1Lon[i * 2 * nCol + 2 * j] = oriLon[i * nCol + j];

			if(j == nCol - 1) {
				step1Lat[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLat[i * nCol + j] - 0.5 * oriLat[i * nCol + j - 1];

				if(oriLon[i * nCol + j] > 90 && oriLon[i * nCol + j - 1] < -90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLon[i * nCol + j] - 0.5 * (oriLon[i * nCol + j - 1] + 360);
				}
				else if(oriLon[i * nCol + j] < -90 && oriLon[i * nCol + j - 1] > 90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * (oriLon[i * nCol + j] + 360) - 0.5 * oriLon[i * nCol + j - 1];
				}
				else{
					step1Lon[i * 2 * nCol + 2 * j + 1] = 1.5 * oriLon[i * nCol + j] - 0.5 * oriLon[i * nCol + j - 1];
				}
				if(step1Lon[i * 2 * nCol + 2 * j + 1] > 180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] -= 360;
				}
				else if(step1Lon[i * 2 * nCol + 2 * j + 1] < -180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] += 360;
				}
			}
			else {
				step1Lat[i * 2 * nCol + 2 * j + 1] = (oriLat[i * nCol + j] + oriLat[i * nCol + j + 1]) / 2;

				if(oriLon[i * nCol + j] > 90 && oriLon[i * nCol + j + 1] < -90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1] + 360) / 2;
				}
				else if(oriLon[i * nCol + j] < -90 && oriLon[i * nCol + j + 1] > 90) {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1] + 360) / 2;
				}
				else {
					step1Lon[i * 2 * nCol + 2 * j + 1] = (oriLon[i * nCol + j] + oriLon[i * nCol + j + 1]) / 2;
				}
				if(step1Lon[i * 2 * nCol + 2 * j + 1] > 180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] -= 360;
				}
				else if(step1Lon[i * 2 * nCol + 2 * j + 1] < -180) {
					step1Lon[i * 2 * nCol + 2 * j + 1] += 360;
				}
			}
		}
	}	

	// Second step for bilinear interpolation
	
	int k = 0;
	for(k = 0; k < nRow; k += scanSize) {

		//  Processing the first row in a scan
		i = k;
		for(j = 0; j < 2 * nCol; j++) {
			newLat[(i * 2) * 2 * nCol + j] = 1.25 * step1Lat[i * 2 * nCol + j] - 0.25 * step1Lat[(i + 1) * 2 * nCol + j];
		
			if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i + 1) * 2 * nCol + j] < -90) {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * step1Lon[(i + 1) * 2 * nCol + j] - 0.25 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
			}
			else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[2 * nCol + j] > 90) {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * (step1Lon[i * 2 * nCol + j] + 360) - 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
			}
			else {
				newLon[(i * 2) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
			}
			if(newLon[j] > 180) {
				newLon[(i * 2) * 2 * nCol + j] -= 360;
			}
			else if(newLon[(i * 2) * 2 * nCol + j] < -180) {
				newLon[(i * 2) * 2 * nCol + j] += 360;
			}
		}

		//  Processing intermediate rows in a scan
		for(i = k; i < k + scanSize - 1; i ++) {
			for(j = 0; j < 2 * nCol; j++) {
				newLat[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lat[i * 2 * nCol + j] + 0.25 * step1Lat[(i + 1) * 2 * nCol + j];
				newLat[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lat[i * 2 * nCol + j] + 0.75 * step1Lat[(i + 1) * 2 * nCol + j];
	
				if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i + 1) * 2 * nCol + j] < -90) {
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lon[i * 2 * nCol + j] + 0.25 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lon[i * 2 * nCol + j] + 0.75 * (step1Lon[(i + 1) * 2 * nCol + j] + 360);
				}
				else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[(i + 1) * 2 * nCol + j] > 90) {
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * (step1Lon[i * 2 * nCol + j] + 360) + 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * (step1Lon[i * 2 * nCol + j] + 360) + 0.75 * step1Lon[(i + 1) * 2 * nCol + j];
				}
				else{
					newLon[(i * 2 + 1) * 2 * nCol + j] = 0.75 * step1Lon[i * 2 * nCol + j] + 0.25 * step1Lon[(i + 1) * 2 * nCol + j];
					newLon[(i * 2 + 2) * 2 * nCol + j] = 0.25 * step1Lon[i * 2 * nCol + j] + 0.75 * step1Lon[(i + 1) * 2 * nCol + j];
				}

				if(newLon[(i * 2 + 1) * 2 * nCol + j] > 180) {
					newLon[(i * 2 + 1) * 2 * nCol + j] -= 360;
				}
				else if(newLon[(i * 2 + 1) * 2 * nCol + j] < -180) {
					newLon[(i * 2 + 1) * 2 * nCol + j] += 360;
				}
				if(newLon[(i * 2 + 2) * 2 * nCol + j] > 180) {
					newLon[(i * 2 + 2) * 2 * nCol + j] -= 360;
				}
				else if(newLon[(i * 2 + 2) * 2 * nCol + j] < -180) {
					newLon[(i * 2 + 2) * 2 * nCol + j] += 360;
				}
			}
		}	

		//  Processing the last row in a scan
		i = k + scanSize - 1;
		for(j = 0; j < 2 * nCol; j++) {
			newLat[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lat[(nRow - 1) * 2 * nCol + j] - 0.25 * step1Lat[(nRow - 2) * 2 * nCol + j];

			if(step1Lon[i * 2 * nCol + j] > 90 && step1Lon[(i - 1) * 2 * nCol + j] < -90) {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * (step1Lon[(i - 1) * 2 * nCol + j] + 360);
			}
			else if(step1Lon[i * 2 * nCol + j] < -90 && step1Lon[(i - 1) * 2 * nCol + j] > 90) {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * (step1Lon[i * 2 * nCol + j] + 360) - 0.25 * step1Lon[(i - 1) * 2 * nCol + j];
			}
			else {
				newLon[(2 * i + 1) * 2 * nCol + j] = 1.25 * step1Lon[i * 2 * nCol + j] - 0.25 * step1Lon[(i - 1) * 2 * nCol + j];
			}
			if(newLon[(2 * i + 1) * 2 * nCol + j] > 180) {
				newLon[(2 * i + 1) * 2 * nCol + j] -= 360;
			}
			else if(newLon[(2 * i + 1) * 2 * nCol + j] < -180) {
				newLon[(2 * i + 1) * 2 * nCol + j] += 360;
			}
		}
	}
/*
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			printf("%lf,%lf\n", step1Lat[i * 2 * nCol + j], step1Lon[i * 2 * nCol + j]);
		}
	}
*/
	free(step1Lat);
	free(step1Lon);	

	
	return;
}
#endif

/**
 * NAME:	upscaleLatLonSpherical
 * DESCRIPTION:	calculate the latitude and longitude of MODIS cell centers at finner resolution based on coarser resolution cell locations, using a planer cooridnate system
 * PARAMETERS:
 * 	double * oriLat:	the latitudes of input cells at coarser resolution
 * 	double * oriLon:	the longitudes of input cells at coarser resolution
 * 	int nRow:		the number of rows of input raster
 * 	int nRol:		the number of columns of input raster
 * 	int scanSize:		the number of cells in a scan
 * 	double * newLat:	the latitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * 	double * newLon:	the longitudes of output cells at finer resolution (the number of rows and columns will be doubled)
 * RETURN:
 * 	TYPE:	float *
 * 	VALUE:	An array of density value at each cell
 */

void upscaleLatLonSpherical(double * oriLat, double * oriLon, int nRow, int nCol, int scanSize, double * newLat, double * newLon) {

	if(0 != nRow % scanSize) {
		printf("nRows:%d is not a multiple of scanSize: %d\n", nRow, scanSize);
		exit(1);
	}

	double * step1Lat;
	double * step1Lon;

	if(NULL == (step1Lat = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lat\n");
		exit(1);
	}

	if(NULL == (step1Lon = (double *)malloc(sizeof(double) * 2 * nRow * nCol))) {
		printf("Out of memeory for step1Lon\n");
		exit(1);
	}

	int i, j;


	// Convert oriLat and oriLon to radians
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {
			oriLat[i * nCol + j] = oriLat[i * nCol + j] * M_PI / 180; 
			oriLon[i * nCol + j] = oriLon[i * nCol + j] * M_PI / 180; 
		}
	}

	double phi1, phi2, phi3, lambda1, lambda2, lambda3, bX, bY;
	double dPhi, dLambda;
	double a, b, x, y, z, f, delta;
	double f1, f2;


	// First step for bilinear interpolation
	f = 1.5;
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < nCol; j++) {

			step1Lat[i * 2 * nCol + 2 * j] = oriLat[i * nCol + j];
			step1Lon[i * 2 * nCol + 2 * j] = oriLon[i * nCol + j];

			if(j == nCol - 1) {
	
				phi1 = oriLat[i * nCol + j - 1];
				phi2 = oriLat[i * nCol + j];
				lambda1 = oriLon[i * nCol + j - 1];
				lambda2 = oriLon[i * nCol + j];

				dPhi = (phi2 - phi1);
				dLambda = (lambda2 - lambda1);

				a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
				delta = 2 * atan2(sqrt(a), sqrt(1-a));

				a = sin((1-f) * delta) / sin(delta);
				b = sin(f * delta) / sin(delta);

				x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
				y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
				z = a * sin(phi1) + b * sin(phi2);

				phi3 = atan2(z, sqrt(x * x + y * y));
				lambda3 = atan2(y, x);

				step1Lat[i * 2 * nCol + 2 * j + 1] = phi3;
				step1Lon[i * 2 * nCol + 2 * j + 1] = lambda3;
			}

			else {

				phi1 = oriLat[i * nCol + j];
				phi2 = oriLat[i * nCol + j + 1];
				lambda1 = oriLon[i * nCol + j];
				lambda2 = oriLon[i * nCol + j + 1];

				bX = cos(phi2) * cos(lambda2 - lambda1);
				bY = cos(phi2) * sin(lambda2 - lambda1);

				phi3 = atan2(sin(phi1) + sin(phi2), sqrt((cos(phi1) + bX) * (cos(phi1) + bX) + bY * bY));
				step1Lat[i * 2 * nCol + 2 * j + 1] = phi3;

				lambda3 = lambda1 + atan2(bY, cos(phi1) + bX) + 3 * M_PI;
				lambda3 = lambda3 - (int)(lambda3 / (2 * M_PI)) * 2 * M_PI - M_PI;

				step1Lon[i * 2 * nCol + 2 * j + 1] = lambda3;	
			}
		}
	}	

	// Second step for bilinear interpolation
	f = 1.25;
	f1 = 0.25;
	f2 = 0.75;

	int k = 0;
	for(k = 0; k < nRow; k += scanSize) {

		//  Processing the first row in a scan
		i = k;
		for(j = 0; j < 2 * nCol; j++) {
			phi1 = step1Lat[(i + 1) * 2 * nCol + j];
			phi2 = step1Lat[i * 2 * nCol + j];
			lambda1 = step1Lon[(i + 1) * 2 * nCol + j];
			lambda2 = step1Lon[i * 2 * nCol + j];
	
			dPhi = (phi2 - phi1);
			dLambda = (lambda2 - lambda1);

			a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
			delta = 2 * atan2(sqrt(a), sqrt(1-a));
	
			a = sin((1-f) * delta) / sin(delta);
			b = sin(f * delta) / sin(delta);

			x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
			y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
			z = a * sin(phi1) + b * sin(phi2);

			phi3 = atan2(z, sqrt(x * x + y * y));
			lambda3 = atan2(y, x);

			newLat[(i * 2) * 2 * nCol + j] = phi3;
			newLon[(i * 2) * 2 * nCol + j] = lambda3;
		}

		//  Processing intermediate rows in a scan
		for(i = k; i < k + scanSize - 1; i ++) {
			for(j = 0; j < 2 * nCol; j++) {
				
				phi1 = step1Lat[i * 2 * nCol + j];
				phi2 = step1Lat[(i + 1) * 2 * nCol + j];
				lambda1 = step1Lon[i * 2 * nCol + j];
				lambda2 = step1Lon[(i + 1) * 2 * nCol + j];
			
				dPhi = (phi2 - phi1);
				dLambda = (lambda2 - lambda1);

				a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
				delta = 2 * atan2(sqrt(a), sqrt(1-a));

				//Interpolate the first intermediate point
				a = sin((1-f1) * delta) / sin(delta);
				b = sin(f1 * delta) / sin(delta);
			
				x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
				y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
				z = a * sin(phi1) + b * sin(phi2);

				phi3 = atan2(z, sqrt(x * x + y * y));
				lambda3 = atan2(y, x);

				newLat[(i * 2 + 1) * 2 * nCol + j] = phi3;
				newLon[(i * 2 + 1) * 2 * nCol + j] = lambda3;

				//Interpolate the second intermediate point
				a = sin((1-f2) * delta) / sin(delta);
				b = sin(f2 * delta) / sin(delta);

				x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
				y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
				z = a * sin(phi1) + b * sin(phi2);
	
				phi3 = atan2(z, sqrt(x * x + y * y));
				lambda3 = atan2(y, x);

				newLat[(i * 2 + 2) * 2 * nCol + j] = phi3;
				newLon[(i * 2 + 2) * 2 * nCol + j] = lambda3;
			}
		}

		//  Processing the last row in a scan
		i = k + scanSize - 1;
		for(j = 0; j < 2 * nCol; j++) {
			phi1 = step1Lat[(i - 1) * 2 * nCol + j];
			phi2 = step1Lat[i * 2 * nCol + j];
			lambda1 = step1Lon[(i - 1) * 2 * nCol + j];
			lambda2 = step1Lon[i * 2 * nCol + j];

			dPhi = (phi2 - phi1);
			dLambda = (lambda2 - lambda1);

			a = sin(dPhi/2) * sin(dPhi/2) + cos(phi1) * cos(phi2) * sin(dLambda/2) * sin(dLambda/2);
			delta = 2 * atan2(sqrt(a), sqrt(1-a));

			a = sin((1-f) * delta) / sin(delta);
			b = sin(f * delta) / sin(delta);

			x = a * cos(phi1) * cos(lambda1) + b * cos(phi2) * cos(lambda2);
			y = a * cos(phi1) * sin(lambda1) + b * cos(phi2) * sin(lambda2);
			z = a * sin(phi1) + b * sin(phi2);

			phi3 = atan2(z, sqrt(x * x + y * y));
			lambda3 = atan2(y, x);

			newLat[(2 * i + 1) * 2 * nCol + j] = phi3;
			newLon[(2 * i + 1) * 2 * nCol + j] = lambda3;
		}
	}

/*
	for(i = 0; i < nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			printf("%lf,%lf\n", step1Lat[i * 2 * nCol + j], step1Lon[i * 2 * nCol + j]);
		}
	}
*/

	// Convert newLat and newLon to degrees
	for(i = 0; i < 2 * nRow; i++) {
		for(j = 0; j < 2 * nCol; j++) {
			newLat[i * 2 * nCol + j] = newLat[i * 2 * nCol + j] * 180 / M_PI;			
			newLon[i * 2 * nCol + j] = newLon[i * 2 * nCol + j] * 180 / M_PI;
		}
	}

	free(step1Lat);
	free(step1Lon);	

	
	return;



}

