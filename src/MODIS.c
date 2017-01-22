#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>  // hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>   // hdf5
#include "libTERRA.h"
#ifndef DIM_MAX
#define DIM_MAX 10
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
    herr_t status = EXIT_SUCCESS;
    float fltTemp = 0.0;
    intn statusn = 0;
    int fail = 0;
    char* correctedName = NULL;
    char* fileTime = NULL;
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
    
    /*****************
     * END VARIABLES *
     *****************/
     
    
    
    /* open the input files */
    _1KMFileID = SDstart( argv[1], DFACC_READ );
    if ( _1KMFileID < 0 )
    {
        FATAL_MSG( "Unable to open 1KM file.\n" );
        _1KMFileID = 0;
        return (EXIT_FAILURE);
    }
    
    if(argv[2]!= NULL) {
        _500mFileID = SDstart( argv[2], DFACC_READ );
        if ( _500mFileID < 0 )
        {
            fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
                 __LINE__ );
            _500mFileID = 0;
            goto cleanupFail;
        }
    }
    
    if(argv[3]!= NULL) {
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
    
    if(modis_count == 1) {
        if ( createGroup( &outputFile, &MODISrootGroupID, "MODIS" ) == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS root group.\n",__FILE__,__func__,__LINE__);
            MODISrootGroupID = 0;
            goto cleanupFail;
        }
    }
    else {
        MODISrootGroupID = H5Gopen2(outputFile, "/MODIS",H5P_DEFAULT);

        if(MODISrootGroupID <0) {
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

        
    if(H5LTset_attribute_string(MODISrootGroupID,argv[5],"GranuleTime",argv[1])<0) 
    {
        fprintf(stderr, "[%s:%s:%d] Cannot add the file path attribute.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }

    // Extract the time substring from the file path
    fileTime = getTime( argv[1], 2 );
    if(H5LTset_attribute_string(MODISrootGroupID,argv[5],"GranuleTime",fileTime)<0)
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

    if(argv[3] !=NULL) {
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
    if(unpack == 1) {
        if(argv[2]!=NULL) {

            /* Forgive me oh mighty programming gods for using the insidious goto.
             * I believe if you look carefully, goto makes a lot of sense in this context.
             */

            _1KMDatasetID = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
                 _1KMFileID);
            if ( _1KMDatasetID == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB data.\n",__FILE__,__func__,__LINE__);
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

    // Close the identifiers related to these datasets
    if( _1KMDatasetID ) status = H5Dclose(_1KMDatasetID); 
    _1KMDatasetID = 0;
    if ( status < 0 ) { WARN_MSG("H5Dclose\n");}

    if ( _1KMUncertID) status = H5Dclose(_1KMUncertID); 
    _1KMUncertID = 0;
    if ( status < 0 ) {WARN_MSG("H5Dclose\n");}

                
/*___________EV_1KM_Emissive___________*/

    if(1==unpack) {

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

     
    // Close the identifiers related to these datasets
    if ( _1KMEmissive) status = H5Dclose(_1KMEmissive);
    _1KMEmissive = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if( _1KMEmissiveUncert) status = H5Dclose(_1KMEmissiveUncert); 
    _1KMEmissiveUncert = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

                          
    /*__________EV_250_Aggr1km_RefSB_______________*/

    if(unpack == 1) {

        if(argv[2]!=NULL) {

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

        
    // Close the identifiers related to these datasets
    if( _250Aggr1km) status = H5Dclose(_250Aggr1km); 
    _250Aggr1km = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if( _250Aggr1kmUncert) status = H5Dclose(_250Aggr1kmUncert); 
    _250Aggr1kmUncert = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
   
    /*__________EV_500_Aggr1km_RefSB____________*/
    if(unpack == 1) {
    
        if(argv[2]!=NULL) {
    
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


    // Release identifiers associated with these datasets
    if( _500Aggr1km) status = H5Dclose(_500Aggr1km); 
    _500Aggr1km = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");

    if( _500Aggr1kmUncert) status = H5Dclose(_500Aggr1kmUncert); 
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

    if( longitudeDatasetID) status = H5Dclose( longitudeDatasetID); 
    longitudeDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");


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

    // get the attribute "_FillValue" value from SD Sun zenith
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
    if( SDSunazimuthDatasetID) status = H5Dclose(SDSunazimuthDatasetID); 
    SDSunazimuthDatasetID = 0;
    if ( status < 0 ) WARN_MSG("H5Dclose\n");               
                        
                /*-------------------------------------
                  ------------- 500m File -------------
                  -------------------------------------*/
                  
                  
                  
                  
    /*_____________EV_250_Aggr500_RefSB____________*/
    
    if(argv[2] !=NULL) {

        if(unpack == 1) {
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


        // close identifiers associated with these datasets
        if( _250Aggr500) status = H5Dclose(_250Aggr500); 
        _250Aggr500 = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if ( _250Aggr500Uncert) status = H5Dclose(_250Aggr500Uncert); 
        _250Aggr500Uncert = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");                    
                            
        if(unpack == 1) {
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
        
        if ( _500RefSB) status = H5Dclose(_500RefSB); 
        _500RefSB = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");

        if ( _500RefSBUncert ) status = H5Dclose(_500RefSBUncert); 
        _500RefSBUncert = 0;
        if ( status < 0 ) WARN_MSG("H5Dclose\n");
                
    }
                      
    
                /*-------------------------------------
                  ------------- 250m File -------------
                  -------------------------------------*/
                  
                  
    /*____________EV_250_RefSB_____________*/
    
    if(argv[3] != NULL) {
        if(unpack == 1) {
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
    
    /* TODO
        H5Dclose is failing on multiple datasets. Need to figure out why.
    */
    /* release associated identifiers */
    if(latitudeAttrID !=0 ) status = H5Aclose(latitudeAttrID); 
    if ( status < 0 ) WARN_MSG("H5Aclose\n");
    if(latitudeDatasetID  !=0 ) status = H5Dclose( latitudeDatasetID ); 
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(longitudeAttrID !=0 ) status = H5Aclose(longitudeAttrID);
    if ( status < 0 ) WARN_MSG("HADclose\n");
    if(longitudeDatasetID !=0 ) status = H5Dclose( longitudeDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(MOD03FileID !=0 ) statusn = SDend(MOD03FileID);
    if(MODIS1KMdataFieldsGroupID !=0 ) status = H5Gclose(MODIS1KMdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS1KMgeolocationGroupID !=0 ) status = H5Gclose(MODIS1KMgeolocationGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS1KMGroupID !=0 ) status = H5Gclose(MODIS1KMGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS250mdataFieldsGroupID !=0 ) status = H5Gclose(MODIS250mdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS250mGroupID !=0 ) status = H5Gclose(MODIS250mGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS500mdataFieldsGroupID !=0 ) status = H5Gclose(MODIS500mdataFieldsGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODIS500mGroupID !=0 ) status = H5Gclose(MODIS500mGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODISgranuleGroupID !=0 ) status = H5Gclose(MODISgranuleGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(MODISrootGroupID !=0 ) status = H5Gclose(MODISrootGroupID);
    if ( status < 0 ) WARN_MSG("H5Gclose\n");
    if(SDSunazimuthDatasetID !=0 ) status = H5Dclose(SDSunazimuthDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(SDSunzenithDatasetID !=0 ) status = H5Dclose(SDSunzenithDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_1KMAttrID !=0 ) status = H5Aclose(_1KMAttrID);
    if ( status < 0 ) WARN_MSG("H5Aclose\n");
    if(_1KMDatasetID !=0 ) status = H5Dclose(_1KMDatasetID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_1KMEmissive !=0 ) status = H5Dclose(_1KMEmissive);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_1KMEmissiveUncert !=0 ) status = H5Dclose(_1KMEmissiveUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_1KMFileID !=0 ) statusn = SDend(_1KMFileID);
    if(_1KMUncertID !=0 ) status = H5Dclose(_1KMUncertID);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250Aggr1km !=0 ) status = H5Dclose(_250Aggr1km);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250Aggr1kmUncert !=0 ) status = H5Dclose(_250Aggr1kmUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250Aggr500 !=0 ) status = H5Dclose(_250Aggr500);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250Aggr500Uncert !=0 ) status = H5Dclose(_250Aggr500Uncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250mFileID !=0 ) statusn = SDend(_250mFileID);
    if(_250RefSB !=0 ) status = H5Dclose(_250RefSB);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_250RefSBUncert !=0 ) status = H5Dclose(_250RefSBUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_500Aggr1km !=0 ) status = H5Dclose(_500Aggr1km);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_500Aggr1kmUncert !=0 ) status = H5Dclose(_500Aggr1kmUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_500mFileID !=0 ) statusn = SDend(_500mFileID);
    if(_500RefSB !=0 ) status = H5Dclose(_500RefSB);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if(_500RefSBUncert !=0 ) status = H5Dclose(_500RefSBUncert);
    if ( status < 0 ) WARN_MSG("H5Dclose\n");
    if ( correctedName != NULL ) free(correctedName);
    if ( status ) WARN_MSG("free\n");
    if ( fileTime ) free(fileTime);
    if ( fail ) return EXIT_FAILURE;

    return EXIT_SUCCESS;
    
}
