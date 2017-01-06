#include <stdio.h>
#include <stdlib.h>
#include <mfhdf.h>	// hdf4 SD interface (includes hdf.h by default)
#include <hdf5.h>	// hdf5
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
	[5] = output filename (already exists);
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
		return (EXIT_FAILURE);
	}
	
    if(argv[2]!= NULL) {
	    _500mFileID = SDstart( argv[2], DFACC_READ );
	    if ( _500mFileID < 0 )
	    {
		    fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
				 __LINE__ );
            SDend(_1KMFileID);
		    return (EXIT_FAILURE);
	    }
    }
	
    if(argv[3]!= NULL) {
	    _250mFileID = SDstart( argv[3], DFACC_READ );
	    if ( _250mFileID < 0 )
	    {
		    fprintf( stderr, "[%s:%s:%d]: Unable to open 250m file.\n", __FILE__, __func__,
				 __LINE__ );
            SDend(_1KMFileID);
            if(_500mFileID) SDend(_500mFileID);
		    return (EXIT_FAILURE);
	    }   
    }
	
	MOD03FileID = SDstart( argv[4], DFACC_READ );
	if ( MOD03FileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open MOD03 file.\n", __FILE__, __func__,
				 __LINE__ );
		SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        return (EXIT_FAILURE);
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
            SDend(_1KMFileID);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
		    return (EXIT_FAILURE);
	    }
    }
    else {
        MODISrootGroupID = H5Gopen2(outputFile, "/MODIS",H5P_DEFAULT);

        if(MODISrootGroupID <0) {
		    fprintf( stderr, "[%s:%s:%d] Failed to open MODIS root group.\n",__FILE__,__func__,__LINE__);
            SDend(_1KMFileID);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
		    return (EXIT_FAILURE);
        }
    }


        // Create the granule group under MODIS group
	if ( createGroup( &MODISrootGroupID, &MODISgranuleGroupID,argv[5] ) )
	{
        SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
		fprintf( stderr, "[%s:%s:%d] Failed to create MODIS root group.\n",__FILE__,__func__,__LINE__);
		return (EXIT_FAILURE);
	}

        
    if(H5LTset_attribute_string(MODISrootGroupID,argv[5],"GranuleTime",argv[1])<0) 
    {
        fprintf(stderr, "[%s:%s:%d] Cannot add the time stamp.\n",__FILE__,__func__,__LINE__);
        SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        return EXIT_FAILURE;
    }

	/* create the 1 kilometer product group */
	if ( createGroup ( &MODISgranuleGroupID, &MODIS1KMGroupID, "1KM" ) )
	{
		fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KM group.\n",__FILE__,__func__,__LINE__);
        SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
		return (EXIT_FAILURE);
	}
	
	// create the data fields group
	if ( createGroup ( &MODIS1KMGroupID, &MODIS1KMdataFieldsGroupID, "Data Fields" ) )
	{
		fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KM data fields group.\n",__FILE__,__func__,__LINE__);
		SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMGroupID);
        return (EXIT_FAILURE);
	}
	
	// create 1KMgeolocation fields group
	if ( createGroup( &MODIS1KMGroupID, &MODIS1KMgeolocationGroupID, "Geolocation" ) )
	{
		fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 1KMgeolocation group.\n",__FILE__,__func__,__LINE__);
		SDend(_1KMFileID);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMGroupID);
        H5Gclose(MODIS1KMdataFieldsGroupID);
        return (EXIT_FAILURE);
	}

    if (argv[2] !=NULL) {
	    /* create the 500m product group */
	    if ( createGroup ( &MODISgranuleGroupID, &MODIS500mGroupID, "500m" ) )
	    {
		    fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 500m group.\n",__FILE__, __func__,__LINE__);
            SDend(_1KMFileID);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
		    return (EXIT_FAILURE);
	    }

        if ( createGroup ( &MODIS500mGroupID, &MODIS500mdataFieldsGroupID, "Data Fields" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 500m data fields group.\n",__FILE__, __func__,__LINE__);
            SDend(_1KMFileID);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            return (EXIT_FAILURE);
        }

	
    }

    if(argv[3] !=NULL) {
	    /* create the 250m product group */
	    if ( createGroup ( &MODISgranuleGroupID, &MODIS250mGroupID, "250m" ) )
	    {
		    fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 250m group.\n",__FILE__, __func__,__LINE__);
		    if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            return (EXIT_FAILURE);
	    }

        if ( createGroup ( &MODIS250mGroupID, &MODIS250mdataFieldsGroupID, "Data Fields" ) )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create MODIS 250m data fields group.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            return (EXIT_FAILURE);
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
            _1KMDatasetID = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
                 _1KMFileID);
            if ( _1KMDatasetID == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
    
            _1KMUncertID = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                DFNT_UINT8, _1KMFileID );
            if ( _1KMUncertID == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                if(_1KMDatasetID) H5Dclose(_1KMDatasetID);
                return EXIT_FAILURE;
            }
            
            status = H5LTset_attribute_string(_1KMDatasetID,"EV_1KM_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_1KMDatasetID,"EV_1KM_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_1KMDatasetID,"EV_1KM_RefSB","valid_min",&fltTemp, 1 );
        }
    }
        
    // ELSE WE ARE NOT UNPACKING DATA
    else {
        _1KMDatasetID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
                H5T_NATIVE_USHORT, _1KMFileID);
        if ( _1KMDatasetID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            return EXIT_FAILURE;
        }
    /*______________EV_1KM_RefSB_Uncert_Indexes______________*/


        _1KMUncertID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
                DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
        if ( _1KMUncertID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            if(_1KMDatasetID) H5Dclose(_1KMDatasetID);
            return EXIT_FAILURE;
        }

    }


    // Close the identifiers related to these datasets
    H5Dclose(_1KMDatasetID);
    H5Dclose(_1KMUncertID);

                
/*___________EV_1KM_Emissive___________*/

    if(1==unpack) {

        _1KMEmissive = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                DFNT_UINT16, _1KMFileID);
        if ( _1KMEmissive == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            return EXIT_FAILURE;
        }
        

        
        _1KMEmissiveUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
                      "EV_1KM_Emissive_Uncert_Indexes",
                      DFNT_UINT8, _1KMFileID);
        if ( _1KMEmissiveUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            H5Dclose(_1KMEmissive);
            return EXIT_FAILURE;
        }
        
        status = H5LTset_attribute_string(_1KMEmissive,"EV_1KM_Emissive","units","Watts/m^2/micrometer/steradian");
        fltTemp = -999.0;
        status = H5LTset_attribute_float(_1KMEmissive,"EV_1KM_Emissive","_FillValue",&fltTemp, 1 );
        fltTemp = 0.0;
        status = H5LTset_attribute_float(_1KMEmissive,"EV_1KM_Emissive","valid_min",&fltTemp, 1 );
    }
    else {
        _1KMEmissive = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
                DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
        if ( _1KMEmissive == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            return EXIT_FAILURE;
        }

        _1KMEmissiveUncert = readThenWrite( MODIS1KMdataFieldsGroupID,
                      "EV_1KM_Emissive_Uncert_Indexes",
                      DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
        if ( _1KMEmissiveUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_1KM_Emissive_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            H5Dclose(_1KMEmissive);
            return EXIT_FAILURE;
        }
    }

    // Add attributes for these two datasets
   
    // EMISSIVE: 

    // UNCERT:
    
    // Close the identifiers related to these datasets
    H5Dclose(_1KMEmissive);
    H5Dclose(_1KMEmissiveUncert);

    /* TODO
        Here we have code structured like:
        if ( unpack == 1 )
            if ( argv[2] != NULL )
                Do unpacking work
        else
            Do normal work

        Should it not be structured:
        if ( argv[2] != NULL )
            if (unpack == 1)
                Do unpacking work
            else
                Do normal work
        First structure could possibly run code for a file that doesn't exist.
    */
						  
	/*__________EV_250_Aggr1km_RefSB_______________*/

    if(unpack == 1) {

        if(argv[2]!=NULL) {

            _250Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                DFNT_UINT16, _1KMFileID);
            if ( _250Aggr1km == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            /*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

            _250Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
                    "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                    DFNT_UINT8, _1KMFileID);
            if ( _250Aggr1kmUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_250Aggr1km);
                return EXIT_FAILURE;
            }
         
            status = H5LTset_attribute_string(_250Aggr1km,"EV_250_Aggr1km_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_250Aggr1km,"EV_250_Aggr1km_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_250Aggr1km,"EV_250_Aggr1km_RefSB","valid_min",&fltTemp, 1 );
        }
    }

    else {

        _250Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
                DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
        if ( _250Aggr1km == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            return EXIT_FAILURE;
        }

/*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/

        _250Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
                    "EV_250_Aggr1km_RefSB_Uncert_Indexes",
                    DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
        if ( _250Aggr1kmUncert == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
            if(_500mFileID) SDend(_500mFileID);
            if(_250mFileID) SDend(_250mFileID);
            SDend(MOD03FileID);
            H5Gclose(MODISrootGroupID);
            H5Gclose(MODIS1KMgeolocationGroupID);
            H5Gclose(MODIS1KMGroupID);
            H5Gclose(MODIS1KMdataFieldsGroupID);
            if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
            if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
            if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
            if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
            H5Dclose(_250Aggr1km);
            return EXIT_FAILURE;
        }

                
    }

        
    // Close the identifiers related to these datasets
    H5Dclose(_250Aggr1km);
    H5Dclose(_250Aggr1kmUncert);
        
	/*__________EV_500_Aggr1km_RefSB____________*/
	if(unpack == 1) {
	
        if(argv[2]!=NULL) {
	
	        _500Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
				  DFNT_UINT16, _1KMFileID );
            if ( _500Aggr1km == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }

		/*__________EV_500_Aggr1km_RefSB_Uncert_Indexes____________*/
	
	        _500Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
						"EV_500_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, _1KMFileID );
            if ( _500Aggr1kmUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_500Aggr1km);
                return EXIT_FAILURE;
            }
            	  
            // ATTRIBUTES
            status = H5LTset_attribute_string(_500Aggr1km,"EV_500_Aggr1km_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_500Aggr1km,"EV_500_Aggr1km_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_500Aggr1km,"EV_500_Aggr1km_RefSB","valid_min",&fltTemp, 1 );
        }

    }

    else {
        _500Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
				  DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID );
        if ( _500Aggr1km == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }

	    _500Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
						"EV_500_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
        if ( _500Aggr1kmUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_Aggr1km_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_500Aggr1km);
                return EXIT_FAILURE;
            }	
    }


    // Release identifiers associated with these datasets
    H5Dclose(_500Aggr1km);
    H5Dclose(_500Aggr1kmUncert);
							
	/*_______________latitude data under geolocation_______________*/
	
	latitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
					  "Latitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( latitudeDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer latitude data.\n",__FILE__,__func__,__LINE__);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMGroupID);
        H5Gclose(MODIS1KMdataFieldsGroupID);
        H5Gclose(MODIS1KMgeolocationGroupID);
        if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
        if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
        if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
        if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
        return EXIT_FAILURE;
    }
    
    H5Dclose( latitudeDatasetID );
	
	/*_______________longitude data under geolocation______________*/
	longitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
					  "Longitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
	if ( longitudeDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer longitude data.\n",__FILE__,__func__,__LINE__);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMgeolocationGroupID);
        H5Gclose(MODIS1KMGroupID);
        H5Gclose(MODIS1KMdataFieldsGroupID);
        if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
        if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
        if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
        if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
        return EXIT_FAILURE;
    }

    H5Dclose( longitudeDatasetID);

		
	/*_______________Sun angle under the granule group______________*/
    SDSunzenithDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun zenith",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunzenithDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer SD Sun zenith data.\n",__FILE__,__func__,__LINE__);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMGroupID);
        H5Gclose(MODIS1KMgeolocationGroupID);
        H5Gclose(MODIS1KMdataFieldsGroupID);
        if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
        if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
        if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
        if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
        return EXIT_FAILURE;
    }

    H5Dclose(SDSunzenithDatasetID);

    SDSunazimuthDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun azimuth",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
    if ( SDSunazimuthDatasetID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to transfer SD Sun azimuth data.\n",__FILE__,__func__,__LINE__);
        if(_500mFileID) SDend(_500mFileID);
        if(_250mFileID) SDend(_250mFileID);
        SDend(MOD03FileID);
        H5Gclose(MODISrootGroupID);
        H5Gclose(MODIS1KMGroupID);
        H5Gclose(MODIS1KMgeolocationGroupID);
        H5Gclose(MODIS1KMdataFieldsGroupID);
        if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
        if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
        if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
        if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
        return EXIT_FAILURE;
    }

    H5Dclose(SDSunazimuthDatasetID);

       
					
						
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
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
	
	        _250Aggr500Uncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, 
						"EV_250_Aggr500_RefSB_Uncert_Indexes",
						DFNT_UINT8, _500mFileID );
            if ( _250Aggr500Uncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_250Aggr500);
                return EXIT_FAILURE;
            }
           
            // ATTRIBUTES 
            status = H5LTset_attribute_string(_250Aggr500,"EV_250_Aggr500_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_250Aggr500,"EV_250_Aggr500_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_250Aggr500,"EV_250_Aggr500_RefSB","valid_min",&fltTemp, 1 );
        }
        else {
            _250Aggr500 = readThenWrite( MODIS500mdataFieldsGroupID, 
                            "EV_250_Aggr500_RefSB",
                            DFNT_UINT16,H5T_NATIVE_USHORT,  _500mFileID );

            if ( _250Aggr500 == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            /*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
        
            _250Aggr500Uncert = readThenWrite( MODIS500mdataFieldsGroupID, 
                            "EV_250_Aggr500_RefSB_Uncert_Indexes",
                            DFNT_UINT8,H5T_STD_U8LE, _500mFileID );
            if ( _250Aggr500Uncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_Aggr500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_250Aggr500);
                return EXIT_FAILURE;
            }

        }


        // close identifiers associated with these datasets
        H5Dclose(_250Aggr500);
        H5Dclose(_250Aggr500Uncert);
                            
                            
        if(unpack == 1) {
            /*____________EV_500_RefSB_____________*/
        
            _500RefSB = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                     _500mFileID );
            if ( _500RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
                /*____________EV_500_RefSB_Uncert_Indexes_____________*/
        
            _500RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                          DFNT_UINT8, _500mFileID );
            if ( _500RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_500RefSB);
                return EXIT_FAILURE;
            }

            // ATTRIBUTES
            status = H5LTset_attribute_string(_500RefSB,"EV_500_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_500RefSB,"EV_500_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_500RefSB,"EV_500_RefSB","valid_min",&fltTemp, 1 );
        }
        else {
            _500RefSB = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
                    H5T_NATIVE_USHORT, _500mFileID );
            /*____________EV_500_RefSB_Uncert_Indexes_____________*/
            if ( _500RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            _500RefSBUncert = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
                          DFNT_UINT8, H5T_STD_U8LE, _500mFileID );
            if ( _500RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_500_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_500RefSB);
                return EXIT_FAILURE;
            }
        }
        
        H5Dclose(_500RefSB);
        H5Dclose(_500RefSBUncert);
				
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
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                  DFNT_UINT8, _250mFileID);
            if ( _250RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_250RefSB);
                return EXIT_FAILURE;
            }
    
            // ATTRIBUTES
            status = H5LTset_attribute_string(_250RefSB,"EV_250_RefSB","units","Watts/m^2/micrometer/steradian");
            fltTemp = -999.0;
            status = H5LTset_attribute_float(_250RefSB,"EV_250_RefSB","_FillValue",&fltTemp, 1 );
            fltTemp = 0.0;
            status = H5LTset_attribute_float(_250RefSB,"EV_250_RefSB","valid_min",&fltTemp, 1 );
        }
        else {
            _250RefSB = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
            H5T_NATIVE_USHORT, _250mFileID );
            if ( _250RefSB == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                return EXIT_FAILURE;
            }
            /*____________EV_250_RefSB_Uncert_Indexes_____________*/

            _250RefSBUncert = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
                  DFNT_UINT8, H5T_STD_U8LE, _250mFileID); 			  
            if ( _250RefSBUncert == EXIT_FAILURE )
            {
                fprintf( stderr, "[%s:%s:%d] Failed to transfer EV_250_RefSB_Uncert_Indexes data.\n",__FILE__,__func__,__LINE__);
                if(_500mFileID) SDend(_500mFileID);
                if(_250mFileID) SDend(_250mFileID);
                SDend(MOD03FileID);
                H5Gclose(MODISrootGroupID);
                H5Gclose(MODIS1KMGroupID);
                H5Gclose(MODIS1KMgeolocationGroupID);
                H5Gclose(MODIS1KMdataFieldsGroupID);
                if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);      
                if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
                if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
                if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);
                H5Dclose(_250RefSB);
                return EXIT_FAILURE;
            }
        }
        H5Dclose(_250RefSB);
        H5Dclose(_250RefSBUncert);
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

       
        
	/*********************
	 * INSERT ATTRIBUTES *
	 *********************/
	 
	/*-------------------- EV_1KM_RefSB --------------------------*/
	//MODIS_1KM_RefSBAttr( _1KMDatasetID );
	
	
	
	/* release associated identifiers */
    if(_500mFileID) SDend(_500mFileID);
    if(_250mFileID) SDend(_250mFileID);
    SDend(MOD03FileID);
    H5Gclose(MODISrootGroupID);
    H5Gclose(MODIS1KMGroupID);
    H5Gclose(MODIS1KMdataFieldsGroupID);
    if(MODIS500mGroupID) H5Gclose(MODIS500mGroupID);
    if(MODIS500mdataFieldsGroupID) H5Gclose(MODIS500mdataFieldsGroupID);
    if(MODIS250mGroupID) H5Gclose(MODIS250mGroupID);
    if(MODIS250mdataFieldsGroupID) H5Gclose(MODIS250mdataFieldsGroupID);	
	H5Gclose(MODIS1KMgeolocationGroupID);


     
        // Attributes are not handled yet. 
#if 0
	H5Aclose(_1KMAttrID);
	H5Aclose(longitudeAttrID);
	H5Aclose(latitudeAttrID);
#endif
	
	
	
	return 0;
	
}
