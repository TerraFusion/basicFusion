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

//herr_t MODIS_1KM_RefSBAttr( hid_t objectID );

/* MY 2016-12-20, handling the MODIS files with and without MOD02HKM and MOD02QKM. */

int MODIS( char* argv[] ,int modis_count, int unpack)
//int MODIS( char* argv[] )
{
	/*************
	 * VARIABLES *
	 *************/
	
	hid_t MODISrootGroupID;
        hid_t MODISgranuleGroupID;
	hid_t MODIS1KMdataFieldsGroupID;
	hid_t MODIS500mdataFieldsGroupID;
	hid_t MODIS250mdataFieldsGroupID;
	hid_t MODIS1KMgeolocationGroupID;

	/**********************
	 * 1KM data variables *
	 **********************/
		/* File IDs */
	int32 _1KMFileID;
		/* Dataset IDs */
	hid_t _1KMDatasetID;
	hid_t _1KMUncertID;
	hid_t _1KMEmissive;
	hid_t _1KMEmissiveUncert;
	hid_t _250Aggr1km;
	hid_t _250Aggr1kmUncert;
	hid_t _500Aggr1km;
	hid_t _500Aggr1kmUncert;
		/* Attribute IDs */
	hid_t _1KMAttrID;
		/* Group IDs */
	hid_t MODIS1KMGroupID;
	
	/***********************
	 * 500m data variables *
	 ***********************/
		/* File IDs */
	int32 _500mFileID;
		/* Dataset IDs */
	hid_t _250Aggr500;	
	hid_t _250Aggr500Uncert;
	hid_t _500RefSB;
	hid_t _500RefSBUncert;
		/* Attribute IDs */
		
		/* Group IDs */
	hid_t MODIS500mGroupID;
	
	/***********************
	 * 250m data variables *
	 ***********************/
		/* File IDs */
	int32 _250mFileID;
	
		/* Dataset IDs */
	hid_t _250RefSB;
	hid_t _250RefSBUncert;
		/* Attribute IDs */
		
		/* Group IDs */
	hid_t MODIS250mGroupID;
	
		
	/************************
	 * MOD03 data variables *
	 ************************/
	 	/* File IDs */
	int32 MOD03FileID;
	
		/* Dataset IDs */
		
		/* Attribute IDs */
		
		/* Group IDs */
		
	/* latitude data variables */
	
	hid_t latitudeDatasetID;
	hid_t latitudeAttrID;
	
	/* longitude data variables */
	
	hid_t longitudeDatasetID;
	hid_t longitudeAttrID;
	
	/* SD sun angles */

        hid_t SDSunzenithDatasetID;
        hid_t SDSunazimuthDatasetID;
	
        //char *granule_full_path;
        //char *modis_group ="/MODIS/";
	/*****************
	 * END VARIABLES *
	 *****************/
	 
	
	
	/* open the input files */
	_1KMFileID = SDstart( argv[1], DFACC_READ );
	if ( _1KMFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 1KM file.\n", __FILE__, __func__,
				 __LINE__ );
		return (EXIT_FAILURE);
	}
	
        if(argv[2]!= NULL) {
	_500mFileID = SDstart( argv[2], DFACC_READ );
	if ( _500mFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 500m file.\n", __FILE__, __func__,
				 __LINE__ );
		return (EXIT_FAILURE);
	}
        }
	
        if(argv[3]!= NULL) {
	_250mFileID = SDstart( argv[3], DFACC_READ );
	if ( _250mFileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open 250m file.\n", __FILE__, __func__,
				 __LINE__ );
		return (EXIT_FAILURE);
	}
        }
	
//printf("MOD03 file name is %s\n",argv[4]);
	MOD03FileID = SDstart( argv[4], DFACC_READ );
	if ( MOD03FileID < 0 )
	{
		fprintf( stderr, "[%s:%s:%d]: Unable to open MOD03 file.\n", __FILE__, __func__,
				 __LINE__ );
		return (EXIT_FAILURE);
	}
	
	
	/********************************************************************************
	 *                                GROUP CREATION                                *
	 ********************************************************************************/
	 
	/* outputfile already exists (created by MOPITT). Create the group directories */
	//create root MODIS group
	
        if(modis_count == 1) {
	if ( createGroup( &outputFile, &MODISrootGroupID, "MODIS" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS root group.\n", __func__);
		exit (EXIT_FAILURE);
	}
        }
        else {
            MODISrootGroupID = H5Gopen2(outputFile, "/MODIS",H5P_DEFAULT);
            if(MODISrootGroupID <0) {
		fprintf( stderr, "[%s]: Failed to open MODIS root group.\n", __func__);
		exit (EXIT_FAILURE);

            }
        }


        // Create the granule group under MODIS group
	if ( createGroup( &MODISrootGroupID, &MODISgranuleGroupID,argv[5] ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS root group.\n", __func__);
		exit (EXIT_FAILURE);
	}

        
        if(H5LTset_attribute_string(MODISrootGroupID,argv[5],"GranuleTime",argv[1])<0) 
        {
            printf("Cannot add the time stamp\n");
            return EXIT_FAILURE;
        }
	/* create the 1 kilometer product group */
	if ( createGroup ( &MODISgranuleGroupID, &MODIS1KMGroupID, "1KM" ) )
	//if ( createGroup ( &MODIS1KMdataFieldsGroupID, &MODIS1KMGroupID, "1KM" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 1KM group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	// create the data fields group
	if ( createGroup ( &MODIS1KMGroupID, &MODIS1KMdataFieldsGroupID, "Data Fields" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS data fields group.\n", __func__);
		exit (EXIT_FAILURE);
	}
	
	// create 1KMgeolocation fields group
	if ( createGroup( &MODIS1KMGroupID, &MODIS1KMgeolocationGroupID, "Geolocation" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 1KMgeolocation group.\n", __func__);
		exit (EXIT_FAILURE);
	}

        if (argv[2] !=NULL) {
	/* create the 500m product group */
	if ( createGroup ( &MODISgranuleGroupID, &MODIS500mGroupID, "500m" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 500m group.\n", __func__);
		exit (EXIT_FAILURE);
	}

        if ( createGroup ( &MODIS500mGroupID, &MODIS500mdataFieldsGroupID, "Data Fields" ) )
        {
                fprintf( stderr, "[%s]: Failed to create MODIS 500m group.\n", __func__);
                exit (EXIT_FAILURE);
        }

	
        }

        if(argv[3] !=NULL) {
	/* create the 250m product group */
	if ( createGroup ( &MODISgranuleGroupID, &MODIS250mGroupID, "250m" ) )
	{
		fprintf( stderr, "[%s]: Failed to create MODIS 250m group.\n", __func__);
		exit (EXIT_FAILURE);
	}

        if ( createGroup ( &MODIS250mGroupID, &MODIS250mdataFieldsGroupID, "Data Fields" ) )
        {
                fprintf( stderr, "[%s]: Failed to create MODIS 250m group.\n", __func__);
                exit (EXIT_FAILURE);
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

        if(unpack == 1) {
	
        if(argv[2]!=NULL) {
	_1KMDatasetID = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
					H5T_NATIVE_USHORT, _1KMFileID);
        
        	_1KMUncertID = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
					DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
	
					
        }
        }
        else {
	_1KMDatasetID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB", DFNT_UINT16, 
					H5T_NATIVE_USHORT, _1KMFileID);
		/*______________EV_1KM_RefSB_Uncert_Indexes______________*/
	/* TODO
		I do not know if H5T_STD_U8LE is the correct data type for the output.
		I could not find an explicit unsigned 8 bit int native type in HDF5 so
		I used one of the predefined types, a standard unsigned 8 bit little endian.
		Note that little endian might be the incorrect one, big endian might need to be
		used.
	*/
	_1KMUncertID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
					DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
	
        }
#if 0					
	/*______________EV_1KM_RefSB_Uncert_Indexes______________*/
	/* TODO
		I do not know if H5T_STD_U8LE is the correct data type for the output.
		I could not find an explicit unsigned 8 bit int native type in HDF5 so
		I used one of the predefined types, a standard unsigned 8 bit little endian.
		Note that little endian might be the incorrect one, big endian might need to be
		used.
	*/
	_1KMUncertID = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_RefSB_Uncert_Indexes",
					DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
#endif
					
	/*___________EV_1KM_Emissive___________*/
	
        if(1==unpack) {

	_1KMEmissive = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
	
	_1KMEmissiveUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID,
						  "EV_1KM_Emissive_Uncert_Indexes",
						  DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
	
        }
        else {
	_1KMEmissive = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_1KM_Emissive",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
       	_1KMEmissiveUncert = readThenWrite( MODIS1KMdataFieldsGroupID,
						  "EV_1KM_Emissive_Uncert_Indexes",
						  DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
	}
					
#if 0
	/*___________EV_1KM_Emissive_Uncert_Indexes*___________*/
	
	_1KMEmissiveUncert = readThenWrite( MODIS1KMdataFieldsGroupID,
						  "EV_1KM_Emissive_Uncert_Indexes",
						  DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);
#endif
						  
	/*__________EV_250_Aggr1km_RefSB_______________*/

        if(unpack == 1) {

        if(argv[2]!=NULL) {

	
	_250Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);
		/*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/
	
	_250Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
						"EV_250_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);				
					
        }
        }

        else {

	_250Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_250_Aggr1km_RefSB",
					DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID);

	/*__________EV_250_Aggr1km_RefSB_Uncert_Indexes_____________*/
	
	_250Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
						"EV_250_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID);				
	
					
        }
        
	
        
	/*__________EV_500_Aggr1km_RefSB____________*/
	if(unpack == 1) {
	
        if(argv[2]!=NULL) {
	
	_500Aggr1km = readThenWrite_MODIS_Unpack( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
				  DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID );

		/*__________EV_500_Aggr1km_RefSB_Uncert_Indexes____________*/
	
	_500Aggr1kmUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS1KMdataFieldsGroupID, 
						"EV_500_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
				  
        }
        }
        else {
        _500Aggr1km = readThenWrite( MODIS1KMdataFieldsGroupID, "EV_500_Aggr1km_RefSB",
				  DFNT_UINT16, H5T_NATIVE_USHORT, _1KMFileID );

	_500Aggr1kmUncert = readThenWrite( MODIS1KMdataFieldsGroupID, 
						"EV_500_Aggr1km_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _1KMFileID );
	
        }

						
//printf("before Latitude \n");
							
	/*_______________latitude data under geolocation_______________*/
	
	latitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
					  "Latitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
//printf("after Latitude \n");
	
	/*_______________longitude data under geolocation______________*/
	longitudeDatasetID = readThenWrite( MODIS1KMgeolocationGroupID,
					  "Longitude",
					  DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);
						
		
	/*_______________Sun angle under the granule group______________*/
        SDSunzenithDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun zenith",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);

        SDSunazimuthDatasetID = readThenWrite(MODISgranuleGroupID,"SD Sun azimuth",
                                             DFNT_FLOAT32, H5T_NATIVE_FLOAT, MOD03FileID);

       
					
						
				/*-------------------------------------
	 			  ------------- 500m File -------------
	 			  -------------------------------------*/
	 			  
	 			  
	 			  
	 			  
	/*_____________EV_250_Aggr500_RefSB____________*/
	
        if(argv[2] !=NULL) {

        if(unpack == 1) {
	_250Aggr500 = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, 
						"EV_250_Aggr500_RefSB",
						DFNT_UINT16, H5T_NATIVE_USHORT, _500mFileID );
       	/*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
	
	_250Aggr500Uncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, 
						"EV_250_Aggr500_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _500mFileID );
	 }
        else {
	_250Aggr500 = readThenWrite( MODIS500mdataFieldsGroupID, 
						"EV_250_Aggr500_RefSB",
						DFNT_UINT16, H5T_NATIVE_USHORT, _500mFileID );
		/*_____________EV_250_Aggr500_RefSB_Uncert_Indexes____________*/
	
	_250Aggr500Uncert = readThenWrite( MODIS500mdataFieldsGroupID, 
						"EV_250_Aggr500_RefSB_Uncert_Indexes",
						DFNT_UINT8, H5T_STD_U8LE, _500mFileID );

        }
						
						
        if(unpack == 1) {
	/*____________EV_500_RefSB_____________*/
	
	_500RefSB = readThenWrite_MODIS_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _500mFileID );
        	/*____________EV_500_RefSB_Uncert_Indexes_____________*/
	
	_500RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _500mFileID );
        }
        else {
	_500RefSB = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _500mFileID );
	/*____________EV_500_RefSB_Uncert_Indexes_____________*/
	
	_500RefSBUncert = readThenWrite( MODIS500mdataFieldsGroupID, "EV_500_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _500mFileID );

        }
				
       }
					  
	
	
				/*-------------------------------------
	 			  ------------- 250m File -------------
	 			  -------------------------------------*/
	 			  
	 			  
	/*____________EV_250_RefSB_____________*/
	
        if(argv[3] != NULL) {
         if(unpack == 1) {
	_250RefSB = readThenWrite_MODIS_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _250mFileID );

		/*____________EV_250_RefSB_Uncert_Indexes_____________*/
	
	_250RefSBUncert = readThenWrite_MODIS_Uncert_Unpack( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _250mFileID); 			  			
         }
         else {
	_250RefSB = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB", DFNT_UINT16,
				H5T_NATIVE_USHORT, _250mFileID );

			/*____________EV_250_RefSB_Uncert_Indexes_____________*/
	
	_250RefSBUncert = readThenWrite( MODIS250mdataFieldsGroupID, "EV_250_RefSB_Uncert_Indexes",
					  DFNT_UINT8, H5T_STD_U8LE, _250mFileID); 			  

         }
	
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
	SDend(_1KMFileID);
     if(argv[2]!=NULL)
	SDend(_500mFileID);
     if(argv[3]!=NULL)
	SDend(_250mFileID);
	SDend(MOD03FileID);
	H5Gclose(MODISrootGroupID);
	H5Gclose(MODIS1KMGroupID);
	H5Gclose(MODIS1KMgeolocationGroupID);
	H5Gclose(MODIS1KMdataFieldsGroupID);

     if(argv[2]!=NULL) { 
	H5Gclose(MODIS500mdataFieldsGroupID);
	H5Gclose(MODIS500mGroupID);
     }
     if(argv[3] != NULL) {
	H5Gclose(MODIS250mdataFieldsGroupID);
	H5Gclose(MODIS250mGroupID);
     }

     if(unpack !=1 || argv[2]!=NULL) {
	H5Dclose(_1KMDatasetID);
	H5Dclose(_1KMUncertID);
     	H5Dclose(_250Aggr1km);
	H5Dclose(_250Aggr1kmUncert);
	H5Dclose(_500Aggr1km);
	H5Dclose(_500Aggr1kmUncert);


     }
	H5Dclose(_1KMEmissive);
	H5Dclose(_1KMEmissiveUncert);
     
     if(argv[2] != NULL) {
	H5Dclose(_500RefSB);
	H5Dclose(_500RefSBUncert);
	H5Dclose(_250Aggr500);
	H5Dclose(_250Aggr500Uncert);
     }
     if(argv[3] != NULL) {
	H5Dclose(_250RefSB);
	H5Dclose(_250RefSBUncert);
     }
	H5Dclose(latitudeDatasetID);
	H5Dclose(longitudeDatasetID);
        H5Dclose(SDSunzenithDatasetID);
        H5Dclose(SDSunazimuthDatasetID);
        // Attributes are not handled yet. 
#if 0
	H5Aclose(_1KMAttrID);
	H5Aclose(longitudeAttrID);
	H5Aclose(latitudeAttrID);
#endif
	
	
	
	return 0;
	
}

#if 0
herr_t MODIS_1KM_RefSBAttr( hid_t objectID )
{
	hid_t dataspaceID;
	int dimSize;
	hid_t attrID;
	/* units */
	attrCreateString( objectID, "units", "degrees" );
	
	/* valid_range */
	dimSize = 2;
	dataspaceID = H5Screate_simple( 1, &dimSize, NULL );
	//attrID = H5Acreate( objectID, 
	return EXIT_SUCCESS;
}
#endif


