#include <stdio.h>
#include <mfhdf.h>
#include <stdlib.h>
#include <hdf5.h>
#include <assert.h>
#include "libTERRA.h"

int ASTER( char* argv[] )
{
	/* TODO
		Need to add checks for the existence of the SWIR group.
		Some files will not have it.
	*/

	/***************************************************************************
	 *                                VARIABLES                                *
	 ***************************************************************************/
	int32 inFileID;
	hid_t ASTERrootGroupID;
	 
	/*********************** 
	 * SWIR data variables *
	 ***********************/
		/* Group IDs */
	hid_t SWIRgroupID;
		/* Dataset IDs */
	hid_t imageData4ID;
	hid_t imageData5ID;
	hid_t imageData6ID;
	hid_t imageData7ID;
	hid_t imageData8ID;
	hid_t imageData9ID;
	
	
	/***********************
	 * VNIR data variables *
	 ***********************/
	 	/* Group IDs */
	 hid_t VNIRgroupID;
	 	/* Dataset IDs */
	 hid_t imageData1ID;
	 hid_t imageData2ID;
	 hid_t imageData3NID;
	
	/**********************
	 * TIR data variables *
	 **********************/
		/* Group IDs */
	hid_t TIRgroupID;
		/* Dataset IDs */
	hid_t imageData10ID;
	hid_t imageData11ID;
	hid_t imageData12ID;
	hid_t imageData13ID;
	hid_t imageData14ID;
	
	/******************************
	 * Geolocation data variables *
	 ******************************/
		/* Group IDs */
	hid_t geoGroupID;
		/* Dataset IDs */
	hid_t latDataID;
	hid_t lonDataID;
	/* open the input file */
	inFileID = SDstart( argv[1], DFACC_READ );
	assert( inFileID != -1 );
	
	/********************************************************************************
	 *                                GROUP CREATION                                *
	 ********************************************************************************/
	
		/* ASTER root group */
	createGroup( &outputFile, &ASTERrootGroupID, "ASTER" );
	assert( ASTERrootGroupID != EXIT_FAILURE );
	
		/* SWIR group */
	createGroup( &ASTERrootGroupID, &SWIRgroupID, "SWIR" );
	assert( SWIRgroupID != EXIT_FAILURE );
	
		/* VNIR group */
	createGroup( &ASTERrootGroupID, &VNIRgroupID, "VNIR" );
	assert( VNIRgroupID != EXIT_FAILURE );
	
		/* TIR group */
	createGroup( &ASTERrootGroupID, &TIRgroupID, "TIR" );
	assert( TIRgroupID != EXIT_FAILURE );
	
		/* geolocation group */
	createGroup( &ASTERrootGroupID, &geoGroupID, "Geolocation" );
	assert( geoGroupID != EXIT_FAILURE );
	
	/******************************************************************************* 
	 *                              INSERT DATASETS                                *
	 *******************************************************************************/
	
		/* SWIR */
	imageData4ID = readThenWrite( SWIRgroupID, "ImageData4",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData4ID != EXIT_FAILURE );
	
	imageData5ID = readThenWrite( SWIRgroupID, "ImageData5",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData5ID != EXIT_FAILURE );
	
	imageData6ID = readThenWrite( SWIRgroupID, "ImageData6",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData6ID != EXIT_FAILURE );
	
	imageData7ID = readThenWrite( SWIRgroupID, "ImageData7",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData7ID != EXIT_FAILURE );
	
	imageData8ID = readThenWrite( SWIRgroupID, "ImageData8",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData8ID != EXIT_FAILURE );
	
	imageData9ID = readThenWrite( SWIRgroupID, "ImageData9",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData9ID != EXIT_FAILURE );
	
		/* VNIR */
	imageData1ID = readThenWrite( VNIRgroupID, "ImageData1",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData1ID != EXIT_FAILURE );
	
	imageData2ID = readThenWrite( VNIRgroupID, "ImageData2",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData2ID != EXIT_FAILURE );
	
	imageData3NID = readThenWrite( VNIRgroupID, "ImageData3N",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData3NID != EXIT_FAILURE );
	
		/* TIR */
	imageData10ID = readThenWrite( TIRgroupID, "ImageData10",
					DFNT_UINT16, H5T_STD_U16LE, inFileID ); 
	assert( imageData10ID != EXIT_FAILURE );
	
	imageData11ID = readThenWrite( TIRgroupID, "ImageData11",
					DFNT_UINT16, H5T_STD_U16LE, inFileID ); 
	assert( imageData11ID != EXIT_FAILURE );
	
	imageData12ID = readThenWrite( TIRgroupID, "ImageData12",
					DFNT_UINT16, H5T_STD_U16LE, inFileID ); 
	assert( imageData12ID != EXIT_FAILURE );
	
	imageData13ID = readThenWrite( TIRgroupID, "ImageData13",
					DFNT_UINT16, H5T_STD_U16LE, inFileID ); 
	assert( imageData13ID != EXIT_FAILURE );
	
	imageData14ID = readThenWrite( TIRgroupID, "ImageData14",
					DFNT_UINT16, H5T_STD_U16LE, inFileID ); 
	assert( imageData14ID != EXIT_FAILURE );
	
		/* geolocation */
	latDataID = readThenWrite( geoGroupID, "Latitude",
					DFNT_FLOAT64, H5T_NATIVE_DOUBLE, inFileID ); 
	assert( latDataID != EXIT_FAILURE );
	
	lonDataID = readThenWrite( geoGroupID, "Longitude",
					DFNT_FLOAT64, H5T_NATIVE_DOUBLE, inFileID ); 
	assert( lonDataID != EXIT_FAILURE );
	
	/* release identifiers */
	SDend(inFileID);
	H5Gclose(ASTERrootGroupID);
	H5Gclose(SWIRgroupID);
	H5Gclose(VNIRgroupID);
	H5Gclose(TIRgroupID);
	H5Gclose(geoGroupID);
	H5Dclose(imageData4ID);
	H5Dclose(imageData5ID);
	H5Dclose(imageData6ID);
	H5Dclose(imageData7ID);
	H5Dclose(imageData8ID);
	H5Dclose(imageData9ID);
	H5Dclose(imageData1ID);
	H5Dclose(imageData2ID);
	H5Dclose(imageData3NID);
	H5Dclose(imageData10ID);
	H5Dclose(imageData11ID);
	H5Dclose(imageData12ID);
	H5Dclose(imageData13ID);
	H5Dclose(imageData14ID);
	
	return EXIT_SUCCESS;
}
