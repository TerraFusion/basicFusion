#include <stdio.h>
#include <stdlib.h>
#include <hdf.h>
#include <hdf5.h>
#include "libTERRA.h"

int MISR( char* argv[] )
{
	/****************************************
	 *		VARIABLES		*
	 ****************************************/

	hid_t MISRrootGroupID;
	/*****************
	 * AA data files *
	 *****************/
		/* File IDs */
	int32 AAfileID;
		/* Group IDs */
	hid_t AAgroupID;
		/* Dataset IDs */
	hid_t AAredID;
	hid_t AAblueID;
	hid_t AAgreenID;

	/*****************
 	 * AF data files *
	 *****************/
		/* File IDs */
	int32 AFfileID;
		/* Group IDs */
	hid_t AFgroupID;
		/* Dataset IDs */
        hid_t AFredID;
        hid_t AFblueID;
        hid_t AFgreenID;

	/*****************
         * AN data files *
         *****************/
                /* File IDs */
        int32 ANfileID;
		/* Group IDs */
	hid_t ANgroupID; 
		/* Dataset IDs */
        hid_t ANredID;
        hid_t ANblueID;
        hid_t ANgreenID;

	/*****************
         * BA data files *
         *****************/
                /* File IDs */
        int32 BAfileID;
		/* Group IDs */
	hid_t BAgroupID;
		/* Dataset IDs */
        hid_t BAredID;
        hid_t BAblueID;
        hid_t BAgreenID;
	/*****************
         * BF data files *
         *****************/
                /* File IDs */
        int32 BFfileID;
		/* Group IDs */
	hid_t BFgroupID;
		/* Dataset IDs */
        hid_t BFredID;
        hid_t BFblueID;
        hid_t BFgreenID;
	/*****************
         * CA data files *
         *****************/
                /* File IDs */
        int32 CAfileID;
		/* Group IDs */
	hid_t CAgroupID;
		/* Dataset IDs */
        hid_t CAredID;
        hid_t CAblueID;
        hid_t CAgreenID;

	/*****************
         * CF data files *
         *****************/
                /* File IDs */
        int32 CFfileID;
		/* Group IDs */
	hid_t CFgroupID;
		/* Dataset IDs */
        hid_t CFredID;
        hid_t CFblueID;
        hid_t CFgreenID;

	/*****************
         * DA data files *
         *****************/
                /* File IDs */
        int32 DAfileID;
		/* Group IDs */
	hid_t DAgroupID;
		/* Dataset IDs */
        hid_t DAredID;
        hid_t DAblueID;
        hid_t DAgreenID;

	/*****************
         * DF data files *
         *****************/
                /* File IDs */
        int32 DFfileID;
		/* Group IDs */
	hid_t DFgroupID;
		/* Dataset IDs */
        hid_t DFredID;
        hid_t DFblueID;
        hid_t DFgreenID;

	/******************
	 * geo data files *
	 ******************/
		/* File IDs */
	int32 geoFileID;
		/* Group IDs */
	hid_t geoGroupID;
		/* Dataset IDs */
	hid_t latitudeID;
	hid_t longitudeID;

	/************************************************
	 *		OPEN INPUT FILES		*
	 ************************************************/

	AAfileID = SDstart( argv[1], DFACC_READ );
	assert ( AAfileID != -1 );

	AFfileID = SDstart( argv[2], DFACC_READ );
	assert( AFfileID != -1 );

	ANfileID = SDstart( argv[3], DFACC_READ );
	assert( ANfileID != -1 );

	BAfileID = SDstart( argv[4], DFACC_READ );
        assert( BAfileID != -1 );

	BFfileID = SDstart( argv[5], DFACC_READ );
        assert( BFfileID != -1 );

	CAfileID = SDstart( argv[6], DFACC_READ );
        assert( CAfileID != -1 );

	CFfileID = SDstart( argv[7], DFACC_READ );
        assert( CFfileID != -1 );

	DAfileID = SDstart( argv[8], DFACC_READ );
        assert( DAfileID != -1 );

	DFfileID = SDstart( argv[9], DFACC_READ );
        assert( DFfileID != -1 );

	geoFileID = SDstart( argv[10], DFACC_READ );
        assert( geoFileID != -1 );
	
	/****************************************
	 *		GROUP CREATION		*
	 ****************************************/

		/* MISR root group */
	createGroup( &outputFile, &MISRrootGroupID, "MISR" );
	assert( MISRrootGroupID != EXIT_FAILURE );
		/* AA group */
	createGroup( &MISRrootGroupID, &AAgroupID, "AA" );
        assert( AAgroupID != EXIT_FAILURE );
		/* AF group */
        createGroup( &MISRrootGroupID, &AFgroupID, "AF" );
        assert( AFgroupID != EXIT_FAILURE );
		 /* AN group */
        createGroup( &MISRrootGroupID, &ANgroupID, "AN" );
        assert( ANgroupID != EXIT_FAILURE );
		 /* BA group */
        createGroup( &MISRrootGroupID, &BAgroupID, "BA" );
        assert( BAgroupID != EXIT_FAILURE );
		 /* BF group */
        createGroup( &MISRrootGroupID, &BFgroupID, "BF" );
        assert( BFgroupID != EXIT_FAILURE );
		 /* CA group */
        createGroup( &MISRrootGroupID, &CAgroupID, "CA" );
        assert( CAgroupID != EXIT_FAILURE );
		 /* CF group */
        createGroup( &MISRrootGroupID, &CFgroupID, "CF" );
        assert( CFgroupID != EXIT_FAILURE );
		 /* DA group */
        createGroup( &MISRrootGroupID, &DAgroupID, "DA" );
        assert( DAgroupID != EXIT_FAILURE );
		 /* DF group */
        createGroup( &MISRrootGroupID, &DFgroupID, "DF" );
        assert( DFgroupID != EXIT_FAILURE );
		 /* geo group */
        createGroup( &MISRrootGroupID, &geoGroupID, "Geolocation" );
        assert( geoGroupID != EXIT_FAILURE );

	/****************************************
	 *		INSERT DATASETS		*
	 ****************************************/

		/***********
		 * AA FILE *
		 ***********/
	AAredID = readThenWrite( AAgroupID, "Red Radiance/RDQI",


	return EXIT_SUCCESS;
}
