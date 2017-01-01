#include <stdio.h>
#include <mfhdf.h>
#include <stdlib.h>
#include <hdf5.h>
#include <assert.h>
#include "libTERRA.h"


/* MY 2016-12-20, KEEP the following  comments for the time being since these are how we need to unpack ASTER data. 
float unc[5][15] =
{
{0.676,0.708,0.423,0.423,0.1087,0.0348,0.0313,0.0299,0.0209,0.0159,-1,-1,-1,-1,-1},
{1.688,1.415,0.862,0.862,0.2174,0.0696,0.0625,0.0597,0.0417,0.0318,0.006822,0.006780,0.006590,0.005693,0.005225},
{2.25,1.89,1.15,1.15,0.290,0.0925,0.0830,0.0795,0.0556,0.0424,-1,-1,-1,-1,-1},
{-1,-1,-1,-1,0.290,0.409,0.390,0.332,0.245,0.265,-1,-1,-1,-1,-1},
{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1},
};

const char* metadata_gain="productmetadata.0";
//const char *band_index_str_list[10] = {"01","02","3N","3B","04","05","06","07","08","09"};
//const char *gain_stat_str_list[5]  ={"HGH","NOR","LO1","LO2","OFF"};
*/

/* MY 2016-12-20 The following 4 functions are usd to obtain the unit conversion coeffients
 * based on ASTER user's guide */

int obtain_gain_index(int32 sd_id,short gain_index[15]);
char* obtain_gain_info(char *whole_string);
short get_band_index(char *band_index_str);
short get_gain_stat(char *gain_stat_str);


int ASTER( char* argv[] ,int aster_count,int unpack)
{
	/* 
		Need to add checks for the existence of the VNIR group.
		Some files will not have it.
	*/


	/***************************************************************************
	 *                                VARIABLES                                *
	 ***************************************************************************/
	int32 inFileID;
        int32 inHFileID;
        int32 h4_status;

	hid_t ASTERrootGroupID;
        hid_t ASTERgranuleGroupID;
	 
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
         hid_t imageData3BID;
         int32 imageData3Bindex = FAIL;
	
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


	/* 
	  MY 2016-12-20:
        	Add checks for the existence of the VNIR group.
		Some files will not have it.
	*/
        char *vnir_grp_name ="VNIR";
        int32 vnir_grp_ref = -1;


         /*
          *    * Open the HDF file for reading.
          *       */
        inHFileID = Hopen(argv[1],DFACC_READ, 0);
	assert( inHFileID != -1 );

        /*
         *    * Initialize the V interface.
         *       */
        h4_status = Vstart (inHFileID);
        assert(h4_status != -1);

        /* If VNIR exists, vnir_grp_ref should be > 0 */
        vnir_grp_ref = H4ObtainLoneVgroupRef(inHFileID,vnir_grp_name);
	assert( vnir_grp_ref != -1 );

        /* No need inHFileID, close H and V interfaces */
        h4_status = Vend(inHFileID);
        assert(h4_status != -1);
        
        h4_status = Hclose(inHFileID);
        assert(h4_status != -1);
        
	/* open the input file */
	inFileID = SDstart( argv[1], DFACC_READ );
	assert( inFileID != -1 );
	
	/********************************************************************************
	 *                                GROUP CREATION                                *
	 ********************************************************************************/
	
		/* ASTER root group */

        /* MY 2016-12-20: Create the root group for the first grnaule */
        if(aster_count == 1) {
	createGroup( &outputFile, &ASTERrootGroupID, "ASTER" );
	assert( ASTERrootGroupID != EXIT_FAILURE );
        }

        else {

            ASTERrootGroupID = H5Gopen2(outputFile, "/ASTER",H5P_DEFAULT);
            if(ASTERrootGroupID <0) {
                fprintf( stderr, "[%s]: Failed to open ASTER root group.\n", __func__);
                exit (EXIT_FAILURE);

            }


        }
	
        // Create the granule group under MODIS group
        if ( createGroup( &ASTERrootGroupID, &ASTERgranuleGroupID,argv[2] ) )
        {
            fprintf( stderr, "[%s]: Failed to create ASTER granule group.\n", __func__);
            exit (EXIT_FAILURE);
        }

        /* MY 2016-12-20: Add the granule time information. Now just add the file name */
	if(H5LTset_attribute_string(ASTERrootGroupID,argv[2],"GranuleTime",argv[1])<0) 
        {
            printf("Cannot add the time stamp\n");
            return EXIT_FAILURE;
        }
	/* SWIR group */
	createGroup( &ASTERgranuleGroupID, &SWIRgroupID, "SWIR" );
	assert( SWIRgroupID != EXIT_FAILURE );
	
		/* Only add the VNIR group if it exists.*/
        if(vnir_grp_ref >0) {
	createGroup( &ASTERgranuleGroupID, &VNIRgroupID, "VNIR" );
	assert( VNIRgroupID != EXIT_FAILURE );
        }
	
		/* TIR group */
	createGroup( &ASTERgranuleGroupID, &TIRgroupID, "TIR" );
	assert( TIRgroupID != EXIT_FAILURE );
	
		/* geolocation group */
	createGroup( &ASTERgranuleGroupID, &geoGroupID, "Geolocation" );
	assert( geoGroupID != EXIT_FAILURE );
	
	/******************************************************************************* 
	 *                              INSERT DATASETS                                *
	 *******************************************************************************/
	
    /* MY 2016-12-20: The following if-block will unpack the ASTER radiance data. I would like to clean up the code a little bit later.*/
    if(unpack == 1) {

/* table 2-3 at https://lpdaac.usgs.gov/sites/default/files/public/product_documentation/aster_l1t_users_guide.pdf
 * row 0: high gain(HGH)
 * row 1: normal gain(NOR)
 * row 2: low gain 1(LO1)
 * row 3: low gain 2(LO2)
 * row 4: off(OFF) (not used just foe mapping the off information at productmetada.0)
 * Wince 3B and 3N share the same Unit conversion coefficient(UNC), we just use oen coeffieent for these two bands)
 * -1 is assigned to N/A and off case.
 *  */
float unc[5][15] =
{
{0.676,0.708,0.423,0.423,0.1087,0.0348,0.0313,0.0299,0.0209,0.0159,-1,-1,-1,-1,-1},
{1.688,1.415,0.862,0.862,0.2174,0.0696,0.0625,0.0597,0.0417,0.0318,0.006822,0.006780,0.006590,0.005693,0.005225},
{2.25,1.89,1.15,1.15,0.290,0.0925,0.0830,0.0795,0.0556,0.0424,-1,-1,-1,-1,-1},
{-1,-1,-1,-1,0.290,0.409,0.390,0.332,0.245,0.265,-1,-1,-1,-1,-1},
{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1},
};

       
        /* Mapping from name ImageData? to band index
           ImageData1 0
           ImageData2 1
           ImageData3N 2
           ImageData3B 3
           ImageData4 4
           ImageData5 5
           ImageData6 6
           ImageData7 7
           ImageData8 8
           ImageData9 9
           ImageData10 10
           ImageData11 11
           ImageData12 12
           ImageData13 13
           ImageData14 14

        */

      
        short gain_index[15];
        float band_unc = 0;

        /* Obtain the index of the gain */
        obtain_gain_index(inFileID,gain_index);
           
        /* obtain the Unit conversion coefficient */
        band_unc  = unc[gain_index[4]][4];
//printf("band_unc is %f\n",band_unc);
		/* SWIR */
        
	imageData4ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData4",
					DFNT_UINT8, inFileID,unc[gain_index[4]][4] ); 
	assert( imageData4ID != EXIT_FAILURE );
	
	imageData5ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData5",
					DFNT_UINT8, inFileID, unc[gain_index[5]][5] ); 
	assert( imageData5ID != EXIT_FAILURE );
	
	imageData6ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData6",
					DFNT_UINT8, inFileID , unc[gain_index[6]][6]); 
	assert( imageData6ID != EXIT_FAILURE );
	
	imageData7ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData7",
					DFNT_UINT8, inFileID, unc[gain_index[7]][7] ); 
	assert( imageData7ID != EXIT_FAILURE );
	
	imageData8ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData8",
					DFNT_UINT8, inFileID , unc[gain_index[8]][8]); 
	assert( imageData8ID != EXIT_FAILURE );
	
	imageData9ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData9",
					DFNT_UINT8, inFileID ,unc[gain_index[9]][9]); 
	assert( imageData9ID != EXIT_FAILURE );
	
		/* VNIR */
        if(vnir_grp_ref >0) {
	imageData1ID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData1",
					DFNT_UINT8, inFileID,unc[gain_index[0]][0] ); 
	assert( imageData1ID != EXIT_FAILURE );
	
	imageData2ID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData2",
					DFNT_UINT8, inFileID,unc[gain_index[1]][1] ); 
	assert( imageData2ID != EXIT_FAILURE );
	
	imageData3NID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData3N",
					DFNT_UINT8, inFileID,unc[gain_index[2]][2] ); 
	assert( imageData3NID != EXIT_FAILURE );

        /* We don't see ImageData3B in the current orbit, however, the table indeed indicates the 3B band.
           So here we check if there is an SDS with the name "ImangeData3B" and then do the conversation. 
        */

        imageData3Bindex = SDnametoindex(inFileID,"ImageData3B");
        if(imageData3Bindex != FAIL) {

            imageData3BID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData3B",
                                        DFNT_UINT8, inFileID,unc[gain_index[3]][3] );
	    assert( imageData3BID != EXIT_FAILURE );

        }

        }// end if(vnir_grp_ref
	
		/* TIR */
	imageData10ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData10",
					DFNT_UINT16, inFileID,unc[gain_index[10]][10] ); 
	assert( imageData10ID != EXIT_FAILURE );
	
	imageData11ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData11",
					DFNT_UINT16, inFileID, unc[gain_index[11]][11] ); 
	assert( imageData11ID != EXIT_FAILURE );
	
	imageData12ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData12",
					DFNT_UINT16, inFileID, unc[gain_index[12]][12] ); 
	assert( imageData12ID != EXIT_FAILURE );
	
	imageData13ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData13",
					DFNT_UINT16, inFileID, unc[gain_index[12]][12] ); 
	assert( imageData13ID != EXIT_FAILURE );
	
	imageData14ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData14",
					DFNT_UINT16, inFileID,unc[gain_index[13]][13] ); 
	assert( imageData14ID != EXIT_FAILURE );
    } // end if(unpacked =1)

    else {
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
        if(vnir_grp_ref >0) {
	imageData1ID = readThenWrite( VNIRgroupID, "ImageData1",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData1ID != EXIT_FAILURE );
	
	imageData2ID = readThenWrite( VNIRgroupID, "ImageData2",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData2ID != EXIT_FAILURE );
	
	imageData3NID = readThenWrite( VNIRgroupID, "ImageData3N",
					DFNT_UINT8, H5T_STD_U8LE, inFileID ); 
	assert( imageData3NID != EXIT_FAILURE );

         /* We don't see ImageData3B in the current orbit, however, the table indeed indicates the 3B band.
           So here we check if there is an SDS with the name "ImangeData3B" and then do the conversation. 
        */

        imageData3Bindex = SDnametoindex(inFileID,"ImageData3B");
        if(imageData3Bindex != FAIL) {

            imageData3BID = readThenWrite( VNIRgroupID, "ImageData3B",
                                        DFNT_UINT8, H5T_STD_U8LE, inFileID);
	    assert( imageData3BID != EXIT_FAILURE );

        }

       }
	
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


    }// else (packed)
	
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
	H5Gclose(ASTERgranuleGroupID);
	H5Gclose(SWIRgroupID);
        
        if(vnir_grp_ref >0) 
	   H5Gclose(VNIRgroupID);
	H5Gclose(TIRgroupID);
	H5Gclose(geoGroupID);
	H5Dclose(imageData4ID);
	H5Dclose(imageData5ID);
	H5Dclose(imageData6ID);
	H5Dclose(imageData7ID);
	H5Dclose(imageData8ID);
	H5Dclose(imageData9ID);

        if(vnir_grp_ref >0) {
	H5Dclose(imageData1ID);
	H5Dclose(imageData2ID);
	H5Dclose(imageData3NID);
        if(imageData3Bindex !=FAIL) 
            H5Dclose(imageData3BID);
        }
	H5Dclose(imageData10ID);
	H5Dclose(imageData11ID);
	H5Dclose(imageData12ID);
	H5Dclose(imageData13ID);
	H5Dclose(imageData14ID);
	
	return EXIT_SUCCESS;
}

char* obtain_gain_info(char *whole_string) {

    char* begin_mark="GAININFORMATION\n";
    char* sub_string=strstr(whole_string,begin_mark);

    return sub_string;
}

short get_band_index(char *band_index_str) {

    const char *band_index_str_list[10] = {"01","02","3N","3B","04","05","06","07","08","09"};
    int ret_value = -1;
    int i = 0;
    for(i= 0; i <11;i++) {

        if(!strncmp(band_index_str,band_index_str_list[i],2)){
//printf("band_index is %s\n",band_index_str);
                ret_value = i;
                break;
        }
    }

    return ret_value;
}

short get_gain_stat(char *gain_stat_str) {

    const char *gain_stat_str_list[5]  ={"HGH","NOR","LO1","LO2","OFF"};
    int ret_value = -1;
    int i = 0;
    for(i= 0; i <5;i++) {

        if(!strncmp(gain_stat_str,gain_stat_str_list[i],3)){
            ret_value = i;
            break;
        }
    }

    return ret_value;
}

int obtain_gain_index(int32 sd_id,short gain_index[15]) {

    const char* metadata_gain="productmetadata.0";
    intn    status;
   int32   attr_index, data_type, n_values;
   char    attr_name[H4_MAX_NC_NAME];
   int     i = 0;
   int     band_index = -1;
   int     temp_gain_index = -1;


    int ret_value = 0;
   /*
   * Find the file attribute defined by FILE_ATTR_NAME.
   */
   attr_index = SDfindattr (sd_id, metadata_gain);

   /*
   * Get information about the file attribute. Note that the first
   * parameter is an SD interface identifier.
   */
   status = SDattrinfo (sd_id, attr_index, attr_name, &data_type, &n_values);

   /* The data type should be DFNT_CHAR, from SD_set_attr.c */
   if (data_type == DFNT_CHAR)
   {
      char *fileattr_data= NULL;
      char * string_gaininfo = NULL;
      char * string_nogaininfo = NULL;
      char* gain_string = NULL;
      size_t gain_string_len = 0;
      char*  tp= NULL;
      char* band_index_str;
      char* gain_stat_str;
      int  gain[15];


      /*
      * Allocate a buffer to hold the attribute data.
      */
      fileattr_data = (char *)HDmalloc (n_values * sizeof(char));

      /*
      * Read the file attribute data.
      */
      status = SDreadattr (sd_id, attr_index, fileattr_data);

      /*
      * Print out file attribute value and free buffer.
      */
//      printf ("File attribute value is : %s\n", fileattr_data);
      //fileattr_data[n_values-1]='\0';
      string_gaininfo = obtain_gain_info(fileattr_data);

      //printf("sub string 1 is %s\n",string_gaininfo);
      string_gaininfo++;
//printf("string_gaininfo size is %d\n",strlen(string_gaininfo));
      string_nogaininfo = obtain_gain_info(string_gaininfo);
      //printf("sub string 2 is %s\n",string_nogaininfo);
//printfk-check=full("string_nogaininfo size is %d\n",strlen(string_nogaininfo));

      // Somehow valgrind complains. Need to check why.
      gain_string_len = strlen(string_gaininfo)-strlen(string_nogaininfo);
      gain_string=malloc(gain_string_len+1);
      strncpy(gain_string,string_gaininfo,gain_string_len);

      gain_string[gain_string_len]='\0';
      tp = gain_string;

      band_index_str = malloc(3);

      memset(band_index_str,0,3);
      gain_stat_str  = malloc(4);
      memset(gain_stat_str,0,4);


      while(*tp!='\0') {
          tp = strchr(tp,'(');
          if(tp == NULL)
             break;
          // Move to the band string
          tp+=2;
          // tp starts with the number
          strncpy(band_index_str,tp,2);
//printf("band_index_str is %s\n",band_index_str);
          band_index = get_band_index(band_index_str);
//printf("band_index is %d\n",band_index);


          //skip 6 characters starting from 0 [01", "]
          tp+=6;
          strncpy(gain_stat_str,tp,3);
//printf("temp_gain_index is %s\n",gain_stat_str);

          temp_gain_index = get_gain_stat(gain_stat_str);

          gain[band_index] = temp_gain_index;

      }

      // Always normal gain
      if(band_index <11) {
            for (i = 10;i<15;i++)
                 gain[i] = 1;
      }

#if 0
      for(i = 0; i <15;i++)
         printf("For band %d, the gain index is %d\n",i,gain[i]);
#endif

      for(i = 0; i <15;i++)
          gain_index[i] = gain[i];
            


//printf("gain_string is %s\n",gain_string);


      //while(strstr
      free(gain_string);

      free(band_index_str);
      free(gain_stat_str);

      free (fileattr_data);
   }

   return ret_value;
}
