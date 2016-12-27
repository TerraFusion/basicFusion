#include "libTERRA.h"
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#define STR_LEN 500


int getNextLine ( char* string, FILE* const inputFile );

int main( int argc, char* argv[] )
{
	if ( argc != 3 )
	{
		fprintf( stderr, "Usage: %s [outputFile] [inputFiles.txt] \n", argv[0] );
		return EXIT_FAILURE;
	}
	
        
	/* open the input file list */
	FILE* const inputFile = fopen( argv[2], "r" );
	char string[STR_LEN];
	
	if ( inputFile == NULL )
	{
		fprintf( stderr, "Fatal error: file \"%s\" does not exist. Exiting program.\n", argv[2] );
		
		return EXIT_FAILURE;
	}
	
	char* MOPITTargs[3];
	char* CERESargs[3];
	char* MODISargs[7];
	char* ASTERargs[4];
	char* MISRargs[11];
    
    int status = EXIT_SUCCESS;
	
	/* Main will assert that the lines found in the inputFiles.txt file match
	 * the expected input files. This will be done using string.h's strstr() function.
	 * This will halt the execution of the program if the line recieved by getNextLine
	 * does not match the expected input. In other words, only input files meant for
	 * MOPITT, CERES, ASERT etc will pass this error check.
	 */
	char MOPITTcheck[] = "MOP01";
	char CEREScheck1[] = "CER_BDS_Terra-FM1";

    /*MY 2016-12-20: add FM2 based on GZ's request. FM2 file is not available yet. */
	char CEREScheck2[] = "CER_BDS_Terra-FM2";
	char MODIScheck1[] = "MOD021KM";
	char MODIScheck2[] = "MOD02HKM";
	char MODIScheck3[] = "MOD02QKM";
	char MODIScheck4[] = "MOD03";
	char ASTERcheck[] = "AST_L1T_";
	char MISRcheck1[] = "MISR_AM1_GRP";
	char MISRcheck2[] = "MISR_AM1_AGP";

    /* MY 2016-12-21: this is added on GZ's requet. This is for solar geometry */
	char MISRcheck3[] = "MISR_AM1_GP";
         
    /* MY 2016-12-21: granule is added on GZ's request. Basically per time per granule for ASTER and MODIS.*/
        
    char granule[] ="granule";
    char modis_granule_suffix[3];
    char aster_granule_suffix[3];

    memset(modis_granule_suffix,0,3);
    memset(aster_granule_suffix,0,3);

    /*MY 2016-12-21: MODIS and ASTER have more than one granule for each orbit. We use a counter to stop the loop. */
    int modis_count = 1;
    int aster_count = 1;

    /*MY 2016-12-21: GZ also requests to unpack the MODIS,ASTER and MISR data, this is the flag for this */
    int unpack      = 0;

    /*MY 2016-12-21: Currently an environment variable TERRA_DATA_UNPACK should be set to run the program to generate the unpacked data,
         *               Without setting this environment variable, the program will just write the packed data */
        
    {
        const char *s;
        s = getenv("TERRA_DATA_UNPACK");
            
        if(s && isdigit((int)*s)) 
            if((unsigned int)strtol(s,NULL,0) == 1)
                unpack = 1;
    }
	
	/**********
	 * MOPITT *
	 **********/
	/* get the MOPITT input file paths from our inputFiles.txt */
	status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        fprintf(stderr,"[%s:%s:%d] Failed to get MOPITT line. Exiting program.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	/* assert will fail if unexpected input is present */
	assert( strstr( string, MOPITTcheck ) != NULL );
	
	MOPITTargs[0] = argv[0];
	/* allocate space for the argument */
	MOPITTargs[1] = malloc( strlen(string)+1 );

    memset(MOPITTargs[1],0,strlen(string)+1);
	/* copy the data over */
	strncpy( MOPITTargs[1], string, strlen(string) );
	MOPITTargs[2] = argv[1];

	printf("Transferring MOPITT..."); fflush(stdout);
	assert ( MOPITT( MOPITTargs ) != EXIT_FAILURE );
	printf("MOPITT done.\nTransferring CERES..."); fflush(stdout);
	
	
	/*********
	 * CERES *
	 *********/
	/* get the CERES input files */
	status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        fprintf(stderr,"[%s:%s:%d] Failed to get CERES line. Exiting program.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	assert( strstr( string, CEREScheck1 ) != NULL );
	
	CERESargs[0] = argv[0];
	/* allocate memory for the argument */
	CERESargs[1] = malloc( strlen(string )+1);
        memset(CERESargs[1],0,strlen(string)+1);
	strncpy( CERESargs[1], string, strlen(string) );
	CERESargs[2] = argv[1];

	assert ( CERES( CERESargs,1) != EXIT_FAILURE );
        free(CERESargs[1]);

// UNCOMMENT When the FM2 is avaliable
#if 0
	getNextLine( string, inputFile);
	assert( strstr( string, CEREScheck2 ) != NULL );
	
	CERESargs[0] = argv[0];
	/* allocate memory for the argument */
	CERESargs[1] = malloc( strlen(string )+1);
        memset(CERESargs[1],0,strlen(string)+1);
	strncpy( CERESargs[1], string, strlen(string) );
	CERESargs[2] = argv[1];

	assert ( CERES( CERESargs) != EXIT_FAILURE );
        free(CERESargs[1]);
#endif


	printf("CERES done.\nTransferring MODIS..."); fflush(stdout);



        
	/*********
	 * MODIS *
	 *********/


	MODISargs[0] = argv[0];
	MODISargs[6] = argv[1];

    /* MY 2016-12-21: Need to form a loop to read various number of MODIS files 
    *  A pre-processing check of the inputFile should be provided to make sure the processing will not exit prematurely */
    while(modis_count != 0) {

	    /* get the MODIS 1KM file */
	    status = getNextLine( string, inputFile);
        if ( status == EXIT_FAILURE )
        {
            fprintf(stderr,"[%s:%s:%d] Failed to get MODIS 1KM line. Exiting program.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        /* Need to check more, now just assume ASTER is after */
        if(strstr(string,ASTERcheck)!=NULL) {
            modis_count = 0;
            continue;
        }
              
            /* should be 1 km */
	    assert( strstr( string, MODIScheck1 ) != NULL );

	    /* allocate memory for the argument */
	    MODISargs[1] = malloc ( strlen(string )+1 );
        memset(MODISargs[1],0,strlen(string)+1);
	    strncpy( MODISargs[1], string, strlen(string));
	
	    status = getNextLine( string, inputFile );
        if ( status == EXIT_FAILURE )
        {
            fprintf(stderr,"[%s:%s:%d] Failed to get MODIS line. Exiting program.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }
//printf("string is %s\n",string);
//
        /*This may be MOD02HKM or MOD03, so need to check */
        if(strstr(string,MODIScheck2) !=NULL) {/*Has MOD02HKM*/

	        /* allocate memory */
	        MODISargs[2] = malloc( strlen(string)+1 );
            memset(MODISargs[2],0,strlen(string)+1);
	        strncpy( MODISargs[2], string, strlen(string));
	
	        /* get the MODIS 250m file */
	        status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                fprintf(stderr,"[%s:%s:%d] Failed to get MODIS line. Exiting program.\n", __FILE__,__func__,__LINE__);
                return EXIT_FAILURE;
            }
	        assert( strstr( string, MODIScheck3 ) != NULL );
	
	        MODISargs[3] = malloc( strlen(string)+1 );
            memset(MODISargs[3],0,strlen(string)+1);
	        strncpy( MODISargs[3], string, strlen(string) );
	
	        /* get the MODIS MOD03 file */
	        status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                fprintf(stderr,"[%s:%s:%d] Failed to get MODIS line. Exiting program.\n", __FILE__,__func__,__LINE__);
                return EXIT_FAILURE;
            }
	        assert( strstr( string, MODIScheck4 ) != NULL );
	
	        MODISargs[4] = malloc( strlen( string )+1 );
            memset(MODISargs[4],0,strlen(string)+1);

            /* remember the granule number */
	        strncpy( MODISargs[4], string, strlen(string) );
            sprintf(modis_granule_suffix,"%d",modis_count);
            MODISargs[5] = malloc(strlen(granule)+strlen(modis_granule_suffix)+1);
            memset(MODISargs[5],0,strlen(granule)+strlen(modis_granule_suffix)+1);
                
            strncpy(MODISargs[5],granule,strlen(granule));
            strncat(MODISargs[5],modis_granule_suffix,strlen(modis_granule_suffix));

	        assert ( MODIS( MODISargs,modis_count,unpack) != EXIT_FAILURE );

        }
        else if(strstr(string,MODIScheck4)!=NULL) {
	        /* allocate memory */
	        MODISargs[2] = NULL;
	        MODISargs[3] = NULL;
	
	        MODISargs[4] = malloc( strlen( string )+1 );
            memset(MODISargs[4],0,strlen(string)+1);
	        strncpy( MODISargs[4], string, strlen(string) );

	        sprintf(modis_granule_suffix,"%d",modis_count);
            MODISargs[5] = malloc(strlen(granule)+strlen(modis_granule_suffix)+1);
            memset(MODISargs[5],0,strlen(granule)+strlen(modis_granule_suffix)+1);
            strncpy(MODISargs[5],granule,strlen(granule));
            strncat(MODISargs[5],modis_granule_suffix,strlen(modis_granule_suffix));

            assert ( MODIS( MODISargs,modis_count,unpack ) != EXIT_FAILURE );
        } 
        else {
            fprintf( stderr, "Fatal error: MODIS file order is not right. The current file should either be MOD03.. or MOD02HKM.. but it is %s\n",string );
            fclose(inputFile);
            free( MOPITTargs[1] );
            free( CERESargs[1] );
            free(MODISargs[1]);
            H5Fclose(outputFile);
            exit(EXIT_FAILURE);
        }

        if(MODISargs[1]!=NULL)
            free(MODISargs[1]);
        if(MODISargs[2]!=NULL)
            free(MODISargs[2]);
        if(MODISargs[3]!=NULL)
            free(MODISargs[3]);
        if(MODISargs[4]!=NULL)
            free(MODISargs[4]);
        if(MODISargs[5]!=NULL)
            free(MODISargs[5]);


        modis_count++;
         
    }

    printf("MODIS done.\nTransferring ASTER..."); fflush(stdout);
	
	/*********
	 * ASTER *
	 *********/
	
	ASTERargs[0] = argv[0];
    ASTERargs[3] = argv[1];

	/* get the ASTER input files */
    /* MY 2016-12-20, Need to loop ASTER files since the number of grnaule may be different for each orbit */
    while(aster_count != 0) {
            
	    if(strstr( string, ASTERcheck ) != NULL ){
	
	        /* allocate memory for the argument */
	        ASTERargs[1] = malloc( strlen(string )+1);
            memset(ASTERargs[1],0,strlen(string)+1);
	        strncpy( ASTERargs[1], string, strlen(string) );
            
            /* remember granule number */
            sprintf(aster_granule_suffix,"%d",aster_count);
            ASTERargs[2] = malloc(strlen(granule)+strlen(aster_granule_suffix)+1);
            memset(ASTERargs[2],0,strlen(granule)+strlen(aster_granule_suffix)+1);

            strncpy(ASTERargs[2],granule,strlen(granule));
            strncat(ASTERargs[2],aster_granule_suffix,strlen(aster_granule_suffix));
 
	        assert ( ASTER( ASTERargs,aster_count,unpack) != EXIT_FAILURE );
            aster_count++;
            free(ASTERargs[1]);
            free(ASTERargs[2]);
	        status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                fprintf(stderr,"[%s:%s:%d] Failed to get ASTER line. Exiting program.\n", __FILE__,__func__,__LINE__);
                return EXIT_FAILURE;
            }
        }
            // Assume MISR is after ASTER
            else if(strstr(string,MISRcheck1)!=NULL) 
                break;
            else {
                fprintf( stderr, "Fatal error: either the ASTER file is wrong or the MISR_GRP file is not right after ASTER file. The file name is %s .\n",string );
                fclose(inputFile);
                free( MOPITTargs[1] );
                free( CERESargs[1] );
                free(MODISargs[1]);
                H5Fclose(outputFile);
                exit(EXIT_FAILURE);
            }
        }

	printf("ASTER done.\nTransferring MISR..."); fflush(stdout);

	
	/********
	 * MISR *
	 ********/
	MISRargs[0] = argv[0];
	
	for ( int i = 1; i < 10; i++ )
	{
		/* get the next MISR input file */
		assert( strstr( string, MISRcheck1 ) != NULL );
		MISRargs[i] = malloc ( strlen( string ) +1);
        memset(MISRargs[i],0,strlen(string)+1);
		strncpy( MISRargs[i], string, strlen(string) );
		status = getNextLine( string, inputFile );
        if ( status == EXIT_FAILURE )
        {
            fprintf(stderr,"[%s:%s:%d] Failed to get MISR line. Exiting program.\n", __FILE__,__func__,__LINE__);
            printf("%s\n", string);
            return EXIT_FAILURE;
        }
	}
	
	assert( strstr( string, MISRcheck2 ) != NULL );
	MISRargs[10] = malloc ( strlen( string ) +1);
    memset(MISRargs[10],0,strlen(string)+1);
	strncpy( MISRargs[10], string, strlen(string) );
	status = getNextLine( string, inputFile );
    if ( status == EXIT_FAILURE )
    {
        fprintf(stderr,"[%s:%s:%d] Failed to get MISR line. Exiting program.\n", __FILE__,__func__,__LINE__);
        printf("%s\n", string);
        return EXIT_FAILURE;
    }
    assert( strstr( string, MISRcheck3 ) != NULL );
    MISRargs[11] = malloc ( strlen( string ) +1);
    memset(MISRargs[11],0,strlen(string)+1);
    strncpy( MISRargs[11], string, strlen(string) );

	assert ( MISR( MISRargs,unpack) != EXIT_FAILURE );
	printf("MISR done.\n");
		
	/* free all memory */
	fclose( inputFile );
	free( MOPITTargs[1] );
	for ( int i = 1; i <= 11; i++ ) free( MISRargs[i] );
	
	H5Fclose(outputFile);
	
	return 0;
}

int getNextLine ( char* string, FILE* const inputFile )
{
	
	do
	{			
		if ( fgets( string, STR_LEN, inputFile ) == NULL )
		{
            if ( feof(inputFile) != 0 )
                return -1;

			fprintf( stderr, "[%s:%s:%d] Unable to get next line.\n", __FILE__, __func__, __LINE__ );
			
			return EXIT_FAILURE;
		}
//printf("string in the inputFile is %s\n",string);
		
		
	} while ( string[0] == '#' || string[0] == '\n' || string[0] == ' ' );
	
	/* remove the trailing newline or space character from the buffer if it exists */
	size_t len = strlen( string )-1;
	if ( string[len] == '\n' || string[len] == ' ')
		string[len] = '\0';

    return EXIT_SUCCESS;	
}
