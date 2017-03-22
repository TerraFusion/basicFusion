#include "libTERRA.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <curses.h>
#define STR_LEN 500
#define LOGIN_NODE "login"
#define MOM_NODE



int getNextLine ( char* string, FILE* const inputFile );

int main( int argc, char* argv[] )
{

    /* Check that this program is not being run on a login node or a MOM node.
     * If it is, shame on you!
     */

    #if 0
    {
        const char* hostname = getenv("HOSTNAME");
        printf("%s", hostname );
        if ( strstr( hostname, LOGIN_NODE ) != NULL )
        {
            fprintf( stderr, "\n\t\x1b[1m\x1b[31m\x1b[47m*******************\n");
            fprintf( stderr, "\t* \x1b[4mCRITICAL ERROR\x1b[24m! *\n");
            fprintf( stderr, "\t*******************\x1b[0m\n\n");
            fprintf( stderr, "You have attempted to run this program on a non-compute node!\n");
            fprintf( stderr, "Don't ever do such a thing! Please refer to README.txt in the root project directory.\n\n");
            return EXIT_FAILURE;
        }

    }   
    #endif

    if ( argc != 3 )
    {
        fprintf( stderr, "Usage: %s [outputFile] [inputFiles.txt] \n", argv[0] );
        fprintf( stderr, "Set environment variable TERRA_DATA_UNPACK to non-zero for unpacking.\n");
        return EXIT_FAILURE;
    }
    

    FILE* new_orbit_info_b = fopen("orbit_info.bin","r");
    long fSize;
    fseek(new_orbit_info_b,0,SEEK_END);
    fSize = ftell(new_orbit_info_b);
    rewind(new_orbit_info_b);
    printf("file size is %d\n",(int)fSize);

    OInfo_t* test_orbit_ptr = NULL;
    test_orbit_ptr = calloc(fSize/sizeof(OInfo_t),sizeof(OInfo_t));
    size_t result = fread(test_orbit_ptr,sizeof(OInfo_t),fSize/sizeof(OInfo_t),new_orbit_info_b);
    if(result != fSize/sizeof(OInfo_t)) {
          free(test_orbit_ptr);
          fclose(new_orbit_info_b);
          printf("fread is not successful.\n");
          return EXIT_FAILURE;
    }
    
    OInfo_t current_orbit_info;

    // This number needs to be obtained from the input file eventually.

    int current_orbit_number = 40110;
    for (int i = 0; i<fSize/sizeof(OInfo_t);i++) {
        if(test_orbit_ptr[i].orbit_number == current_orbit_number)
            current_orbit_info = test_orbit_ptr[i];
    }
    fclose(new_orbit_info_b);
    free(test_orbit_ptr);
        
    /* open the input file list */
    FILE* const inputFile = fopen( argv[2], "r" );
    char string[STR_LEN];
    
    if ( inputFile == NULL )
    {
        FATAL_MSG("file \"%s\" does not exist. Exiting program.\n", argv[2]);
        return EXIT_FAILURE;
    }
    
    char* MOPITTargs[3] = {NULL};
    char* CERESargs[3] = {NULL};
    char* MODISargs[7] = {NULL};
    char* ASTERargs[4] = {NULL};
    char* MISRargs[12] = {NULL};
    
    int status = EXIT_SUCCESS;
    int fail = 0;
    
    /* Main will assert that the lines found in the inputFiles.txt file match
     * the expected input files. This will be done using string.h's strstr() function.
     * This will halt the execution of the program if the line recieved by getNextLine
     * does not match the expected input. In other words, only input files meant for
     * MOPITT, CERES, ASTER etc will pass this error check.
     */
    char MOPITTcheck[] = "MOP01";
    char CEREScheck1[] = "CER_SSF_Terra-FM1";

    /*MY 2016-12-20: add FM2 based on GZ's request.  */
    char CEREScheck2[] = "CER_SSF_Terra-FM2";
    char MODIScheck1[] = "MOD021KM";
    char MODIScheck2[] = "MOD02HKM";
    char MODIScheck3[] = "MOD02QKM";
    char MODIScheck4[] = "MOD03";
    char ASTERcheck[] = "AST_L1T_";
    char MISRcheck1[] = "MISR_AM1_GRP";
    char MISRcheck2[] = "MISR_AM1_AGP";

    /* MY 2016-12-21: this is added on GZ's requet. This is for solar geometry */
    char MISRcheck3[] = "MISR_AM1_GP";

     /* MY 2017-03-04: Add high-resolution Lat/lon  */
    char MISRcheck4[] = "MISR_HRLL";
        
    /* MY 2016-12-21: granule is added on GZ's request. Basically per time per granule for ASTER and MODIS.*/
        
    char granule[] ="granule";
    char modis_granule_suffix[3] = {'\0'};
    char aster_granule_suffix[3] = {'\0'};

    memset(modis_granule_suffix,0,3);
    memset(aster_granule_suffix,0,3);

    /*MY 2016-12-21: MODIS and ASTER have more than one granule for each orbit. We use a counter to stop the loop. */
    int ceres_count = 1;
    int ceres_fm1_count = 1;
    int ceres_fm2_count = 1;
    int ceres_start_index = 0;
    int* ceres_start_index_ptr=&ceres_start_index;
    int ceres_end_index = 0;
    int* ceres_end_index_ptr=&ceres_end_index;
    int modis_count = 1;
    int aster_count = 1;

    /*MY 2016-12-21: GZ also requests to unpack the MODIS,ASTER and MISR data, this is the flag for this */
    int unpack      = 0;

    /*MY 2016-12-21: Currently an environment variable TERRA_DATA_UNPACK should be set 
     *to run the program to generate the unpacked data,
     *without setting this environment variable, the program will just write the packed data */
        
    {
        const char *s;
        s = getenv("TERRA_DATA_UNPACK");
            
        if(s && isdigit((int)*s)) 
            if((unsigned int)strtol(s,NULL,0) == 1)
                unpack = 1;
    }
    
    if ( unpack ) printf("\n_____UNPACKING ENABLED_____\n");
    else printf("\n_____UNPACKING DISABLED_____\n");


    /* remove output file if it already exists. Note that no conditional statements are used. If file does not exist,
     * this function will throw an error but we do not care.
     */
    /* MY 2017-01-27: remove is not necessary. The HDF5 file create operation will handle this 
                      Correction: leave it for the time being since the createOutputFile uses the EXCL flag.
                      TODO: Will turn this off in the operation.
    */
    remove( argv[1] );

    /* create the output file or open it if it exists */
    if ( createOutputFile( &outputFile, argv[1] )) 
    {    
        FATAL_MSG("Unable to create output file.\n");
        outputFile = 0;
        goto cleanupFail;
    }
   

    /**********
     * MOPITT *
     **********/
    /* get the MOPITT input file paths from our inputFiles.txt */
    status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get MOPITT line. Exiting program.\n");
        goto cleanupFail;
    }
    /* strstr will fail if unexpected input is present */
    if ( strstr( string, MOPITTcheck ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for MOPITT.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n",string,MOPITTcheck);
        goto cleanupFail;
    }
    
    MOPITTargs[0] = argv[0];

    /* allocate space for the argument */
    MOPITTargs[1] = malloc( strlen(string)+1 );
    memset(MOPITTargs[1],0,strlen(string)+1);

    /* copy the data over */
    strncpy( MOPITTargs[1], string, strlen(string) );
    MOPITTargs[2] = argv[1];

    printf("Transferring MOPITT..."); fflush(stdout);
    if ( MOPITT( MOPITTargs, current_orbit_info ) == EXIT_FAILURE )
    {
        FATAL_MSG("MOPITT failed data transfer.\nExiting program.\n");
        goto cleanupFail;
    }

    free(MOPITTargs[1]); MOPITTargs[1] = NULL;

    printf("MOPITT done.\nTransferring CERES..."); fflush(stdout);
    
    /*********
     * CERES *
     *********/
    /* Get the CERES  */
#if 0
    status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get CERES line. Exiting program.\n");
        goto cleanupFail;
    }


    if ( strstr( string, CEREScheck1 ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for CERES.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, CEREScheck1);
        goto cleanupFail;
    }
    
    CERESargs[0] = argv[0];

    /* Allocate memory for the argument */
    CERESargs[1] = malloc( strlen(string )+1);
    memset(CERESargs[1],0,strlen(string)+1);
    strncpy( CERESargs[1], string, strlen(string) );
    CERESargs[2] = argv[1];

    if ( CERES( CERESargs,1) == EXIT_FAILURE )
    {
        FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
        goto cleanupFail;
    }

    
    free(CERESargs[1]); CERESargs[1] = NULL;

    // Handler CERES FM2
    getNextLine( string, inputFile);
    if ( strstr( string, CEREScheck2 ) == NULL ) 
    {
        FATAL_MSG("Failed to get CERES line.\nExpected: %s\nRecieved: %s\n", CEREScheck2, string);
        goto cleanupFail;
    }
    
    CERESargs[0] = argv[0];

    /* Allocate memory for the argument */
    CERESargs[1] = malloc( strlen(string )+1);
    memset(CERESargs[1],0,strlen(string)+1);
    strncpy( CERESargs[1], string, strlen(string) );
    CERESargs[2] = argv[1];

    if ( CERES( CERESargs,2) == EXIT_FAILURE )
    {
        FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
        goto cleanupFail;
    }
    free(CERESargs[1]); CERESargs[1] = NULL;


    printf("CERES done.\nTransferring MODIS..."); fflush(stdout);

#endif

  CERESargs[0] = argv[0];
  CERESargs[1] = argv[1];

  while(ceres_count != 0) {
    status = getNextLine( string, inputFile);
printf("string is %s\n",string);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get CERES line. Exiting program.\n");
        goto cleanupFail;
    }

    if(strstr(string,MODIScheck1) != NULL) {
        ceres_count = 0;
        continue;
    }

    if ( strstr( string, CEREScheck1 ) != NULL )
    {
       CERESargs[2] = malloc(strlen(string)+1); 
       memset(CERESargs[2],0,strlen(string)+1);
       strncpy( CERESargs[2], string, strlen(string) );
       //FM1
       ceres_fm1_count++;
       status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
            goto cleanupFail;
       }

// Temp turn off for debugging
#if 0
       status = CERES_Subset(CERESargs,1,ceres_fm1_count,unpack,ceres_start_index,ceres_end_index);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
            goto cleanupFail;
       }
#endif

    }
    else if ( strstr( string, CEREScheck2 ) != NULL )
    {
       CERESargs[2] = malloc(strlen(string)+1); 
       memset(CERESargs[2],0,strlen(string)+1);
       strncpy( CERESargs[2], string, strlen(string) );
       //FM2
       ceres_fm2_count++;
       status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
            goto cleanupFail;
       }

#if 0
       status = CERES_Subset(CERESargs,2,ceres_fm2_count,unpack,ceres_start_index,ceres_end_index);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
            goto cleanupFail;
       }
#endif

    }
    else {
        FATAL_MSG("CERES files are neither CER_SSF_Terra-FM1 nor CER_SSF_Terra-FM2.\n");
        goto cleanupFail;
    }

    if(CERESargs[2]) free(CERESargs[2]); CERESargs[2] = NULL;   
    ceres_count++;
  }

    printf("CERES done.\nTransferring MODIS..."); fflush(stdout);


    /*********
     * MODIS *
     *********/


    MODISargs[0] = argv[0];
    MODISargs[6] = argv[1];

    /* MY 2016-12-21: Need to form a loop to read various number of MODIS files 
    *  A pre-processing check of the inputFile should be provided to make sure the processing 
    *  will not exit prematurely */
    while(modis_count != 0) {

#if 0
        /* get the MODIS 1KM file */
        status = getNextLine( string, inputFile);
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get MODIS 1KM line. Exiting program.\n");
            goto cleanupFail;
        }
#endif

        /* Need to check more, now just assume ASTER is after */
        if(strstr(string,ASTERcheck)!=NULL) {
            modis_count = 0;
            continue;
        }

        /* Should be 1 km */
        if ( strstr( string, MODIScheck1 ) == NULL )
        {
            FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MODIScheck1);
            goto cleanupFail;
        }

 
        /* Allocate memory for the argument */
        MODISargs[1] = malloc ( strlen(string )+1 );
        memset(MODISargs[1],0,strlen(string)+1);
        strncpy( MODISargs[1], string, strlen(string));
    
        status = getNextLine( string, inputFile );
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
            goto cleanupFail;
        }

        /*This may be MOD02HKM or MOD03, so need to check */
        if(strstr(string,MODIScheck2) !=NULL) {/*Has MOD02HKM*/

            /* Allocate memory */
            MODISargs[2] = malloc( strlen(string)+1 );
            memset(MODISargs[2],0,strlen(string)+1);
            strncpy( MODISargs[2], string, strlen(string));
    
            /* Get the MODIS 250m file */
            status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
                goto cleanupFail;
            }


            if ( strstr( string, MODIScheck3 ) == NULL )
            {
                FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MODIScheck3);
                goto cleanupFail;
            }   
    
            MODISargs[3] = malloc( strlen(string)+1 );
            memset(MODISargs[3],0,strlen(string)+1);
            strncpy( MODISargs[3], string, strlen(string) );
    
            /* Get the MODIS MOD03 file */
            status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
                goto cleanupFail;
            }


            if ( strstr( string, MODIScheck4 ) == NULL )
            {
                FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MODIScheck4);
                goto cleanupFail;
            }
    
            MODISargs[4] = malloc( strlen( string )+1 );
            memset(MODISargs[4],0,strlen(string)+1);

            /* Remember the granule number */
            strncpy( MODISargs[4], string, strlen(string) );
            sprintf(modis_granule_suffix,"%d",modis_count);
            MODISargs[5] = malloc(strlen(granule)+strlen(modis_granule_suffix)+1);
            memset(MODISargs[5],0,strlen(granule)+strlen(modis_granule_suffix)+1);
                
            strncpy(MODISargs[5],granule,strlen(granule));
            strncat(MODISargs[5],modis_granule_suffix,strlen(modis_granule_suffix));

            status = MODIS( MODISargs,modis_count,unpack);
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("MODIS failed data transfer.\nExiting program.\n");
                goto cleanupFail;
            }

        }

        else if(strstr(string,MODIScheck4)!=NULL) {/* No 500m and 250m */

            /*Set 500m and 250m arguments to NULL */
            MODISargs[2] = NULL;
            MODISargs[3] = NULL;
    
            /* Allocate memory */
            MODISargs[4] = malloc( strlen( string )+1 );
            memset(MODISargs[4],0,strlen(string)+1);
            strncpy( MODISargs[4], string, strlen(string) );

            sprintf(modis_granule_suffix,"%d",modis_count);
            MODISargs[5] = malloc(strlen(granule)+strlen(modis_granule_suffix)+1);
            memset(MODISargs[5],0,strlen(granule)+strlen(modis_granule_suffix)+1);
            strncpy(MODISargs[5],granule,strlen(granule));
            strncat(MODISargs[5],modis_granule_suffix,strlen(modis_granule_suffix));

            status = MODIS( MODISargs,modis_count,unpack );
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("MODIS failed data transfer.\nExiting program.\n");
                goto cleanupFail;
            }
        } 

        else {
            FATAL_MSG("MODIS file order is not right. The current file should either be MOD03.. or MOD02HKM.. but it is %s\n", string);
            goto cleanupFail;
        }

        if(MODISargs[1]) free(MODISargs[1]); MODISargs[1] = NULL;
        if(MODISargs[2]) free(MODISargs[2]); MODISargs[2] = NULL;
        if(MODISargs[3]) free(MODISargs[3]); MODISargs[3] = NULL;
        if(MODISargs[4]) free(MODISargs[4]); MODISargs[4] = NULL;
        if(MODISargs[5]) free(MODISargs[5]); MODISargs[5] = NULL;

        /* get the next MODIS 1KM file */
        status = getNextLine( string, inputFile);
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get MODIS 1KM line. Exiting program.\n");
            goto cleanupFail;
        }


        modis_count++;
         
    }

    printf("MODIS done.\nTransferring ASTER..."); fflush(stdout);
    
    /*********
     * ASTER *
     *********/
    
    ASTERargs[0] = argv[0];
    ASTERargs[3] = argv[1];

    /* Get the ASTER input files */
    /* MY 2016-12-20, Need to loop ASTER files since the number of granules may be different for each orbit */
    while(aster_count != 0) {
            
        if(strstr( string, ASTERcheck ) != NULL ){
    
            /* Allocate memory for the argument */
            ASTERargs[1] = malloc( strlen(string )+1);
            memset(ASTERargs[1],0,strlen(string)+1);
            strncpy( ASTERargs[1], string, strlen(string) );
            
            /* Remember granule number */
            sprintf(aster_granule_suffix,"%d",aster_count);
            ASTERargs[2] = malloc(strlen(granule)+strlen(aster_granule_suffix)+1);
            memset(ASTERargs[2],0,strlen(granule)+strlen(aster_granule_suffix)+1);

            strncpy(ASTERargs[2],granule,strlen(granule));
            strncat(ASTERargs[2],aster_granule_suffix,strlen(aster_granule_suffix));
 
            /* EXECUTE ASTER DATA TRANSFER */
            status = ASTER( ASTERargs,aster_count,unpack);
            
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("ASTER failed data transfer.\nExiting program.\n");
                goto cleanupFail;
            }

            aster_count++;
            free(ASTERargs[1]); ASTERargs[1] = NULL;
            free(ASTERargs[2]); ASTERargs[2] = NULL;

            status = getNextLine( string, inputFile );
            if ( status == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to get ASTER line. Exiting program.\n");
                goto cleanupFail;
            }
        }

        // Assume MISR is after ASTER
        else if(strstr(string,MISRcheck1)!=NULL) 
            break;
        else {
            FATAL_MSG("either the ASTER file is wrong or the MISR_GRP file is not right after ASTER file.\n\tThe received line is %s.\n", string);
            goto cleanupFail;
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
        if ( strstr( string, MISRcheck1 ) == NULL )
        {
            FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MISRcheck1);
            goto cleanupFail;
        }

        MISRargs[i] = malloc( strlen( string ) +1 );
        memset(MISRargs[i],0,strlen(string)+1);
        strncpy( MISRargs[i], string, strlen(string) );
        status = getNextLine( string, inputFile );
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get MISR line. Exiting program.\n");
            goto cleanupFail;
        }
    }
    
    if ( strstr( string, MISRcheck2 ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MISRcheck2);
        goto cleanupFail;
    }

    MISRargs[10] = malloc ( strlen( string ) +1);
    memset(MISRargs[10],0,strlen(string)+1);
    strncpy( MISRargs[10], string, strlen(string) );
    status = getNextLine( string, inputFile );
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get MISR line. Exiting program.\n");
        goto cleanupFail;
    }

    if ( strstr( string, MISRcheck3 ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MISRcheck3);
        goto cleanupFail;
    }

    MISRargs[11] = malloc ( strlen( string ) +1);
    memset(MISRargs[11],0,strlen(string)+1);
    strncpy( MISRargs[11], string, strlen(string) );

    status = getNextLine( string, inputFile );
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get MISR line. Exiting program.\n");
        goto cleanupFail;
    }

    if ( strstr( string, MISRcheck4 ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", string, MISRcheck4);
        goto cleanupFail;
    }

    MISRargs[12] = malloc ( strlen( string ) +1);
    memset(MISRargs[12],0,strlen(string)+1);
    strncpy( MISRargs[12], string, strlen(string) );

    // EXECUTE MISR DATA TRANSFER
    status = MISR( MISRargs,unpack);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("MISR failed data transfer.\nExiting program.\n");
        goto cleanupFail;
    }
    printf("MISR done.\n");
    printf("Data transfer successful.\n");

    /* free all memory */

    if ( 0 )
    {
        cleanupFail:
        fail = 1;
    }

    if ( outputFile ) H5Fclose(outputFile);
    if ( inputFile ) fclose(inputFile);
    if ( MOPITTargs[1] ) free(MOPITTargs[1]);
    //if ( CERESargs[1] ) free(CERESargs[1]);
    if ( MODISargs[1] ) free(MODISargs[1]);
    if (  MODISargs[2] ) free( MODISargs[2]);
    if ( MODISargs[3] ) free ( MODISargs[3] );
    if ( MODISargs[4] ) free( MODISargs[4] );
    if ( MODISargs[5] ) free( MODISargs[5] );
    if ( ASTERargs[1] ) free ( ASTERargs[1] );
    if ( ASTERargs[2] ) free ( ASTERargs[2] );
    for ( int j = 1; j <= 12; j++ ) if ( MISRargs[j] ) free (MISRargs[j]);

    if ( fail ) return -1;
    
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
            FATAL_MSG("Unable to get next line.\n");    
            return EXIT_FAILURE;
        }       
    } while ( string[0] == '#' || string[0] == '\n' || string[0] == ' ' );
    
    /* remove the trailing newline or space character from the buffer if it exists */
    size_t len = strlen( string )-1;
    if ( string[len] == '\n' || string[len] == ' ')
        string[len] = '\0';

    return EXIT_SUCCESS;    
}
