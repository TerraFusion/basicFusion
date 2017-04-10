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

    if ( argc != 4 )
    {
        fprintf( stderr, "Usage: %s [outputFile] [inputFiles.txt] [orbit_info.bin]\n", argv[0] );
        fprintf( stderr, "Set environment variable TERRA_DATA_UNPACK to non-zero for unpacking.\n");
        return EXIT_FAILURE;
    }
    

    // open the orbit_info.bin file
    FILE* new_orbit_info_b = fopen(argv[3],"r");
    long fSize;

    // get the size of the file
    fseek(new_orbit_info_b,0,SEEK_END);
    fSize = ftell(new_orbit_info_b);
    // rewind file pointer to beginning of file
    rewind(new_orbit_info_b);


    OInfo_t* test_orbit_ptr = NULL;
    test_orbit_ptr = calloc(fSize/sizeof(OInfo_t),sizeof(OInfo_t));

    // read the file into the test_orbit_ptr struct array
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
    for ( int i = 0; i<fSize/sizeof(OInfo_t);i++) {
        if( test_orbit_ptr[i].orbit_number == current_orbit_number )
        {
            current_orbit_info = test_orbit_ptr[i];
            break;
        }
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
    char* CERESargs[4] = {NULL};
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
    char ceres_granule_suffix[3] = {'\0'};
    

    memset(modis_granule_suffix,0,3);
    memset(aster_granule_suffix,0,3);
    memset(ceres_granule_suffix,0,3);

    /*MY 2016-12-21: MODIS and ASTER have more than one granule for each orbit. We use a counter to stop the loop. */
    int ceres_count = 1;
    int ceres_fm1_count = 1;
    int ceres_fm2_count = 1;
    int ceres_start_index = 0;
    int* ceres_start_index_ptr=&ceres_start_index;
    int ceres_end_index = 0;
    int* ceres_end_index_ptr=&ceres_end_index;
    int ceres_subset_num_elems = 0;
    int32* ceres_subset_num_elems_ptr=NULL;


    
    int modis_count = 1;
    int aster_count = 1;

    /*MY 2016-12-21: GZ also requests to unpack the MODIS,ASTER and MISR data, this is the flag for this */
    int unpack      = 1;

    /*MY 2016-12-21: Currently an environment variable TERRA_DATA_UNPACK should be set 
     *to run the program to generate the unpacked data,
     *without setting this environment variable, the program will just write the packed data */
        
    {
        const char *s;
        s = getenv("TERRA_DATA_PACK");
            
        if(s && isdigit((int)*s)) 
            if((unsigned int)strtol(s,NULL,10) == 1)
                unpack = 0;
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
    
    printf("Transferring MOPITT..."); fflush(stdout); 
    /* get the MOPITT input file paths from our inputFiles.txt */
    status = getNextLine( string, inputFile);

    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get MOPITT line. Exiting program.\n");
        goto cleanupFail;
    }

    int MOPITTgranule = 1;

    // minor: strstr may be called twice in the loop. No need to change. KY 2017-04-10
    do
    {
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

        if ( MOPITT( MOPITTargs, current_orbit_info, &MOPITTgranule ) == EXIT_FAILURE )
        {
            FATAL_MSG("MOPITT failed data transfer.\nExiting program.\n");
            goto cleanupFail;
        }

        free(MOPITTargs[1]); MOPITTargs[1] = NULL;

        status = getNextLine( string, inputFile);

        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get MOPITT line. Exiting program.\n");
            goto cleanupFail;
        }


        /* LTC Apr-7-2017 Note that if the current granule was skipped, MOPITTgranule will be decremented by MOPITT.c
           so that it contains the correct running total for number of granules that have been read.*/
        MOPITTgranule++;

    } while ( strstr( string, MOPITTcheck ) != NULL );


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

#if 0
    status = getNextLine( string, inputFile);
//printf("string is %s\n",string);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get CERES line. Exiting program.\n");
        goto cleanupFail;
    }
#endif

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
       status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
            goto cleanupFail;
       }
#if DEBUG
printf("CERES: the starting index is %d\n",*ceres_start_index_ptr);
printf("CERES: the ending index is %d\n",*ceres_end_index_ptr);
#endif

     if(*ceres_start_index_ptr >=0 && *ceres_end_index_ptr >=0) {
//#if 1
             /* Remember the granule number */
       sprintf(ceres_granule_suffix,"%d",ceres_fm1_count);
       CERESargs[3] = malloc(strlen(granule)+strlen(ceres_granule_suffix)+1);
       memset(CERESargs[3],0,strlen(granule)+strlen(ceres_granule_suffix)+1);
                
       strncpy(CERESargs[3],granule,strlen(granule));
       strncat(CERESargs[3],ceres_granule_suffix,strlen(ceres_granule_suffix));

       ceres_subset_num_elems = (*ceres_end_index_ptr) -(*ceres_start_index_ptr)+1; 
       ceres_subset_num_elems_ptr =&ceres_subset_num_elems;
       status = CERES(CERESargs,1,ceres_fm1_count,(int32*)ceres_start_index_ptr,NULL,ceres_subset_num_elems_ptr);
       //status = CERES_Subset(CERESargs,1,ceres_fm1_count,unpack,ceres_start_index,ceres_end_index);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
            goto cleanupFail;
       }
       ceres_fm1_count++;      
     }
//#endif

    }
    else if ( strstr( string, CEREScheck2 ) != NULL )
    {
       CERESargs[2] = malloc(strlen(string)+1); 
       memset(CERESargs[2],0,strlen(string)+1);
       strncpy( CERESargs[2], string, strlen(string) );
       //FM2
       status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
            goto cleanupFail;
       }

//#if 1
     if(*ceres_start_index_ptr >=0 && *ceres_end_index_ptr >=0) {
       sprintf(ceres_granule_suffix,"%d",ceres_fm2_count);
       CERESargs[3] = malloc(strlen(granule)+strlen(ceres_granule_suffix)+1);
       memset(CERESargs[3],0,strlen(granule)+strlen(ceres_granule_suffix)+1);
                
       strncpy(CERESargs[3],granule,strlen(granule));
       strncat(CERESargs[3],ceres_granule_suffix,strlen(ceres_granule_suffix));

       //status = CERES(CERESargs,2,ceres_fm2_count);
       ceres_subset_num_elems = (*ceres_end_index_ptr) -(*ceres_start_index_ptr)+1; 
       ceres_subset_num_elems_ptr =&ceres_subset_num_elems;
       status = CERES(CERESargs,2,ceres_fm2_count,(int32*)ceres_start_index_ptr,NULL,ceres_subset_num_elems_ptr);
       //status = CERES_Subset(CERESargs,1,ceres_fm1_count,unpack,ceres_start_index,ceres_end_index);
       if ( status == EXIT_FAILURE )
       {
            FATAL_MSG("CERES failed data transfer.\nExiting program.\n");
            goto cleanupFail;
       }
       ceres_fm2_count++;
     }

//#endif

    }
    else {
        FATAL_MSG("CERES files are neither CER_SSF_Terra-FM1 nor CER_SSF_Terra-FM2.\n");
        goto cleanupFail;
    }

    status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get next line. Exiting program.\n");
        goto cleanupFail;
    }

    if(CERESargs[2]) free(CERESargs[2]); CERESargs[2] = NULL;   
    if(CERESargs[3]) free(CERESargs[3]); CERESargs[3] = NULL;   
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
            FATAL_MSG("Failed to get next line. Exiting program.\n");
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
    if ( MODISargs[1] ) free(MODISargs[1]);
    if (  MODISargs[2] ) free( MODISargs[2]);
    if ( MODISargs[3] ) free ( MODISargs[3] );
    if ( MODISargs[4] ) free( MODISargs[4] );
    if ( MODISargs[5] ) free( MODISargs[5] );
    if ( ASTERargs[1] ) free ( ASTERargs[1] );
    if ( ASTERargs[2] ) free ( ASTERargs[2] );
    if ( TAI93toUTCoffset ) free(TAI93toUTCoffset);
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

#if 0
int validateInFiles( char* inFileList, OInfo_t orbitInfo )
{
    short fail = 0;
    const char MOPITTcheck[] = "MOP01";
    const char CEREScheck[] = "CER_SSF_Terra";
    const char MODIScheck1[] = "MOD021KM";
    const char MODIScheck2[] = "MOD02HKM";
    const char MODIScheck3[] = "MOD02QKM";
    const char MODIScheck4[] = "MOD03";
    const char ASTERcheck[] = "AST_L1T_";
    const char MISRcheck1[] = "MISR_AM1_GRP";
    const char MISRcheck2[] = "MISR_AM1_AGP";
    char* MOPITTtime = NULL;
    char* CEREStime = NULL;
    char tempString[STR_LEN];
    int i;
    herr_t status;

    memset(tempString, 0, STR_LEN);

    /* open the input file list */
    FILE* inputFile = fopen( inFileList, "r" );
    char string[STR_LEN];

    if ( inputFile == NULL )
    {
        FATAL_MSG("file \"%s\" does not exist\n", inFileList);
        return EXIT_FAILURE;
    }

    /********************************************
     *                  MOPITT                  *
     ********************************************/

    /* MOPITT file granularity is 24 hours. Thus the granularity of the dates that we need to check
       is a day. Don't need to check any more accurately than a day.
     */
    // MOPITT filename has YYYYMMDD, check using that
    status = getNextLine( string, inputFile);
    if ( status == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to get input file line.\n");
        goto cleanupFail;
    }
    /* strstr will fail if unexpected input is present */
    if ( strstr( string, MOPITTcheck ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for MOPITT.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n",string,MOPITTcheck);
        goto cleanupFail;
    }

    do
    {
        // extract the time from MOPITT file path
        MOPITTtime = getTime( string, 0 );
        if ( MOPITTtime == NULL )
        {
            FATAL_MSG("Failed to extract MOPITT time.\n");
            goto cleanupFail;
        }        

        // If the MOPITT date is same as orbit date, then file is valid
        // extract year
        for ( i = 0; i < 4; i++ )
        {
            tempString[i] = MOPITTtime[i];
        }
        int MOPITTyear = atoi(tempString);
        if ( MOPITTyear == 0 )
        {
            FATAL_MSG("failed to extract MOPITT year.\n");
            goto cleanupFail;
        }

        // extract month
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 2; i++ )
        {
            tempString[i] = MOPITTtime[i+4];
        }
        int MOPITTmonth = atoi(tempString);
        if ( MOPITTmonth == 0 )
        {
            FATAL_MSG("failed to extract MOPITT month.\n");
            goto cleanupFail;
        }

        // extract day
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 2; i++ )
        {
            tempString[i] = MOPITTtime[i+6];
        }
        int MOPITTday = atoi(tempString);
        
        if ( MOPITTday == 0 )
        {
            FATAL_MSG("failed to extract MOPITT day.\n");
            goto cleanupFail;
        }

        free(MOPITTtime); MOPITTtime = 0;

        // check if the orbit start or orbit end times are within the MOPITT granule
        if ( !((orbitInfo.start_year == MOPITTyear && orbitInfo.start_month == MOPITTmonth && orbitInfo.start_day == MOPITTday ) ||
             (orbitInfo.end_year == MOPITTyear && orbitInfo.end_month == MOPITTmonth && orbitInfo.end_day == MOPITTday ) ) )
        {
            FATAL_MSG("MOPITT file does not reside within the given orbit.\n\tOffending file:%s\n\tOrbit start: %d/%d/%d\n\tOrbit end: %d/%d/%d\n\tDates given in YYYY/MM/DD\n", string, orbitInfo.start_year, orbitInfo.start_month, orbitInfo.start_day,orbitInfo.end_year, orbitInfo.end_month, orbitInfo.end_day);
            goto cleanupFail;
        }

        status = getNextLine( string, inputFile);
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get input file line. Exiting program.\n");
            goto cleanupFail;
        }

    } while ( strstr( string, MOPITTcheck ) != NULL );


    /********************************************
     *                  CERES                   *
     ********************************************/

    /* CERES file time accuracy is down to the hour, so bounds checking only needs to be accurate up to hour */

    if ( strstr( string, CEREScheck ) == NULL )
    {
        FATAL_MSG("Received an unexpected input line for CERES.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n",string,CEREScheck);
        goto cleanupFail;
    }

    do
    {
        // extract the time from MOPITT file path
        CEREStime = getTime( string, 1 );
        if ( CEREStime == NULL )
        {
            FATAL_MSG("Failed to extract MOPITT time.\n");
            goto cleanupFail;
        }

        // If the CERES date is same as orbit date, then file is valid
        // extract year
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 4; i++ )
        {
            tempString[i] = CEREStime[i+7];
        }
        int CERESyear = atoi(tempString);
        if ( CERESyear == 0 )
        {
            FATAL_MSG("failed to extract CERES year.\n");
            goto cleanupFail;
        }

        // extract month
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 2; i++ )
        {
            tempString[i] = CEREStime[i+11];
        }
        int CERESmonth = atoi(tempString);
        if ( CERESmonth == 0 )
        {
            FATAL_MSG("failed to extract CERES month.\n");
            goto cleanupFail;
        }

        // extract day
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 2; i++ )
        {
            tempString[i] = CEREStime[i+13];
        }
        int CERESday = atoi(tempString);

        if ( CERESday == 0 )
        {
            FATAL_MSG("failed to extract CERES day.\n");
            goto cleanupFail;
        }

        // extract hour
        memset(tempString, 0, STR_LEN);
        for ( i = 0; i < 2; i++ )
        {
            tempString[i] = CEREStime[i+15];
        }
        int CEREShour = atoi(tempString);
    
        if ( CEREShour == 0 )
        {
            FATAL_MSG("failed to extract CERES hour.\n");
            goto cleanupFail;
        }


        // TODO create function that increments the times
        free(CEREStime); CEREStime = 0;
        int CERESendYear = CERESyear;
        int CERESendMonth = CERESmonth;
        int CERESendDay = CERESday;
        int CERESendHour = CEREShour;

        // CERES SSF file has granularity of 1 hour. Increment the start time by an hour.
        CERESendHour++;

        //if ( )

        // check if the orbit start or orbit end times are within the CERES granule
        if ( ! ((CERESyear >= orbitInfo.start_year && CERESyear <= orbitInfo.end_year &&
             CERESmonth >= orbitInfo.start_month && CERESmonth <= orbitInfo.end_month &&
             CERESday >= orbitInfo.start_day && CERESday <= orbitInfo.end_day &&
             CEREShour >= orbitInfo.start_hour && CEREShour <= orbitInfo.end_hour) 
             ||
             (CERESyear >= orbitInfo.start_hour) ))
        {
            FATAL_MSG("CERES file does not reside within the given orbit.\n\tOffending file:%s\n\tOrbit start: %d/%d/%d\n\tOrbit end: %d/%d/%d\n\tDates given in YYYY/MM/DD\n", string, orbitInfo.start_year, orbitInfo.start_month, orbitInfo.start_day,orbitInfo.end_year, orbitInfo.end_month, orbitInfo.end_day);
            goto cleanupFail;
        }

        status = getNextLine( string, inputFile);
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to get input file line. Exiting program.\n");
            goto cleanupFail;
        }


    } while ( strstr( string, CEREScheck ) != NULL );

    




    /********************************************
     *                  MODIS                   *
     ********************************************/

    /* MODIS files have starting accuracy up to the minute */
    

    if ( 0 )
    {
        cleanupFail:
        fail = 1;
    }

    if (MOPITTtime) free(MOPITTtime);
    if (CEREStime) free(CEREStime);

    if ( fail ) 
        return EXIT_FAILURE;
    else
        return EXIT_SUCCESS;
}
#endif
