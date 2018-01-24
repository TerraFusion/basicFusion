#include "libTERRA.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <curses.h>
#include <time.h>
#define STR_LEN 500
#define LOGIN_NODE "login"
#define MOM_NODE

int getNextLine ( char* string, FILE* const inputFile );
int Add_CF_Provenance_Attrs();
hid_t outputFile;

int main( int argc, char* argv[] )
{

    /* Various arguments to each instrument function */
    char* CERESargs[4] = {NULL};
    char* MODISargs[7] = {NULL};
    char* ASTERargs[4] = {NULL};
    char* MISRargs[13] = {NULL};

    char* granuleList = NULL;
    size_t granListSize = 0;
    char* granTempPtr = NULL;
    int status = RET_SUCCESS;
    int fail = 0;
    int CERESgranuleNum = 0;
    herr_t errStatus;

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
    char* CER_curTime = NULL;
    char* CER_prevTime = NULL;

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
    FILE* new_orbit_info_b = NULL;
    OInfo_t current_orbit_info;
    OInfo_t* test_orbit_ptr = NULL;

    FILE* inputFile = NULL;
    char inputLine[STR_LEN];
    time_t sTime;
    time_t eTime;

    /*MY 2016-12-21: GZ also requests to unpack the MODIS,ASTER and MISR data, this is the flag for this */
    int unpack      = 1;
    
    /* LTC Jun 21, 2017: Add messages for when USE_GZIP and USE_CHUNK environment variables are set */
    int useGZIP = 0;
    int useChunk = 0;

    if ( argc != 4 )
    {
        fprintf( stderr, "Usage: %s [outputFile] [inputFiles.txt] [orbit_info.bin]\n", argv[0] );
        fprintf( stderr, "Set environment variable TERRA_DATA_PACK to non-zero to retain packed data.\n");
        fprintf( stderr, "Set environment variable USE_GZIP from 1 to 9 to set HDF compression level.\n");
        fprintf( stderr, "Set environment variable USE_CHUNK to enable HDF dataset chunking.\n");
        goto cleanupFail;
    }

    /* Get the starting execution Unix time */
    sTime = time(NULL);    

    /* open the input file list */
    inputFile = fopen( argv[2], "r" );

    if ( inputFile == NULL )
    {
        FATAL_MSG("file \"%s\" does not exist. Exiting program.\n", argv[2]);
        goto cleanupFail;
    }

    // open the orbit_info.bin file
    new_orbit_info_b = fopen(argv[3],"r");
    if(new_orbit_info_b == NULL) ｛
        FATAL_MSG("file \"%s\" does not exist. Exiting program.\n", argv[3]);
        goto cleanupFail;
    ｝
    long fSize = 0;

    // get the size of the file
    if(fseek(new_orbit_info_b,0,SEEK_END)!=0) {
        FATAL_MSG("file \"%s\" fseek function fails. Exiting program.\n", argv[3]);
        goto cleanupFail;
    }
    fSize = ftell(new_orbit_info_b);
    if(fSize <0) {
        FATAL_MSG("file \"%s\" ftell function fails. Exiting program.\n", argv[3]);
        goto cleanupFail;
    }
    // rewind file pointer to beginning of file
    rewind(new_orbit_info_b);


    test_orbit_ptr = calloc(fSize/sizeof(OInfo_t),sizeof(OInfo_t));

    // read the file into the test_orbit_ptr struct array
    size_t result = fread(test_orbit_ptr,sizeof(OInfo_t),fSize/sizeof(OInfo_t),new_orbit_info_b);
    if(result != fSize/sizeof(OInfo_t))
    {
        FATAL_MSG("fread is not successful.\n");
        goto cleanupFail;
    }

    /* Get the orbit number from inputFiles.txt */
    status = getNextLine( inputLine, inputFile );
    if ( status == FATAL_ERR )
    {
        FATAL_MSG("Failed to get the next line.\n");
        goto cleanupFail;
    }
    if ( !isdigit(*inputLine) )
    {
        FATAL_MSG("The first line in the %s file must contain the orbit number.\n", argv[2]);
        goto cleanupFail;
    }

    int current_orbit_number = atoi(inputLine);
    for ( int i = 0; i<fSize/sizeof(OInfo_t); i++)
    {
        if( test_orbit_ptr[i].orbit_number == current_orbit_number )
        {
            current_orbit_info = test_orbit_ptr[i];
            break;
        }
    }

    if(fclose(new_orbit_info_b)!=0) {
        FATAL_MSG("Failed to close file %s\n",argv[3]);
        goto cleanupFail;
    }
    new_orbit_info_b = NULL;
    free(test_orbit_ptr);
    test_orbit_ptr = NULL;



    /*MY 2016-12-21: Currently an environment variable TERRA_DATA_UNPACK should be set
     *to run the program to generate the unpacked data,
     *without setting this environment variable, the program will just write the packed data */

    {
        const char *s;
        s = getenv("TERRA_DATA_PACK");

        int envVal;

        if(s && isdigit((int)*s)) 
        {
            envVal = strtol(s, NULL, 10);
            unpack = envVal;
        }
        else
            unpack = 0;

        s = getenv("USE_CHUNK");
        
        if ( s && isdigit((int)*s)) 
        {   envVal = strtol(s, NULL, 10);
            useChunk = envVal;
        }
        else
            useChunk = 0;

        s = getenv("USE_GZIP");
        if ( s && isdigit((int)*s)) 
        {   envVal = strtol(s, NULL, 10);
            useGZIP = envVal;
        }
        else
            useGZIP = 0;

        // USE_GZIP should always be between 0 through 9. Make sure this is the case.
        if ( useGZIP < 0 || useGZIP > 9 )
        {
            FATAL_MSG("The environment variable USE_GZIP should be between 0 and 9 inclusive.\n\tIts current value is %d.\n", useGZIP);
            goto cleanupFail;
        }

    }

    if ( unpack ) printf("\n_____UNPACKING ENABLED_____\n");
    else printf("\n_____UNPACKING DISABLED_____\n");
    if ( useChunk ) printf("_____CHUNKING ENABLED_____\n");
    else printf("\n_____CHUNKING DISABLED_____\n");
    if ( useGZIP ) printf("\n_____GZIP ENABLED -- LEVEL %d_____\n", useGZIP );
    else printf("\n_____GZIP DISABLED_____\n");

    /* remove output file if it already exists. Note that no conditional statements are used. If file does not exist,
     * this function will throw an error but we do not care.
     */
    /* MY 2017-01-27: remove is not necessary. The HDF5 file create operation will handle this
                      Correction: leave it for the time being since the createOutputFile uses the EXCL flag.
                      TODO: Will turn this off in the operation.
    */
    /* Landon Aug 18 2017: I want to keep this remove here because it will ensure HDF doesn't try to open an existing file.
     * remove() provides hardly any performance decrease. It's just a safety measure.
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

    printf("Transferring MOPITT...");
    fflush(stdout);

    /* get the MOPITT input file paths from our inputFiles.txt */
    status = getNextLine( inputLine, inputFile);

    if ( status == FATAL_ERR )
    {
        FATAL_MSG("Failed to get MOPITT line. Exiting program.\n");
        goto cleanupFail;
    }


    /* strstr returns non-null if the strings match */
    if ( strstr(inputLine, "MOP N/A" ) == NULL )
    { 
        // minor: strstr may be called twice in the loop. No need to change. KY 2017-04-10
        do
        {
            /* strstr will fail if unexpected input is present */
            if ( strstr( inputLine, MOPITTcheck ) == NULL )
            {
                FATAL_MSG("Received an unexpected input line for MOPITT.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n",inputLine,MOPITTcheck);
                goto cleanupFail;
            }

            status = MOPITT( inputLine, current_orbit_info );
            if(status == FATAL_ERR )
            {
                FATAL_MSG("MOPITT failed data transfer on file:\n\t%s\nExiting program.\n", inputLine);
                goto cleanupFail;
            }

            if(status != RET_SUCCESS_NO_PROCESS) {

                /* strrchr finds last occurance of character in a string */
                granTempPtr = strrchr( inputLine, '/' );
                if ( granTempPtr == NULL )
                {
                    FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                    goto cleanupFail;
                }
                errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                if ( errStatus == FATAL_ERR )
                {
                    FATAL_MSG("Failed to update the granule list.\n");
                    goto cleanupFail;
                }
            }

#if 0
            if ( MOPITT( inputLine, current_orbit_info ) == FATAL_ERR )
            {
                FATAL_MSG("MOPITT failed data transfer on file:\n\t%s\nExiting program.\n", inputLine);
                goto cleanupFail;
            }
#endif

            status = getNextLine( inputLine, inputFile);
            if ( status == FATAL_ERR )
            {
                FATAL_MSG("Failed to get MOPITT line. Exiting program.\n");
                goto cleanupFail;
            }

        } while ( strstr( inputLine, MOPITTcheck ) != NULL );

        printf("MOPITT done.\nTransferring CERES...");
        fflush(stdout);
    }
    else
    {
        printf("No MOPITT files found.\nTransferring CERES...");
        fflush(stdout);
        status = getNextLine( inputLine, inputFile );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get next line. Exiting program.\n");
            goto cleanupFail;
        }
    }

    /*********
     * CERES *
     *********/
    /* Get the CERES  */

    CERESargs[0] = argv[0];
    CERESargs[1] = argv[1];

    if ( strstr(inputLine, "CER N/A" ) == NULL )
    {
        while(ceres_count != 0)
        {
            
            if(strstr(inputLine,MODIScheck1) != NULL || strstr(inputLine, "MOD N/A") != NULL )
            {
                ceres_count = 0;
                continue;
            }
            /*  The granule number is incremented when the date between two files differs */
            CER_curTime = getTime( inputLine, 1 );
            if ( CER_curTime == NULL )
            {
                FATAL_MSG("Failed to fetch the time substring in CERES file.\n\t%s\n",inputLine);
                goto cleanupFail;
            }

            long int curTime = strtol(CER_curTime, NULL, 10);
            long int prevTime = 0;
            if ( CER_prevTime != NULL )
                prevTime = strtol(CER_prevTime, NULL, 10);

            if ( curTime != prevTime )
                CERESgranuleNum++; 

            /*_______FM1________*/

            if ( strstr( inputLine, CEREScheck1 ) != NULL )
            {
                CERESargs[2] = calloc(strlen(inputLine)+1, 1);
                
                strncpy( CERESargs[2], inputLine, strlen(inputLine) );
                status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
                    goto cleanupFail;
                }

                if(*ceres_start_index_ptr >=0 && *ceres_end_index_ptr >=0)
                {

                    ceres_subset_num_elems = (*ceres_end_index_ptr) -(*ceres_start_index_ptr)+1;
                    ceres_subset_num_elems_ptr =&ceres_subset_num_elems;
                    

                    granTempPtr = strrchr( inputLine, '/' );
                    if ( granTempPtr == NULL )
                    {
                        FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                        goto cleanupFail;
                    }
                    errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                    if ( errStatus == FATAL_ERR )
                    {
                        FATAL_MSG("Failed to update the granule list.\n");
                        goto cleanupFail;
                    }
                    status = CERES(CERESargs,1,ceres_fm1_count,(int32*)ceres_start_index_ptr,NULL,ceres_subset_num_elems_ptr);
                    if ( status == FATAL_ERR )
                    {
                        FATAL_MSG("CERES failed data transfer on file:\n\t%s\nExiting program.\n", inputLine);
                        goto cleanupFail;
                    }
                    ceres_fm1_count++;
                }

            }

            /*______FM2______*/
            else if ( strstr( inputLine, CEREScheck2 ) != NULL )
            {
                CERESargs[2] = calloc(strlen(inputLine)+1, 1);
                strncpy( CERESargs[2], inputLine, strlen(inputLine) );
                
                status = CERES_OrbitInfo(CERESargs,ceres_start_index_ptr,ceres_end_index_ptr,current_orbit_info);
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("CERES failed to obtain orbit info.\nExiting program.\n");
                    goto cleanupFail;
                }

                if(*ceres_start_index_ptr >=0 && *ceres_end_index_ptr >=0)
                {
                    ceres_subset_num_elems = (*ceres_end_index_ptr) - (*ceres_start_index_ptr)+1;
                    ceres_subset_num_elems_ptr =&ceres_subset_num_elems;
                    
                    granTempPtr = strrchr( inputLine, '/' );
                    if ( granTempPtr == NULL )
                    {
                        FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                        goto cleanupFail;
                    }
                    errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                    if ( errStatus == FATAL_ERR )
                    {
                        FATAL_MSG("Failed to update the granule list.\n");
                        goto cleanupFail;
                    }
                    status = CERES(CERESargs,2,ceres_fm2_count,(int32*)ceres_start_index_ptr,NULL,ceres_subset_num_elems_ptr);
                    if ( status == FATAL_ERR )
                    {
                        FATAL_MSG("CERES failed data transfer on file:\n\t%s\nExiting program.\n", inputLine);
                        goto cleanupFail;
                    }
                    ceres_fm2_count++;
                }
            }
            else
            {
                FATAL_MSG("CERES files are neither CER_SSF_Terra-FM1 nor CER_SSF_Terra-FM2.\n");
                goto cleanupFail;
            }

            status = getNextLine( inputLine, inputFile);
            if ( status == FATAL_ERR )
            {
                FATAL_MSG("Failed to get next line. Exiting program.\n");
                goto cleanupFail;
            }

            if(CERESargs[2]) free(CERESargs[2]);
            CERESargs[2] = NULL;
            if(CERESargs[3]) free(CERESargs[3]);
            CERESargs[3] = NULL;
            ceres_count++;

            if ( CER_prevTime ) free(CER_prevTime);
            
            CER_prevTime = CER_curTime;
            CER_curTime = NULL;
        }
        // No longer need the CERES file times
        free(CER_prevTime); CER_prevTime = NULL;

        printf("CERES done.\nTransferring MODIS...");
        fflush(stdout);
    }
    else
    {
        printf("No CERES files found.\nTransferring MODIS...");
        fflush(stdout);
        status = getNextLine( inputLine, inputFile);
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get next line. Exiting program.\n");
            goto cleanupFail;
        }    
    }

    /*********
     * MODIS *
     *********/


    MODISargs[0] = argv[0];
    MODISargs[6] = argv[1];

    /* MY 2016-12-21: Need to form a loop to read various number of MODIS files
    *  A pre-processing check of the inputFile should be provided to make sure the processing
    *  will not exit prematurely */
    if ( strstr(inputLine, "MOD N/A" ) == NULL )
    {
        while(modis_count != 0)
        {

            /* Need to check more, now just assume ASTER is after */
            if(strstr(inputLine,ASTERcheck) || strstr(inputLine,"AST N/A") )
            {
                modis_count = 0;
                continue;
            }

            /* Should be 1 km */
            if ( strstr( inputLine, MODIScheck1 ) == NULL )
            {
                FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MODIScheck1);
                goto cleanupFail;
            }

            granTempPtr = strrchr( inputLine, '/' );
            if ( granTempPtr == NULL )
            {
                FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                goto cleanupFail;
            }
            errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
            if ( errStatus == FATAL_ERR )
            {
                FATAL_MSG("Failed to update the granule list.\n");
                goto cleanupFail;
            }

            /* Allocate memory for the argument */
            MODISargs[1] = malloc ( strlen(inputLine )+1 );
            memset(MODISargs[1],0,strlen(inputLine)+1);
            strncpy( MODISargs[1], inputLine, strlen(inputLine));

            status = getNextLine( inputLine, inputFile );
            if ( status == FATAL_ERR )
            {
                FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
                goto cleanupFail;
            }
            
            
            granTempPtr = strrchr( inputLine, '/' );
            if ( granTempPtr == NULL )
            {
                FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                goto cleanupFail;
            }
            errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
            if ( errStatus == FATAL_ERR )
            {
                FATAL_MSG("Failed to update the granule list.\n");
                goto cleanupFail;
            }

            /*This may be MOD02HKM or MOD03, so need to check */
            if(strstr(inputLine,MODIScheck2) !=NULL)  /*Has MOD02HKM*/
            {

                /* Allocate memory */
                MODISargs[2] = malloc( strlen(inputLine)+1 );
                memset(MODISargs[2],0,strlen(inputLine)+1);
                strncpy( MODISargs[2], inputLine, strlen(inputLine));

                /* Get the MODIS 250m file */
                status = getNextLine( inputLine, inputFile );
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
                    goto cleanupFail;
                }

               
                /* Find the last occurance of the slash character so we can grab the filename from inputLine */ 
                granTempPtr = strrchr( inputLine, '/' );
                if ( granTempPtr == NULL )
                {
                    FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                    goto cleanupFail;
                }
                errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                if ( errStatus == FATAL_ERR )
                {
                    FATAL_MSG("Failed to update the granule list.\n");
                    goto cleanupFail;
                }

                if ( strstr( inputLine, MODIScheck3 ) == NULL )
                {
                    FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MODIScheck3);
                    goto cleanupFail;
                }

                MODISargs[3] = calloc( strlen(inputLine)+1 , 1);
                strncpy( MODISargs[3], inputLine, strlen(inputLine) );

                /* Get the MODIS MOD03 file */
                status = getNextLine( inputLine, inputFile );
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("Failed to get MODIS line. Exiting program.\n");
                    goto cleanupFail;
                }

                granTempPtr = strrchr( inputLine, '/' );
                if ( granTempPtr == NULL )
                {
                    FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                    goto cleanupFail;
                }
                errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                if ( errStatus == FATAL_ERR )
                {
                    FATAL_MSG("Failed to update the granule list.\n");
                    goto cleanupFail;
                }

                if ( strstr( inputLine, MODIScheck4 ) == NULL )
                {
                    FATAL_MSG("Received an unexpected input line for MODIS.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MODIScheck4);
                    goto cleanupFail;
                }

                MODISargs[4] = malloc( strlen( inputLine )+1 );
                memset(MODISargs[4],0,strlen(inputLine)+1);

                /* Remember the granule number */
                strncpy( MODISargs[4], inputLine, strlen(inputLine) );
                sprintf(modis_granule_suffix,"%d",modis_count);

                status = MODIS( MODISargs,modis_count,unpack);
                if ( status == FATAL_ERR )
                {    
                    FATAL_MSG("MODIS failed data transfer on this granule:\n)");
                    for ( int j = 1; j < 5; j++ )
                    {    
                        if ( MODISargs[j] )
                            fprintf( stderr, "\t%s\n", MODISargs[j]);
                    }
                    printf("Exiting program.\n");
                    goto cleanupFail;
                }

            }

            else if(strstr(inputLine,MODIScheck4)!=NULL)  /* No 500m and 250m */
            {

                /*Set 500m and 250m arguments to NULL */
                MODISargs[2] = NULL;
                MODISargs[3] = NULL;

                /* Allocate memory */
                MODISargs[4] = calloc( strlen( inputLine )+1, 1);
                strncpy( MODISargs[4], inputLine, strlen(inputLine) );

                sprintf(modis_granule_suffix,"%d",modis_count);
                MODISargs[5] = calloc(strlen(granule)+strlen(modis_granule_suffix)+1, 1);
                
                strncpy(MODISargs[5],granule,strlen(granule));
                strncat(MODISargs[5],modis_granule_suffix,strlen(modis_granule_suffix));

                status = MODIS( MODISargs,modis_count,unpack );
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("MODIS failed data transfer on this granule:\n)");
                    for ( int j = 1; j < 5; j++ )
                    {    
                        if ( MODISargs[j] )
                            printf("\t%s\n", MODISargs[j]);
                    }
                    printf("Exiting program.\n");
                    goto cleanupFail;
                }
            }

            else
            {
                FATAL_MSG("MODIS file order is not right. The current file should either be MOD03.. or MOD02HKM.. but it is %s\n", inputLine);
                goto cleanupFail;
            }

            if(MODISargs[1]) free(MODISargs[1]);
            MODISargs[1] = NULL;
            if(MODISargs[2]) free(MODISargs[2]);
            MODISargs[2] = NULL;
            if(MODISargs[3]) free(MODISargs[3]);
            MODISargs[3] = NULL;
            if(MODISargs[4]) free(MODISargs[4]);
            MODISargs[4] = NULL;
            if(MODISargs[5]) free(MODISargs[5]);
            MODISargs[5] = NULL;

            /* get the next MODIS 1KM file */
            status = getNextLine( inputLine, inputFile);
            if ( status == FATAL_ERR )
            {
                FATAL_MSG("Failed to get next line. Exiting program.\n");
                goto cleanupFail;
            }

            modis_count++;

        }

        printf("MODIS done.\nTransferring ASTER...");
        fflush(stdout);
    }
    else
    {
        printf("No MODIS files found.\nTransferring ASTER...");
        fflush(stdout);
        status = getNextLine( inputLine, inputFile);
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get next line. Exiting program.\n");
            goto cleanupFail;
        }

    }
    /*********
     * ASTER *
     *********/

    ASTERargs[0] = argv[0];
    ASTERargs[3] = argv[1];

    /* Get the ASTER input files */
    /* MY 2016-12-20, Need to loop ASTER files since the number of granules may be different for each orbit */
    if ( strstr(inputLine, "AST N/A" ) == NULL )
    {
        while(aster_count != 0)
        {

            if(strstr( inputLine, ASTERcheck ) != NULL )
            {

                granTempPtr = strrchr( inputLine, '/' );
                if ( granTempPtr == NULL )
                {
                    FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                    goto cleanupFail;
                }
                errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
                if ( errStatus == FATAL_ERR )
                {
                    FATAL_MSG("Failed to update the granule list.\n");
                    goto cleanupFail;
                }
                /* Allocate memory for the argument */
                ASTERargs[1] = calloc( strlen(inputLine )+1, 1);
                strncpy( ASTERargs[1], inputLine, strlen(inputLine) );

                /* EXECUTE ASTER DATA TRANSFER */
                status = ASTER( ASTERargs,aster_count,unpack);

                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("ASTER failed data transfer on file:\n\t%s\nExiting program.\n", inputLine);
                    goto cleanupFail;
                }

                aster_count++;
                free(ASTERargs[1]);
                ASTERargs[1] = NULL;
                free(ASTERargs[2]);
                ASTERargs[2] = NULL;

                status = getNextLine( inputLine, inputFile );
                if ( status == FATAL_ERR )
                {
                    FATAL_MSG("Failed to get ASTER line. Exiting program.\n");
                    goto cleanupFail;
                }
            }

            // Assume MISR is after ASTER
            else 
                break;
        }

        printf("ASTER done.\nTransferring MISR...");
        fflush(stdout);
    }
    else
    {
        printf("No ASTER files found.\nTransferring MISR...");
        fflush(stdout);
        status = getNextLine( inputLine, inputFile);
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get next line. Exiting program.\n");
            goto cleanupFail;
        }

    }

    /********
     * MISR *
     ********/
    MISRargs[0] = argv[0];

    if ( strstr(inputLine, "MIS N/A" ) == NULL )
    {
        

        for ( int i = 1; i < 10; i++ )
        {

            if(strstr(inputLine,"MISR_AM1_GRP_MISS")==NULL) {
            
            granTempPtr = strrchr( inputLine, '/' );
            if ( granTempPtr == NULL )
            {
                FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
                goto cleanupFail;
            }
            errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
            
            if ( errStatus == FATAL_ERR )
            {
                FATAL_MSG("Failed to update the granule list.\n");
                goto cleanupFail;
            }
            }
            /* get the next MISR input file */
            if ( strstr( inputLine, MISRcheck1 ) == NULL )
            {
                FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MISRcheck1);
                goto cleanupFail;
            }

            MISRargs[i] = calloc( strlen( inputLine ) +1, 1 );
            strncpy( MISRargs[i], inputLine, strlen(inputLine) );
            status = getNextLine( inputLine, inputFile );

            if ( status == FATAL_ERR )
            {
                FATAL_MSG("Failed to get MISR line. Exiting program.\n");
                goto cleanupFail;
            }

        }
 
        granTempPtr = strrchr( inputLine, '/' );
        if ( granTempPtr == NULL )
        {
            FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
            goto cleanupFail;
        }
        errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
        if ( errStatus == FATAL_ERR )
        {
            FATAL_MSG("Failed to update the granule list.\n");
            goto cleanupFail;
        }

        if ( strstr( inputLine, MISRcheck2 ) == NULL )
        {
            FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MISRcheck2);
            goto cleanupFail;
        }

        MISRargs[10] = calloc ( strlen( inputLine ) +1, 1);
        strncpy( MISRargs[10], inputLine, strlen(inputLine) );
        status = getNextLine( inputLine, inputFile );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get MISR line. Exiting program.\n");
            goto cleanupFail;
        }

        if ( strstr( inputLine, MISRcheck3 ) == NULL )
        {
            FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MISRcheck3);
            goto cleanupFail;
        }

        if(strstr(inputLine,"MISR_AM1_GP_MISS") ==NULL) {
        granTempPtr = strrchr( inputLine, '/' );
        if ( granTempPtr == NULL )
        {
            FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
            goto cleanupFail;
        }
        errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
        if ( errStatus == FATAL_ERR )
        {
            FATAL_MSG("Failed to update the granule list.\n");
            goto cleanupFail;
        }
        }

        MISRargs[11] = malloc ( strlen( inputLine ) +1);
        memset(MISRargs[11],0,strlen(inputLine)+1);
        strncpy( MISRargs[11], inputLine, strlen(inputLine) );

        status = getNextLine( inputLine, inputFile );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get MISR line. Exiting program.\n");
            goto cleanupFail;
        }

        if ( strstr( inputLine, MISRcheck4 ) == NULL )
        {
            FATAL_MSG("Received an unexpected input line for MISR.\n\tReceived string:\n\t%s\n\tExpected to receive string containing a substring of: %s\nExiting program.\n", inputLine, MISRcheck4);
            goto cleanupFail;
        }


        granTempPtr = strrchr( inputLine, '/' );
        if ( granTempPtr == NULL )
        {
            FATAL_MSG("Failed to find the last occurance of slash character in the input line.\n");
            goto cleanupFail;
        }
        errStatus = updateGranList( &granuleList, granTempPtr + 1, &granListSize );
        if ( errStatus == FATAL_ERR )
        {
            FATAL_MSG("Failed to update the granule list.\n");
            goto cleanupFail;
        }

        MISRargs[12] = malloc ( strlen( inputLine ) +1);
        memset(MISRargs[12],0,strlen(inputLine)+1);
        strncpy( MISRargs[12], inputLine, strlen(inputLine) );

        // EXECUTE MISR DATA TRANSFER
        status = MISR( MISRargs,unpack);
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("MISR failed data transfer.\nExiting program.\n");
            goto cleanupFail;
        }
        printf("MISR done.\n");
    }
    else
        printf("No MISR files found.\n");

    // Add some CF Provenance attributes
    errStatus = Add_CF_Provenance_Attrs();
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to add CF provenance attributes in root group.\n");
        goto cleanupFail;
    }
    
    /* Attach the granuleList as an attribute to the root HDF5 object */
    // Sometimes there are no any granules for this orbit. Now I just record as a string "None".
    if(granuleList == NULL) {
       granuleList= malloc(5);
       char* temp_granuleList="None";
       strncpy(granuleList,temp_granuleList,4);
       granuleList[4]='\0';

    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "InputGranules", granuleList);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set Input Granules attribute in root group.\n");
        goto cleanupFail;
    }
    

    printf("Data transfer successful.\n");

    /* free all memory */

    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( outputFile ) H5Fclose(outputFile);
    if ( inputFile ) fclose(inputFile);
    if ( MODISargs[1] ) free(MODISargs[1]);
    if (  MODISargs[2] ) free( MODISargs[2]);
    if ( MODISargs[3] ) free ( MODISargs[3] );
    if ( MODISargs[4] ) free( MODISargs[4] );
    if ( MODISargs[5] ) free( MODISargs[5] );
    if ( ASTERargs[1] ) free ( ASTERargs[1] );
    if ( ASTERargs[2] ) free ( ASTERargs[2] );
    if ( TAI93toUTCoffset ) free(TAI93toUTCoffset);
    if ( test_orbit_ptr) free(test_orbit_ptr);
    if ( new_orbit_info_b) fclose(new_orbit_info_b);
    if ( CER_curTime ) free( CER_curTime );
    if ( CER_prevTime ) free(CER_prevTime);
    for ( int j = 1; j <= 12; j++ )
        if ( MISRargs[j] ) free (MISRargs[j]);
    if ( granuleList ) free(granuleList);

    eTime = time(NULL);
    /* Print the program execution time */
    time_t runTime = eTime - sTime;
    printf("Running time: %ld seconds\n", runTime);

    if ( fail ) return -1;

    return 0;
}

/*  getNextLine

 DESCRIPTION:
    This function grabs the next valid file path in inputFile. It skips any line that starts with '#', '\n', or ' '.
    The result is stored in the string argument. It is the duty of the caller to ensure that string argument has
    enough space to hold any string in the inputFile. If the string is larger than the macro STR_LEN, the string
    will be cut off.

 ARGUMENTS:
    char* string            -- The pointer to a string array where the resultant string will be stored.
    FILE* const inputFile   -- The file to fetch the next string from.

 EFFECTS:
    Modifies the memory pointed to by string.

 RETURN:
    FATAL_ERR
    RET_SUCCESS
*/

int getNextLine ( char* string, FILE* const inputFile )
{

    do
    {
        if ( fgets( string, STR_LEN, inputFile ) == NULL )
        {
            if ( feof(inputFile) != 0 )
                return -1;
            FATAL_MSG("Unable to get next line.\n");
            return FATAL_ERR;
        }
    } while ( string[0] == '#' || string[0] == '\n' || string[0] == ' ' );

    /* remove the trailing newline or space character from the buffer if it exists */
    size_t len = strlen( string )-1;
    if ( string[len] == '\n' || string[len] == ' ')
        string[len] = '\0';

    return RET_SUCCESS;
}

int Add_CF_Provenance_Attrs() {

    int errStatus = RET_SUCCESS;
    char *tf_acknowledge = "This data set was created with funding from NASA ACCESS Grant #NNX16AM07A.";
    char *tf_version = "First version of this product";
    char *tf_contri_name ="Muqun Yang, Landon Clipp, Yizhao Gao, Guangyu Zhao, Larry Di Girolamo"; 
    char *tf_contri_email ="myang6@hdfgroup.org, gzhao1@illinois.edu";
    char *tf_inst ="The HDF Group, University of Illinois";
    char *tf_kw = "Earth Science, TERRA, MISR, MODIS, ASTER,CERES,MOPITT, Radiance";
    char *tf_lic = "No constraints on data access or use";
    char *tf_plat= "TERRA";
    char *tf_Pro_Level = "Level 1";
    char *tf_Pro_Version = "V001";
    char *tf_project  = "NASA ACCESS to Terra Fusion Product";
    char *tf_sensors ="MODIS,MISR,ASTER,CERES,MOPITT";
    char *tf_summary ="A Terra level-1 radiance basic fusion product that combines calibrated radiance measurements from MISR, MODIS, ASTER, CERES and MOPITT";
    char *tf_title = "TERRA L1B Radiance Basic Fusion Product"; 
    errStatus = H5LTset_attribute_string( outputFile, "/", "acknowledgement", tf_acknowledge);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the acknowledgement attribute in root group.\n");
        return FATAL_ERR;
    }

    errStatus = H5LTset_attribute_string( outputFile, "/", "version", tf_version);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the version attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "contributor_name", tf_contri_name);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the contributor_name attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "contributor_email", tf_contri_email);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the contributor_email attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "institution", tf_inst);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the institution attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "keywords", tf_kw);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the keywords attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "license", tf_lic);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the license attribute in root group.\n");
        return FATAL_ERR;
    }

    errStatus = H5LTset_attribute_string( outputFile, "/", "platform", tf_plat);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the platform attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "processing_level", tf_Pro_Level);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the processing_level attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "product_version", tf_Pro_Version);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the product_version attribute in root group.\n");
        return FATAL_ERR;
    }

    errStatus = H5LTset_attribute_string( outputFile, "/", "project", tf_project);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the project attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "sensors", tf_sensors);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the sensors attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "summary", tf_summary);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the summary attribute in root group.\n");
        return FATAL_ERR;
    }
    errStatus = H5LTset_attribute_string( outputFile, "/", "title", tf_title);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to set the title attribute in root group.\n");
        return FATAL_ERR;
    }

    return RET_SUCCESS;
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
    char* MODIStime = NULL;
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
        return FATAL_ERR;
    }


    /* MOPITT and CERES don't need to be checked. The validity of those files will be
     * checked in the subsetting functions.
     */
    do
    {
        status = getNextLine(string, inputFile );
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to get next line.\n");
            goto cleanupFail;
        }
    } while ( strstr(string, MODIScheck1) == NULL );



    /********************************************
     *                  MODIS                   *
     ********************************************/

    /* MODIS files have starting accuracy up to the minute */

    MODIStime = getTime(string, 2);
    if ( MODIStime == NULL )
    {
        FATAL_MSG("Failed to get MODIS time.\n");
        goto cleanupFail;
    }
    /* get MODIS year */
    for ( int i = 0; i < 4; i++ )
        tempString[i] = MODIStime[i];

    int MODISyear = atoi(tempString);
    if ( MODISyear == 0)
    {
        FATAL_MSG("Failed to get MODIS year.\n");
        goto cleanupFail;
    }

    memset(tempString, 0, STR_LEN);

    /* get MODIS proper day */
    for ( int i = 0; i < 3; i++ )
        tempString[i] = MODIStime[i+4];

    int MODISproperDay = atoi(tempString);
    if ( MODISproperDay == 0 )
    {
        FATAL_MSG("Failed to find proper MODIS day.\n");
        goto cleanupFail;
    }

    int MODISmonth;
    int MODISday;

    int leapYear = isLeapYear(MODISyear);
    if ( leapYear < 0 )
    {
        FATAL_MSG("Failed to determine if it is a leap year.\n");
        goto cleanupFail;
    }

    /* Using the MODIS proper day (which is a running total count of the days in the year)
     * find the day of the month.
     */

    if ( MODISproperDay >= 1 && MODISproperDay <= 31 )
    {
        MODISmonth = 1;
        MODISday = MODISproperDay;
    }
    else if ( MODISproperDay > 31 && MODISproperDay <= 59 + leapYear )
    {
        MODISmonth = 2;
        MODISday = MODISproperDay - 31;
    }
    else if ( MODISproperDay > 59 + leapYear && MODISproperDay <= 90 + leapYear )
    {
        MODISmonth = 3;
        MODISday = MODISproperDay - 59 + leapYear ;
    }
    else if ( MODISproperDay > 90  + leapYear&& MODISproperDay <= 120 + leapYear )
    {
        MODISmonth = 4;
        MODISday = MODISproperDay - 90 + leapYear;
    }
    else if ( MODISproperDay > 120 + leapYear && MODISproperDay <= 151 + leapYear )
    {
        MODISmonth = 5;
        MODISday = MODISproperDay - 120 + leapYear;
    }
    else if ( MODISproperDay > 151 + leapYear && MODISproperDay <= 181 + leapYear )
    {
        MODISmonth = 6;
        MODISday = MODISproperDay - 151 + leapYear;
    }
    else if ( MODISproperDay > 181 + leapYear && MODISproperDay <= 212 + leapYear )
    {
        MODISmonth = 7;
        MODISday = MODISproperDay - 181 + leapYear;
    }
    else if ( MODISproperDay > 212 + leapYear && MODISproperDay <= 243 + leapYear )
    {
        MODISmonth = 8;
        MODISday = MODISproperDay - 212 + leapYear;
    }
    else if ( MODISproperDay > 243 + leapYear && MODISproperDay <= 273 + leapYear )
    {
        MODISmonth = 9;
        MODISday = MODISproperDay - 243 + leapYear;
    }
    else if ( MODISproperDay > 273 + leapYear && MODISproperDay <= 304 + leapYear )
    {
        MODISmonth = 10;
        MODISday = MODISproperDay - 273 + leapYear;
    }
    else if ( MODISproperDay > 304 + leapYear && MODISproperDay <= 334 + leapYear )
    {
        MODISmonth = 11;
        MODISday = MODISproperDay - 304 + leapYear;
    }
    else if ( MODISproperDay > 334 + leapYear && MODISproperDay <= 365 + leapYear )
    {
        MODISmonth = 12;
        MODISday = MODISproperDay - 334 + leapYear;
    }
    else
    {
        FATAL_MSG("Failed to find the MODIS day.\n");
        goto cleanupFail;
    }

    /* Find the MODIS hour */

    memset(tempString, 0, STR_LEN);

    for ( int i = 0; i < 2; i++ )
        tempString[i] = MODIStime[i+8];

    int MODIShour = atoi(tempString);
    if ( MODIShour == 0 )
    {
        FATAL_MSG("Failed to find MODIS hour.\n");
        goto cleanupFail;
    }

    /* find MODIS minute */
    memset(tempString, 0, STR_LEN);


    for ( int i = 0; i < 2; i++ )
        tempString[i] = MODIStime[i+10];

    int MODISminute = atoi(tempString);
    if ( MODISminute == 0 )
    {
        FATAL_MSG("Failed to find MODIS minute.\n");
        goto cleanupFail;
    }

    /* Perform boundary checking */

    if ( ! (( MODISyear >= orbitInfo.start_year && MODISyear <= orbitInfo.end_year) &&
            ( MODISmonth >= orbitInfo.start_month && MODISmonth <= orbitInfo.end_month ) &&
            ( MODISday >= orbitInfo.start_day && MODISday <= orbitInfo.end_day ) &&
            ( MODIShour >= orbitInfo.start_hour && MODIShour <= orbitInfo.end_hour ) &&
            ( MODISminute >= orbitInfo.start_minute && MODISminute <= orbitInfo.end_minute ) ) )
    {
        FATAL_MSG("MODIS granule's starting time does not reside within orbit's start and end time.\n");
        goto cleanupFail;
    }

    /* next step is to make sure that the following MODIS files in the current MODIS group (either MOD03 or HKM AND QKM AND MOD03)
     * have the same date and time as the 1KM file. Note, each grouping of (1KM AND MOD03) OR (1KM AND HKM AND QKM AND MOD03)
     * is considered to be one group. The files MUST be ordered according to one of the two choices for a group.
     */


    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if (MODIStime) free(MODIStime);
    if ( fail )
        return FATAL_ERR;
    else
        return RET_SUCCESS;
}
#endif
