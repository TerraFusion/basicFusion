#include <stdio.h>
#include <mfhdf.h>
#include <stdlib.h>
#include <hdf5.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include "interp/aster/ASTERLatLon.h"
#include "libTERRA.h"

/* Author: MuQun Yang myang6@hdfgroup.org*/
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
int readThenWrite_ASTER_HR_LatLon(hid_t SWIRgeoGroupID,hid_t TIRgeoGroupID,hid_t VNIRgeoGroupID,char*latname,char*lonname,int32 h4_type,hid_t h5_type,int32 inFileID, hid_t outputFileID, char* granuleAppend);


/*
    argv[0] = program name
    argv[1] = granule file path
    argv[2] = granule name (granule1, granule2 etc)
    argv[3] = output file name
*/

int ASTER( char* argv[],int aster_count,int unpack)
{
    /*
        Need to add checks for the existence of the VNIR group.
        Some files will not have it.
    */


    /***************************************************************************
     *                                VARIABLES                                *
     ***************************************************************************/
    int32 inFileID = 0;
    int32 inHFileID = 0;
    int32 h4_status = 0;

    hid_t ASTERrootGroupID = 0;
    hid_t ASTERgranuleGroupID = 0;
    hid_t solar_geometryGroup = 0;
    hid_t pointingAngleGroup = 0;
    hid_t tempDsetID = 0;
    hid_t simplSpace = 0;
    hid_t solarGeomDim = 0;
    hid_t pointingDsetID = 0;
    char* dimName = NULL;
    char* fileTime = NULL;
    char* productmeta0 = NULL;
    char* sensorName [3] = { NULL, NULL, NULL};
    char* tempStr = NULL;
    char* pointer2 = NULL;
    char* granuleSuffix = NULL;
    char* tmpCharPtr = NULL;
    char* SWIRlat = NULL;
    char* SWIRlon = NULL;
    char* SWIRcoord = NULL;
    char* TIRlat = NULL;
    char* TIRlon = NULL;
    char* TIRcoord = NULL;
    char* VNIRlat = NULL;
    char* VNIRlon = NULL;
    char* VNIRcoord = NULL;
    ssize_t pathSize = 0;
    int fail = 0;
    int i;
    herr_t errStatus = 0;
    herr_t status = 0;
    int iStatus = 0;

    /***********************
     * SWIR data variables *
     ***********************/
    /* Group IDs */
    hid_t SWIRgroupID = 0;
    /* Dataset IDs */
    hid_t imageData4ID = 0;
    hid_t imageData5ID = 0;
    hid_t imageData6ID = 0;
    hid_t imageData7ID = 0;
    hid_t imageData8ID = 0;
    hid_t imageData9ID = 0;


    /***********************
     * VNIR data variables *
     ***********************/
    /* Group IDs */
    hid_t VNIRgroupID = 0;
    /* Dataset IDs */
    hid_t imageData1ID = 0;
    hid_t imageData2ID = 0;
    hid_t imageData3NID = 0;
    hid_t imageData3BID = 0;
    int32 imageData3Bindex = FAIL;

    /**********************
     * TIR data variables *
     **********************/
    /* Group IDs */
    hid_t TIRgroupID = 0;
    /* Dataset IDs */
    hid_t imageData10ID = 0;
    hid_t imageData11ID = 0;
    hid_t imageData12ID = 0;
    hid_t imageData13ID = 0;
    hid_t imageData14ID = 0;

    /******************************
     * Geolocation data variables *
     ******************************/
    /* Group IDs */
    hid_t geoGroupID = 0;

    hid_t SWIRgeoGroupID = 0;
    hid_t TIRgeoGroupID  = 0;
    hid_t VNIRgeoGroupID = 0;
    /* Dataset IDs */
    hid_t latDataID = 0;
    hid_t lonDataID = 0;

    const char* radianceNames[15] = {"ImageData1", "ImageData2", "ImageData3N", "ImageData3B", "ImageData4", "ImageData5", "ImageData6",
                               "ImageData7", "ImageData8", "ImageData9", "ImageData10", "ImageData11", "ImageData12",
                               "ImageData13", "ImageData14" 
                              };

    const char *pointAngleDimName = "ASTER_PointingAngleDim";
    const char *solarGeomDimName  = "ASTER_Solar_GeometryDim";
    /*
      MY 2016-12-20:
            Add checks for the existence of the VNIR group.
        Some files will not have it.
    */
    char *vnir_grp_name ="VNIR";
    char* swir_grp_name = "SWIR";
    char* tir_grp_name = "TIR";
    int32 vnir_grp_ref = -1;
    int32 swir_grp_ref = -1;
    int32 tir_grp_ref = -1;
// JUST FOR DEBUGGING
//unpack=0;

    /*
     *    * Open the HDF file for reading.
     *       */
    inHFileID = Hopen(argv[1],DFACC_READ, 0);
    if ( inHFileID < 0 )
    {
        FATAL_MSG("Failed to open ASTER file.\n\t%s\n", argv[1]);
        inHFileID = 0;
        goto cleanupFail;
    }

    /*
     *    * Initialize the V interface.
     *       */
    h4_status = Vstart (inHFileID);
    if ( h4_status < 0 )
    {
        FATAL_MSG("Failed to start V interface.\n");
        goto cleanupFail;
    }

    /* If VNIR exists, vnir_grp_ref should be > 0 */
    vnir_grp_ref = H4ObtainLoneVgroupRef(inHFileID,vnir_grp_name);
    if ( vnir_grp_ref < 0 )
    {
        FATAL_MSG("Failed to obtain lone V group reference.\n");
        goto cleanupFail;
    }
    swir_grp_ref = H4ObtainLoneVgroupRef(inHFileID,swir_grp_name);
    if ( swir_grp_ref < 0 )
    {
        FATAL_MSG("Failed to obtain lone V group reference.\n");
        goto cleanupFail;
    }
    tir_grp_ref = H4ObtainLoneVgroupRef(inHFileID,tir_grp_name);
    if ( tir_grp_ref < 0 )
    {
        FATAL_MSG("Failed to obtain lone V group reference.\n");
        goto cleanupFail;
    }



    /* No need inHFileID, close H and V interfaces */
    h4_status = Vend(inHFileID);
    h4_status = Hclose(inHFileID);
    inHFileID = 0;

    /* open the input file */
    inFileID = SDstart( argv[1], DFACC_READ );
    if ( inFileID < 0 )
    {
        FATAL_MSG("Failed to open the ASTER input file.\n\t%s\n", argv[1]);
        inFileID = 0;
        goto cleanupFail;
    }


    /* LTC May 24, 2017: We need to create a string that contains suffix "_g%d" where "%d" is the number of the current granule.
     * This is needed for the copyDimensions function calls to create unique dimensions for each of the datasets.
     */

    // integer division is a floor operation
    int numDigit = numDigits(aster_count);
    granuleSuffix = calloc ( 2 + numDigit + 1, 1 ); // 2 for "_g", numDigit for the integer, 1 for null terminator
    sprintf(granuleSuffix, "_g%d", aster_count);

    /********************************************************************************
     *                                GROUP CREATION                                *
     ********************************************************************************/

    /* ASTER root group */

    /* MY 2016-12-20: Create the root group for the first grnaule */
    if(aster_count == 1)
    {
        createGroup( &outputFile, &ASTERrootGroupID, "ASTER" );
        if ( ASTERrootGroupID == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create ASTER root group.\n");
            ASTERrootGroupID = 0;
            goto cleanupFail;
        }

    }

    else
    {

        ASTERrootGroupID = H5Gopen2(outputFile, "/ASTER",H5P_DEFAULT);
        if(ASTERrootGroupID <0)
        {
            FATAL_MSG("Failed to open the ASTER root group in the output file.\n");
            ASTERrootGroupID = 0;
            goto cleanupFail;
        }
    }

    // Create the granule group under ASTER group
    if ( createGroup( &ASTERrootGroupID, &ASTERgranuleGroupID,argv[2] ) )
    {
        FATAL_MSG("Failed to create ASTER granule group.\n");
        ASTERgranuleGroupID = 0;
        goto cleanupFail;
    }

    tmpCharPtr = strrchr( argv[1], '/' );
    if ( tmpCharPtr == NULL )
    {
        FATAL_MSG("Failed to find character within a string.\n");
        goto cleanupFail;
    }

    /* MY 2016-12-20: Add the granule time information. Now just add the file name */
    if(H5LTset_attribute_string(ASTERrootGroupID,argv[2],"GranuleName",tmpCharPtr+1)<0)
    {
        FATAL_MSG("Failed to add ASTER string attribute.\n");
        goto cleanupFail;
    }

    fileTime = getTime( argv[1], 3 );
    /* Landon 2017-1-21: Extract the time information from file name and add as attribute */
    if(H5LTset_attribute_string(ASTERrootGroupID,argv[2],"GranuleTime",fileTime)<0)
    {
        FATAL_MSG("Failed to add ASTER string attribute.\n");
        goto cleanupFail;
    }

    free ( fileTime );
    fileTime = NULL;


    /* LTC May 23, 2017: Create the Solar_Geometry group */
    createGroup( &ASTERgranuleGroupID, &solar_geometryGroup, "Solar_Geometry");
    if ( solar_geometryGroup == EXIT_FAILURE )
    {
        solar_geometryGroup = 0;
        FATAL_MSG("Failed to create the Solar_Geometry group.\n");
        goto cleanupFail;
    }

    /* LTC May 23, 2017: Create the PointingAngle group */
    createGroup( &ASTERgranuleGroupID, &pointingAngleGroup, "PointingAngle");
    if ( pointingAngleGroup == EXIT_FAILURE )
    {
        pointingAngleGroup = 0;
        FATAL_MSG("Failed to create the PointingAngle group.\n");
        goto cleanupFail;
    }

    /* SWIR group */
    if ( swir_grp_ref > 0 )
    {
        createGroup( &ASTERgranuleGroupID, &SWIRgroupID, "SWIR" );
        if ( SWIRgroupID == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create ASTER SWIR group.\n");
            SWIRgroupID = 0;
            goto cleanupFail;
        }
    }

    /* Only add the VNIR group if it exists.*/
    if(vnir_grp_ref >0)
    {
        createGroup( &ASTERgranuleGroupID, &VNIRgroupID, "VNIR" );
        if ( VNIRgroupID == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create ASTER VNIR group.\n");
            VNIRgroupID = 0;
            goto cleanupFail;
        }
    }

    if ( tir_grp_ref > 0 )
    {
        /* TIR group */
        createGroup( &ASTERgranuleGroupID, &TIRgroupID, "TIR" );
        if ( TIRgroupID == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create ASTER TIR group.\n");
            TIRgroupID = 0;
            goto cleanupFail;
        }
    }

    /* geolocation group */
    createGroup( &ASTERgranuleGroupID, &geoGroupID, "Geolocation" );
    if ( geoGroupID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to create ASTER geolocation group.\n");
        geoGroupID = 0;
        goto cleanupFail;
    }

    /*******************************************************************************
     *                              INSERT DATASETS                                *
     *******************************************************************************/


    /* LTC May 23, 2017: Add the SOLARDIRECTION values obtained from the productmetadata.0 (attribute of input ASTER file group)
     * as two, single-element HDF datasets.
     */


    // First open the productmetadata.0 attribute

    char prodmet0Name[] = "productmetadata.0";
    int32 meta0IDX = SDfindattr(inFileID, prodmet0Name);
    if ( meta0IDX == FAIL )
    {
        FATAL_MSG("Failed to get index of attribute.\n");
        goto cleanupFail;
    }

    // Get the size of the attribute
    int32 numElems;
    int32 ntype;
    intn statusn = SDattrinfo(inFileID, meta0IDX, prodmet0Name, &ntype, &numElems);
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to get the number of elements of the attribute.\n");
        goto cleanupFail;
    }

    productmeta0 = calloc( numElems+1, 1 );
    if ( productmeta0 == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    // read the attribute
    statusn = SDreadattr(inFileID, meta0IDX, productmeta0);
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to read the productmetadata.0 attribute.\n");
        goto cleanupFail;
    }

    // find the beginning of the POINTINGANGLES group
    char* productmeta0SubStr1 = strstr( productmeta0, "POINTINGANGLES" );
    if ( productmeta0SubStr1 == NULL )
    {
        FATAL_MSG("Failed to find the beginning of POINTINGANGLES metadata group.\n");
        goto cleanupFail;
    }

    // find the end of the POINTINGANGLES group
    char* productmeta0SubStr2 = strstr( productmeta0SubStr1+14, "END_GROUP");
    if ( productmeta0SubStr2 == NULL )
    {
        FATAL_MSG("Failed to find the end of POINTINGANGLES metadata group.\n");
        goto cleanupFail;
    }
    productmeta0SubStr2 = strstr( productmeta0SubStr2, "POINTINGANGLES");
    if ( productmeta0SubStr2 == NULL )
    {
        FATAL_MSG("Failed to find the end of POINTINGANGLES metadata group.\n");
        goto cleanupFail;
    }

    /* Next step is to find all of the SENSORNAME and POINTINGANGLE objects within the POINTINGANGLES group. */
    float pointAngleVal [3] = { 1000.0f, 1000.0f, 1000.0f };

    int j;
    char* pointer1 = productmeta0SubStr1;


    for ( i = 0; i < 3; i++ )
    {

        pointer1 = strstr( pointer1, "SENSORNAME");
        if ( pointer1 == NULL || pointer1 > productmeta0SubStr2 )
        {
            // If another SENSORNAME is not found within the POINTINGANGLES group, then exit out of the loop
            break;
        }

        pointer1 = strstr( pointer1, "VALUE");
        if ( pointer1 == NULL || pointer1 > productmeta0SubStr2 )
        {
            FATAL_MSG("Failed to find substring.\n");
            if ( pointer1 > productmeta0SubStr2 )
                fprintf(stderr, "\tThe desired substring was found to reside outside of the POINTINGANGLES group.\n");
            goto cleanupFail;
        }

        pointer1 = strstr( pointer1, "\"");
        if ( pointer1 == NULL || pointer1 > productmeta0SubStr2 )
        {
            FATAL_MSG("Failed to find substring.\n");
            if ( pointer1 > productmeta0SubStr2 )
                fprintf(stderr, "\tThe desired substring was found to reside outside of the POINTINGANGLES group.\n");
            goto cleanupFail;
        }

        // pointer1 points to the double quote. we want the first letter.
        pointer1++;

        // assert that pointer1 points to a capital alphabetic character
        if ( *pointer1 - 65 < 0 || *pointer1 - 65 > 25 )
        {
            FATAL_MSG("Expected an alphabetic character, but did not receive it.\n");
            goto cleanupFail;
        }
        pointer2 = strstr(pointer1, "\"");
        if ( pointer2 == NULL || pointer2 > productmeta0SubStr2 )
        {
            FATAL_MSG("Failed to find substring.\n");
            if ( pointer2 > productmeta0SubStr2 )
                fprintf(stderr, "\tThe desired substring was found to reside outside of the POINTINGANGLES group.\n");
            goto cleanupFail;
        }

        pointer2--;

        // copy sensorname over
        sensorName[i] = calloc( pointer2 - pointer1 + 2, 1);

        for ( j = 0; j < pointer2 - pointer1 + 1; j++ )
        {
            sensorName[i][j] = pointer1[j];
        }

        // check to make sure that the name that was just copied over is either "SWIR", "TIR" or "VIR"
        if ( strcmp( sensorName[i], "SWIR") && strcmp( sensorName[i], "TIR" ) && strcmp( sensorName[i], "VNIR" ) )
        {
            FATAL_MSG("The retrieved sensor name is neither \"SWIR\", \"TIR\" nor \"VIR\"\n");
            goto cleanupFail;
        }

        // find the pointing angle value
        pointer1 = strstr(pointer2, "POINTINGANGLE");
        if ( pointer1 == NULL || pointer1 > productmeta0SubStr2 )
        {
            FATAL_MSG("Failed to find substring.\n");
            if ( pointer1 > productmeta0SubStr2 )
                fprintf(stderr, "\tThe desired substring was found to reside outside of the POINTINGANGLES group.\n");
            goto cleanupFail;
        }

        pointer1 = strstr(pointer1, "VALUE");
        if ( pointer1 == NULL || pointer1 > productmeta0SubStr2 )
        {
            FATAL_MSG("Failed to find substring.\n");
            if ( pointer1 > productmeta0SubStr2 )
                fprintf(stderr, "\tThe desired substring was found to reside outside of the POINTINGANGLES group.\n");
            goto cleanupFail;
        }

        while ( !isdigit(*pointer1) )
            pointer1++;

        pointer2 = pointer1;

        // while pointer2 is digit or is a period
        while ( isdigit(*pointer2) || !(*pointer2 - 46) )
            pointer2++;

        pointer2--;

        tempStr = calloc( pointer2 - pointer1 + 2, 1 );
        for ( j = 0; j < pointer2 - pointer1 + 1; j++ )
        {
            tempStr[j] = pointer1[j];
        }

        pointAngleVal[i] = atof(tempStr);

        free(tempStr);
        tempStr = NULL;


        pointer1 = pointer2;
    } // end for

    hsize_t tempInt = 1;

    /* We need to attach dimensions to the pointing angle datasets, so make those first. */
    /* Make pointing angle dimension */
    
    // create dataspace
    simplSpace = H5Screate_simple( 1, &tempInt, NULL );
    if ( simplSpace < 0 )
    {
        FATAL_MSG("Failed to create a dataspace.\n");
        simplSpace = 0;
        goto cleanupFail;
    }
    // create dataset
    if ( aster_count == 1 )
    {    
        tempDsetID = H5Dcreate2( outputFile, pointAngleDimName, H5T_NATIVE_FLOAT, simplSpace, H5P_DEFAULT, H5P_DEFAULT,
                                       H5P_DEFAULT);
        if ( tempDsetID < 0 )
        {
            FATAL_MSG("Failed to create dataset.\n");
            tempDsetID = 0;
            goto cleanupFail;
        }
        // set dataset as a dimension scale
        status = H5DSset_scale( tempDsetID, "Placeholder" );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to set dataset as dimension scale.\n");
            goto cleanupFail;
        }
        
        iStatus = change_dim_attr_NAME_value(tempDsetID);
        if ( iStatus == FAIL )
        {
            FATAL_MSG("Failed to set the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }
    }
    else
    {
        tempDsetID = H5Dopen2( outputFile, pointAngleDimName, H5P_DEFAULT);
        if ( tempDsetID < 0 )
        {
            FATAL_MSG("Failed to open dataset.\n");
            tempDsetID = 0;
            goto cleanupFail;
        }
    }
    /* Make the pointingAngle datasets and then attach the dimension scale we just made */
    for ( j = 0; j < i; j++)
    {
        status = H5LTmake_dataset_float( pointingAngleGroup, sensorName[j], 1, &tempInt, &(pointAngleVal[j]) );
        if ( status != 0 )
        {
            FATAL_MSG("Failed to add a pointing angle dataset.\n");
            goto cleanupFail;
        }

        // Attach pure dimension
        pointingDsetID = H5Dopen2( pointingAngleGroup, sensorName[j], H5P_DEFAULT );
        if ( pointingDsetID < 0 )
        {
            FATAL_MSG("Failed to open dataset.\n");
            pointingDsetID = 0;
            goto cleanupFail;
        }
    
        status = H5DSattach_scale( pointingDsetID, tempDsetID, 0); 
        if ( status < 0 )
        {
            FATAL_MSG("Failed to attach scale to dataset.\n");
            goto cleanupFail;
        }
        H5Dclose(pointingDsetID); pointingDsetID = 0;
        
    }

    H5Dclose(tempDsetID); tempDsetID = 0;

    /* Create a Solar_Geometry dimension (same process as the dimension for pointing_angle) */

    if ( aster_count == 1 )
    {
        solarGeomDim = H5Dcreate2( outputFile, solarGeomDimName, H5T_NATIVE_FLOAT, simplSpace, H5P_DEFAULT, H5P_DEFAULT,
                                       H5P_DEFAULT);
        if ( solarGeomDim < 0 )
        {
            FATAL_MSG("Failed to create dataset.\n");
            tempDsetID = 0;
            goto cleanupFail;
        }

        status = H5DSset_scale( solarGeomDim, "Placeholder" );
        if ( status < 0 )
        {
            FATAL_MSG("Failed to set dataset as dimension scale.\n");
            goto cleanupFail;
        }

        iStatus = change_dim_attr_NAME_value(solarGeomDim);
        if ( iStatus == FAIL )
        {
            FATAL_MSG("Failed to set the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }
    } 
    else
    {
        solarGeomDim = H5Dopen2( outputFile, solarGeomDimName, H5P_DEFAULT );
        if ( solarGeomDim < 0 )
        {
            FATAL_MSG("Failed to open dataset.\n");
            solarGeomDim = 0;
            goto cleanupFail;
        }
    }

    // Find the SOLARDIRECTION object in the productmetadata.0
    pointer1 = productmeta0SubStr2;

    pointer1 = strstr(pointer1, "SOLARDIRECTION");
    if ( pointer1 == NULL )
    {
        FATAL_MSG("Failed to find the SOLARDIRECTION object in productmetadata.0\n");
        goto cleanupFail;
    }

    pointer1 = strstr(pointer1, "VALUE");
    if ( pointer1 == NULL )
    {
        FATAL_MSG("Failed to find desired substring.\n");
        goto cleanupFail;
    }

    pointer1 = strstr(pointer1, "(");
    if ( pointer1 == NULL )
    {
        FATAL_MSG("Failed to find desired substring.\n");
        goto cleanupFail;
    }

    // find the first solar direction value
    while( !isdigit(*pointer1) ) pointer1++;

    pointer2 = pointer1;

    while ( isdigit(*pointer2) || *pointer2 == 46 ) pointer2++;

    pointer2--;

    tempStr = calloc( pointer2 - pointer1 + 2, 1 );

    for ( i = 0; i < pointer2 - pointer1 + 1; i++ )
    {
        tempStr[i] = pointer1[i];
    }

    float solarDir[2];

    solarDir[0] = atof(tempStr);
    free(tempStr);
    tempStr = NULL;
    pointer1 = pointer2+1;


    // find the second solar direction value
    while( !isdigit(*pointer1) ) pointer1++;

    pointer2 = pointer1;
    while ( isdigit(*pointer2) || *pointer2 == 46 ) pointer2++;

    pointer2--;

    tempStr = calloc( pointer2 - pointer1 + 2, 1 );

    for ( i = 0; i < pointer2 - pointer1 + 1; i++ )
    {
        tempStr[i] = pointer1[i];
    }

    solarDir[1] = atof(tempStr);
    free(tempStr);
    tempStr = NULL;
    
    status = H5LTmake_dataset_float( solar_geometryGroup, "SolarAzimuth", 1, &tempInt, &(solarDir[0]) );
    if ( status != 0 )
    {
        FATAL_MSG("Failed to add a solar geometry dataset.\n");
        goto cleanupFail;
    }

    tempDsetID = H5Dopen2( solar_geometryGroup, "SolarAzimuth", H5P_DEFAULT );
    if ( tempDsetID < 0 )
    {
        FATAL_MSG("Failed to open dataset.\n");
        goto cleanupFail;
    }

    status = H5DSattach_scale( tempDsetID, solarGeomDim, 0);
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach scale to dataset.\n");
        goto cleanupFail;
    }

    H5Dclose(tempDsetID); tempDsetID = 0;


    status = H5LTmake_dataset_float( solar_geometryGroup, "SolarElevation", 1, &tempInt, &(solarDir[1]) );
    if ( status != 0 )
    {
        FATAL_MSG("Failed to add a solar geometry dataset.\n");
        tempDsetID = 0;
        goto cleanupFail;
    }

    tempDsetID = H5Dopen2( solar_geometryGroup, "SolarElevation", H5P_DEFAULT );
    if ( tempDsetID < 0 )
    {
        FATAL_MSG("Failed to open dataset.\n");
        tempDsetID = 0;
        goto cleanupFail;
    }

    status = H5DSattach_scale( tempDsetID, solarGeomDim, 0);
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach scale to dataset.\n");
        goto cleanupFail;
    }

    H5Dclose(tempDsetID); tempDsetID = 0;



    /* DONE WITH POINTING ANGLES AND SOLAR GEOMETRY */


    /* MY 2016-12-20: The following if-block will unpack the ASTER radiance data. I would like to clean up the code a little bit later.*/
    if(unpack == 1)
    {

        /* table 2-3 at https://lpdaac.usgs.gov/sites/default/files/public/product_documentation/aster_l1t_users_guide.pdf
         * row 0: high gain(HGH)
         * row 1: normal gain(NOR)
         * row 2: low gain 1(LO1)
         * row 3: low gain 2(LO2)
         * row 4: off(OFF) (not used just for mapping the off information at productmetada.0)
         * Since 3B and 3N share the same Unit conversion coefficient(UNC), we just use oen coeffieent for these two bands)
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
        float band_unc = 0.0f;
 
        /* Obtain the index of the gain */
        if(obtain_gain_index(inFileID,gain_index)==-1)
        {
            FATAL_MSG("Cannot obtain gain index.\n");
            goto cleanupFail;
        }
        /* obtain the Unit conversion coefficient */
        band_unc = unc[gain_index[4]][4];
        
        if ( swir_grp_ref > 0 )
        {

            /* SWIR */
            imageData4ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData4",
                           DFNT_UINT8, inFileID,unc[gain_index[4]][4] );
            if ( imageData4ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData4 dataset.\n");
                imageData4ID = 0;
                goto cleanupFail;
            }

            imageData5ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData5",
                           DFNT_UINT8, inFileID, unc[gain_index[5]][5] );
            if ( imageData5ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData5 dataset.\n");
                imageData5ID = 0;
                goto cleanupFail;
            }


            imageData6ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData6",
                           DFNT_UINT8, inFileID, unc[gain_index[6]][6]);
            if ( imageData6ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData6 dataset.\n");
                imageData6ID = 0;
                goto cleanupFail;
            }

            imageData7ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData7",
                           DFNT_UINT8, inFileID, unc[gain_index[7]][7] );
            if ( imageData7ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData7 dataset.\n");
                imageData7ID = 0;
                goto cleanupFail;
            }


            imageData8ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData8",
                           DFNT_UINT8, inFileID, unc[gain_index[8]][8]);
            if ( imageData8ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData8 dataset.\n");
                imageData8ID = 0;
                goto cleanupFail;
            }


            imageData9ID = readThenWrite_ASTER_Unpack( SWIRgroupID, "ImageData9",
                           DFNT_UINT8, inFileID,unc[gain_index[9]][9]);
            if ( imageData9ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData9 dataset.\n");
                imageData9ID = 0;
                goto cleanupFail;
            }
        }

        /* VNIR */
        if(vnir_grp_ref >0)
        {
            imageData1ID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData1",
                           DFNT_UINT8, inFileID,unc[gain_index[0]][0] );
            if ( imageData1ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData1 dataset.\n");
                imageData1ID = 0;
                goto cleanupFail;
            }

            imageData2ID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData2",
                           DFNT_UINT8, inFileID,unc[gain_index[1]][1] );
            if ( imageData2ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData2 dataset.\n");
                imageData2ID = 0;
                goto cleanupFail;
            }


            imageData3NID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData3N",
                            DFNT_UINT8, inFileID,unc[gain_index[2]][2] );
            if ( imageData3NID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData3N dataset.\n");
                imageData3NID = 0;
                goto cleanupFail;
            }


            /* We don't see ImageData3B in the current orbit, however, the table indeed indicates the 3B band.
               So here we check if there is an SDS with the name "ImangeData3B" and then do the conversation.
            */

            imageData3Bindex = SDnametoindex(inFileID,"ImageData3B");
            if(imageData3Bindex != FAIL)
            {
                imageData3BID = readThenWrite_ASTER_Unpack( VNIRgroupID, "ImageData3B",
                                DFNT_UINT8, inFileID,unc[gain_index[3]][3] );
                if ( imageData3BID == EXIT_FAILURE )
                {
                    FATAL_MSG("Failed to transfer ASTER ImageData3B dataset.\n");
                    imageData3BID = 0;
                    goto cleanupFail;
                }

            }
        }// end if(vnir_grp_ref)

        /* TIR */
        if ( tir_grp_ref > 0 )
        {
            imageData10ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData10",
                            DFNT_UINT16, inFileID,unc[gain_index[10]][10] );
            if ( imageData10ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData10 dataset.\n");
                imageData10ID = 0;
                goto cleanupFail;
            }


            imageData11ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData11",
                            DFNT_UINT16, inFileID, unc[gain_index[11]][11] );
            if ( imageData11ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData11 dataset.\n");
                imageData11ID = 0;
                goto cleanupFail;
            }


            imageData12ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData12",
                            DFNT_UINT16, inFileID, unc[gain_index[12]][12] );
            if ( imageData12ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData12 dataset.\n");
                imageData12ID = 0;
                goto cleanupFail;
            }


            imageData13ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData13",
                            DFNT_UINT16, inFileID, unc[gain_index[12]][12] );
            if ( imageData13ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData13 dataset.\n");
                imageData13ID = 0;
                goto cleanupFail;
            }


            imageData14ID = readThenWrite_ASTER_Unpack( TIRgroupID, "ImageData14",
                            DFNT_UINT16, inFileID,unc[gain_index[13]][13] );
            if ( imageData14ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData14 dataset.\n");
                imageData14ID = 0;
                goto cleanupFail;
            }
        }
    } // end if(unpacked =1)

    else
    {
        if ( swir_grp_ref > 0 )
        {
            imageData4ID = readThenWrite(NULL, SWIRgroupID, "ImageData4",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData4ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData4 dataset.\n");
                imageData4ID = 0;
                goto cleanupFail;
            }

            imageData5ID = readThenWrite(NULL, SWIRgroupID, "ImageData5",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData5ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData5 dataset.\n");
                imageData5ID = 0;
                goto cleanupFail;
            }

            imageData6ID = readThenWrite(NULL, SWIRgroupID, "ImageData6",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData6ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData6 dataset.\n");
                imageData6ID = 0;
                goto cleanupFail;
            }

            imageData7ID = readThenWrite(NULL, SWIRgroupID, "ImageData7",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData7ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData7 dataset.\n");
                imageData7ID = 0;
                goto cleanupFail;
            }

            imageData8ID = readThenWrite(NULL, SWIRgroupID, "ImageData8",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData8ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData8 dataset.\n");
                imageData8ID = 0;
                goto cleanupFail;
            }

            imageData9ID = readThenWrite(NULL, SWIRgroupID, "ImageData9",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData9ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData9 dataset.\n");
                imageData9ID = 0;
                goto cleanupFail;
            }
        }

        /* VNIR */
        if(vnir_grp_ref >0)
        {
            imageData1ID = readThenWrite(NULL, VNIRgroupID, "ImageData1",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData1ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData1 dataset.\n");
                imageData1ID = 0;
                goto cleanupFail;

            }

            imageData2ID = readThenWrite(NULL, VNIRgroupID, "ImageData2",
                                         DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData2ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData2 dataset.\n");
                imageData2ID = 0;
                goto cleanupFail;
            }

            imageData3NID = readThenWrite(NULL, VNIRgroupID, "ImageData3N",
                                          DFNT_UINT8, H5T_STD_U8LE, inFileID );
            if ( imageData3NID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData3N dataset.\n");
                imageData3NID = 0;
                goto cleanupFail;
            }

            /* We don't see ImageData3B in the current orbit, however, the table indeed indicates the 3B band.
               So here we check if there is an SDS with the name "ImangeData3B" and then do the conversation.
            */

            imageData3Bindex = SDnametoindex(inFileID,"ImageData3B");
            if(imageData3Bindex != FAIL)
            {

                imageData3BID = readThenWrite(NULL, VNIRgroupID, "ImageData3B",
                                              DFNT_UINT8, H5T_STD_U8LE, inFileID);
                if ( imageData3BID == EXIT_FAILURE )
                {
                    FATAL_MSG("Failed to transfer ASTER ImageData3B dataset.\n");
                    imageData3BID = 0;
                    goto cleanupFail;
                }

            }

        } // end if(vnir_grp_ref >0)

        /* TIR */
        if ( tir_grp_ref > 0 )
        {
            imageData10ID = readThenWrite(NULL, TIRgroupID, "ImageData10",
                                          DFNT_UINT16, H5T_STD_U16LE, inFileID );
            if ( imageData10ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData10 dataset.\n");
                imageData10ID = 0;
                goto cleanupFail;
            }

            imageData11ID = readThenWrite(NULL, TIRgroupID, "ImageData11",
                                          DFNT_UINT16, H5T_STD_U16LE, inFileID );
            if ( imageData11ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData11 dataset.\n");
                imageData11ID = 0;
                goto cleanupFail;
            }

            imageData12ID = readThenWrite(NULL, TIRgroupID, "ImageData12",
                                          DFNT_UINT16, H5T_STD_U16LE, inFileID );
            if ( imageData12ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData12 dataset.\n");
                imageData12ID = 0;
                goto cleanupFail;
            }

            imageData13ID = readThenWrite(NULL, TIRgroupID, "ImageData13",
                                          DFNT_UINT16, H5T_STD_U16LE, inFileID );
            if ( imageData13ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData13 dataset.\n");
                imageData13ID = 0;
                goto cleanupFail;
            }

            imageData14ID = readThenWrite(NULL, TIRgroupID, "ImageData14",
                                          DFNT_UINT16, H5T_STD_U16LE, inFileID );
            if ( imageData14ID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to transfer ASTER ImageData14 dataset.\n");
                imageData14ID = 0;
                goto cleanupFail;
            }
        }

    }// else (packed)

    /* LTC May 26, 2017: Add the units attribute to all of the radiance fields */

    hid_t dsetArray[15] = { imageData1ID, imageData2ID, imageData3NID, imageData3BID, imageData4ID, imageData5ID, imageData6ID,
                            imageData7ID, imageData8ID, imageData9ID, imageData10ID, imageData11ID, imageData12ID, imageData12ID,
                            imageData14ID };

    for ( i = 0; i < 4; i++ )
    {
        if ( !dsetArray[i] ) continue;

        status = H5LTset_attribute_string( VNIRgroupID, radianceNames[i], "Units", "Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to set string attribute. i = %d\n", i);
            goto cleanupFail;
        }
    }

    for ( i = 0; i < 6; i++ )
    {
        if ( !dsetArray[i+4] ) continue;

        status = H5LTset_attribute_string( SWIRgroupID, radianceNames[i+4], "Units", "Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to set string attribute. i = %d\n", i);
            goto cleanupFail;
        }
    }

    for ( i = 0; i < 5; i++ )
    {
        if ( !dsetArray[i+10] ) continue;

        status = H5LTset_attribute_string( TIRgroupID, radianceNames[i+10], "Units", "Watts/m^2/micrometer/steradian");
        if ( status < 0 )
        {
            FATAL_MSG("Failed to set string attribute. i = %d\n", i);
            goto cleanupFail;
        }
    }

    // Copy the dimensions
    if(vnir_grp_ref >0)
    {
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData1", outputFile, imageData1ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData2", outputFile, imageData2ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData3N", outputFile, imageData3NID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        if ( imageData3BID)
        {
            errStatus = copyDimension(granuleSuffix, inFileID, "ImageData3B", outputFile, imageData3BID);
            if ( errStatus == FAIL )
            {
                FATAL_MSG("Failed to copy dimensions.\n");
                goto cleanupFail;
            }
        }
    }

    if ( swir_grp_ref > 0 )
    {
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData4", outputFile, imageData4ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData5", outputFile, imageData5ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData6", outputFile, imageData6ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData7", outputFile, imageData7ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData8", outputFile, imageData8ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData9", outputFile, imageData9ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
    }

    if ( tir_grp_ref > 0 )
    {
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData10", outputFile, imageData10ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData11", outputFile, imageData11ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData12", outputFile, imageData12ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData13", outputFile, imageData13ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
        errStatus = copyDimension(granuleSuffix, inFileID, "ImageData14", outputFile, imageData14ID);
        if ( errStatus == FAIL )
        {
            FATAL_MSG("Failed to copy dimensions.\n");
            goto cleanupFail;
        }
    }


    /* geolocation */
    latDataID = readThenWrite(NULL, geoGroupID, "Latitude",
                              DFNT_FLOAT64, H5T_NATIVE_DOUBLE, inFileID );
    if ( latDataID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to transfer ASTER Latitude dataset.\n");
        latDataID = 0;
        goto cleanupFail;
    }
    errStatus = copyDimension( NULL, inFileID, "Latitude", outputFile, latDataID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }


    // MY 2016-01-26: add latitude units.
    if(H5LTset_attribute_string(geoGroupID,"Latitude","units","degrees_north")<0)
    {
        FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
        goto cleanupFail;
    }

    lonDataID = readThenWrite(NULL, geoGroupID, "Longitude",
                              DFNT_FLOAT64, H5T_NATIVE_DOUBLE, inFileID );
    if ( lonDataID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to transfer ASTER Longitude dataset.\n");
        lonDataID = 0;
        goto cleanupFail;
    }
    errStatus = copyDimension( NULL, inFileID, "Longitude", outputFile, lonDataID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }


    if(H5LTset_attribute_string(geoGroupID,"Longitude","units","degrees_east")<0)
    {
        FATAL_MSG("Unable to insert ASTER longitude units attribute.\n");
        goto cleanupFail;
    }

    // Adding high-resolution lat/lon dataset
    if(unpack == 1)
    {

        if ( swir_grp_ref > 0 )
        {
            createGroup( &SWIRgroupID, &SWIRgeoGroupID, "Geolocation" );
            if ( SWIRgeoGroupID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to create ASTER SWIR Geolocation group.\n");
                SWIRgeoGroupID = 0;
                goto cleanupFail;
            }
        }

        if ( tir_grp_ref > 0 )
        {
            createGroup( &TIRgroupID, &TIRgeoGroupID, "Geolocation" );
            if ( TIRgeoGroupID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to create ASTER TIR Geolocation group.\n");
                TIRgeoGroupID = 0;
                goto cleanupFail;
            }
        }

        if(vnir_grp_ref >0)
        {
            createGroup( &VNIRgroupID, &VNIRgeoGroupID, "Geolocation" );
            if ( VNIRgeoGroupID == EXIT_FAILURE )
            {
                FATAL_MSG("Failed to create ASTER VNIR Geolocation group.\n");
                VNIRgeoGroupID = 0;
                goto cleanupFail;
            }

        }
        iStatus = readThenWrite_ASTER_HR_LatLon(SWIRgeoGroupID,TIRgeoGroupID,VNIRgeoGroupID,"Latitude","Longitude",DFNT_FLOAT64,H5T_NATIVE_DOUBLE,inFileID, outputFile, granuleSuffix);

        if ( iStatus == EXIT_FAILURE )
        {
            FATAL_MSG("HR_LatLon failed.\n");
            goto cleanupFail;
        }

        /* Save the geolocation paths */
        if ( SWIRgeoGroupID )
        {
            /* Latitude */
            pathSize = H5Iget_name( SWIRgeoGroupID, NULL, 0 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to get the size of the path name.\n");
                goto cleanupFail;
            }
            SWIRlon = calloc(pathSize + strlen("/Longitude") + 1, 1 );
            if ( SWIRlon == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            pathSize = pathSize+strlen("/Latitude");

            SWIRlat= calloc(pathSize+1, 1 );
            if ( SWIRlat == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }

            pathSize = H5Iget_name( SWIRgeoGroupID, SWIRlat, pathSize+1 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to retrieve latitude path name.\n");
                goto cleanupFail;
            }
            strncpy(SWIRlon, SWIRlat, strlen(SWIRlat));
            strncat(SWIRlat, "/Latitude", strlen("/Latitude"));
            strncat(SWIRlon, "/Longitude", strlen("/Longitude"));
            
            SWIRcoord = calloc(strlen(SWIRlon) + strlen(SWIRlat) + 2, 1 );
            if ( SWIRcoord == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            strncpy(SWIRcoord,SWIRlon,strlen(SWIRlon));
            strncat(SWIRcoord, " ", 1);
            strncat(SWIRcoord, SWIRlat, strlen(SWIRlat));
            free(SWIRlat); SWIRlat = NULL;
            free(SWIRlon); SWIRlon = NULL;
        
            for ( i = 0; i < 6; i++ )
            {
                if ( !dsetArray[i+4] ) continue;

                status = H5LTset_attribute_string( SWIRgroupID, radianceNames[i+4], "coordinates", SWIRcoord);
                if ( status < 0 )
                {
                    FATAL_MSG("Failed to set string attribute. i = %d\n", i);
                    goto cleanupFail;
                }
            }
        }

        if ( TIRgeoGroupID )
        {
            /* Latitude */
            pathSize = H5Iget_name( TIRgeoGroupID, NULL, 0 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to get the size of the path name.\n");
                goto cleanupFail;
            }
            TIRlon = calloc(pathSize + strlen("/Longitude") + 1, 1 );
            if ( TIRlon == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            pathSize = pathSize+strlen("/Latitude");

            TIRlat= calloc(pathSize+1, 1 );
            if ( TIRlat == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }

            pathSize = H5Iget_name( TIRgeoGroupID, TIRlat, pathSize+1 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to retrieve latitude path name.\n");
                goto cleanupFail;
            }
            /* Copy over the Geolocation path to TIRlon */
            strncpy(TIRlon, TIRlat, strlen(TIRlat));
            /* Concatenate Latitude or Longitude to the paths */
            strncat(TIRlat, "/Latitude", strlen("/Latitude"));
            strncat(TIRlon, "/Longitude", strlen("/Longitude"));

            TIRcoord = calloc(strlen(TIRlon) + strlen(TIRlat) + 2, 1 );
            if ( TIRcoord == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            strncpy(TIRcoord,TIRlon,strlen(TIRlon));
            strncat(TIRcoord, " ", 1);
            strncat(TIRcoord, TIRlat, strlen(TIRlat));
            free(TIRlat); TIRlat = NULL;
            free(TIRlon); TIRlon = NULL;

            for ( i = 0; i < 5; i++ )
            {
                if ( !dsetArray[i+10] ) continue;

                status = H5LTset_attribute_string( TIRgroupID, radianceNames[i+10], "coordinates", TIRcoord);
                if ( status < 0 )
                {
                    FATAL_MSG("Failed to set string attribute. i = %d\n", i);
                    goto cleanupFail;
                }
            }
        }

        if ( VNIRgeoGroupID )
        {
            /* Latitude */
            pathSize = H5Iget_name( VNIRgeoGroupID, NULL, 0 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to get the size of the path name.\n");
                goto cleanupFail;
            }
            
            VNIRlon = calloc(pathSize + strlen("/Longitude") + 1, 1 );
            if ( VNIRlon == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            pathSize = pathSize+strlen("/Latitude");

            VNIRlat= calloc(pathSize+1, 1 );
            if ( VNIRlat == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }

            pathSize = H5Iget_name( VNIRgeoGroupID, VNIRlat, pathSize+1 );
            if ( pathSize < 0 )
            {
                FATAL_MSG("Failed to retrieve latitude path name.\n");
                goto cleanupFail;
            }
            strncpy(VNIRlon, VNIRlat, strlen(VNIRlat));
            strncat(VNIRlat, "/Latitude", strlen("/Latitude"));
            strncat(VNIRlon, "/Longitude", strlen("/Longitude"));

            VNIRcoord = calloc(strlen(VNIRlon) + strlen(VNIRlat) + 2, 1 );
            if ( VNIRcoord == NULL )
            {
                FATAL_MSG("Failed to allocate memory.\n");
                goto cleanupFail;
            }
            strncpy(VNIRcoord,VNIRlon,strlen(VNIRlon));
            strncat(VNIRcoord, " ", 1);
            strncat(VNIRcoord, VNIRlat, strlen(VNIRlat));
            free(VNIRlat); VNIRlat = NULL;
            free(VNIRlon); VNIRlon = NULL;

            
            for ( i = 0; i < 4; i++ )
            {
                if ( !dsetArray[i] ) continue;

                status = H5LTset_attribute_string( VNIRgroupID, radianceNames[i], "coordinates", VNIRcoord);
                if ( status < 0 )
                {
                    FATAL_MSG("Failed to set string attribute. i = %d\n", i);
                    goto cleanupFail;
                }
            }
        } 
    } // end if ( unpack == 1 )


    /* release identifiers */
    if ( 0 )
    {
cleanupFail:
        fail = 1;
    }

    if ( solar_geometryGroup ) H5Gclose(solar_geometryGroup);
    if ( inHFileID ) Vend(inHFileID);
    if ( inHFileID ) Hclose(inHFileID);
    if ( inFileID ) SDend(inFileID);
    if ( ASTERrootGroupID ) H5Gclose(ASTERrootGroupID);
    if ( ASTERgranuleGroupID ) H5Gclose(ASTERgranuleGroupID);
    if ( fileTime ) free ( fileTime );
    if ( SWIRgroupID ) H5Gclose(SWIRgroupID);
    if ( VNIRgroupID ) H5Gclose(VNIRgroupID);
    if ( TIRgroupID ) H5Gclose(TIRgroupID);
    if ( geoGroupID ) H5Gclose(geoGroupID);
    if ( SWIRgeoGroupID ) H5Gclose(SWIRgeoGroupID);
    if ( VNIRgeoGroupID ) H5Gclose(VNIRgeoGroupID);
    if ( TIRgeoGroupID ) H5Gclose(TIRgeoGroupID);
    if ( imageData4ID ) H5Dclose(imageData4ID);
    if ( imageData5ID ) H5Dclose(imageData5ID);
    if ( imageData6ID ) H5Dclose(imageData6ID);
    if ( imageData7ID ) H5Dclose(imageData7ID);
    if ( imageData8ID ) H5Dclose(imageData8ID);
    if ( imageData9ID ) H5Dclose(imageData9ID);
    if ( imageData1ID ) H5Dclose(imageData1ID);
    if ( imageData2ID ) H5Dclose(imageData2ID);
    if ( imageData3NID ) H5Dclose(imageData3NID);
    if ( imageData10ID ) H5Dclose(imageData10ID);
    if ( imageData11ID ) H5Dclose(imageData11ID);
    if ( imageData12ID ) H5Dclose(imageData12ID);
    if ( imageData13ID ) H5Dclose(imageData13ID);
    if ( imageData14ID ) H5Dclose(imageData14ID);
    if ( imageData3BID ) H5Dclose(imageData3BID);
    if ( latDataID ) H5Dclose(latDataID);
    if ( lonDataID ) H5Dclose(lonDataID);
    if ( productmeta0 ) free (productmeta0);
    if ( pointingAngleGroup) H5Gclose(pointingAngleGroup);
    if ( sensorName[0] ) free(sensorName[0]);
    if ( sensorName[1] ) free(sensorName[1]);
    if ( sensorName[2] ) free(sensorName[2]);
    if ( tempStr ) free(tempStr);
    if ( granuleSuffix ) free(granuleSuffix);
    if ( tempDsetID ) H5Dclose(tempDsetID);
    if ( dimName ) free(dimName);
    if ( simplSpace ) H5Sclose(simplSpace);
    if ( solarGeomDim ) H5Dclose(solarGeomDim);
    if ( pointingDsetID ) H5Dclose(pointingDsetID);
    if ( TIRlat ) free(TIRlat);
    if ( TIRlon ) free(TIRlon);
    if ( TIRcoord ) free(TIRcoord);
    if ( VNIRlat ) free(VNIRlat);
    if ( VNIRlon ) free(VNIRlon);
    if ( VNIRcoord ) free(VNIRcoord);
    if ( SWIRlat ) free(SWIRlat);
    if ( SWIRlon ) free(SWIRlon);
    if ( SWIRcoord) free(SWIRcoord);
    if ( fail ) return EXIT_FAILURE;
    return EXIT_SUCCESS;

}

char* obtain_gain_info(char *whole_string)
{

    char* begin_mark="GAININFORMATION\n";
    char* sub_string=strstr(whole_string,begin_mark);

    return sub_string;
}

short get_band_index(char *band_index_str)
{

    const char *band_index_str_list[10] = {"01","02","3N","3B","04","05","06","07","08","09"};
    int i = 0;
    for(i= 0; i <11; i++)
    {

        if(!strncmp(band_index_str,band_index_str_list[i],2))
        {
            return i;
        }
    }

    FATAL_MSG("Failed to get the ASTER band index.\n");
    return -1;
}

short get_gain_stat(char *gain_stat_str)
{

    const char *gain_stat_str_list[5]  = {"HGH","NOR","LO1","LO2","OFF"};
    int i = 0;
    for(i= 0; i <5; i++)
    {

        if(!strncmp(gain_stat_str,gain_stat_str_list[i],3))
        {
            return i;
        }
    }
    
    FATAL_MSG("Failed to get the ASTER gain status.\n");
    return -1;
}

int obtain_gain_index(int32 sd_id,short gain_index[15])
{

    const char* metadata_gain="productmetadata.0";
    intn    status = 0;
    int32   attr_index = -1;
    int32   data_type = -1;
    int32   n_values = -1;
    char    attr_name[H4_MAX_NC_NAME];
    int     i = 0;
    int     band_index = -1;
    int     temp_gain_index = -1;
    char *fileattr_data= NULL;
    char * string_gaininfo = NULL;
    char * string_nogaininfo = NULL;
    char* gain_string = NULL;
    size_t gain_string_len = 0;
    char*  tp= NULL;
    char* band_index_str = NULL;
    char* gain_stat_str = NULL;
    int  gain[15] = {0};
    int ret_value = 0;
    /*
    * Find the file attribute defined by FILE_ATTR_NAME.
    */
    attr_index = SDfindattr (sd_id, metadata_gain);
    if(attr_index == FAIL)
    {
        FATAL_MSG("SDfindattr failed for the attribute <productmetadata.0>.\n");
        goto cleanupFail;
    }

    /*
    * Get information about the file attribute. Note that the first
    * parameter is an SD interface identifier.
    */
    status = SDattrinfo (sd_id, attr_index, attr_name, &data_type, &n_values);
    if(status == FAIL)
    {
        FATAL_MSG("SDattrinfo failed for the attribute <productmetadata.0>.\n");
        goto cleanupFail;
    }


    /* The data type should be DFNT_CHAR, from SD_set_attr.c */
    if (data_type == DFNT_CHAR)
    {
        /*
        * Allocate a buffer to hold the attribute data.
        */
        fileattr_data = calloc (n_values + 1, 1);
        if(fileattr_data == NULL)
        {
            FATAL_MSG("Cannot allocate the buffer for fileattr_data.\n");
            goto cleanupFail;
        }

        /*
         * Read the file attribute data.
         */
        status = SDreadattr (sd_id, attr_index, fileattr_data);
        if(status == FAIL)
        {
            FATAL_MSG("SDreadattr failed for attribute <productmetadata.0>\n");
            goto cleanupFail;
        }

        /*
        * Print out file attribute value and free buffer.
        */

        string_gaininfo = obtain_gain_info(fileattr_data);

        string_gaininfo++;

        string_nogaininfo = obtain_gain_info(string_gaininfo);

        gain_string_len = strlen(string_gaininfo) - strlen(string_nogaininfo);
        gain_string=malloc(gain_string_len+1);
        if(gain_string == NULL)
        {
            FATAL_MSG("Cannot allocate the buffer for gain_string.\n");
            goto cleanupFail;
        }

        strncpy(gain_string,string_gaininfo,gain_string_len);

        gain_string[gain_string_len]='\0';
        tp = gain_string;

        band_index_str = malloc(3);
        if(band_index_str == NULL)
        {
            FATAL_MSG("Cannot allocate the buffer for band_index_str.\n");
            goto cleanupFail;
        }

        memset(band_index_str,0,3);
        gain_stat_str  = malloc(4);
        if(gain_stat_str == NULL)
        {
            FATAL_MSG("Cannot allocate the buffer for gain_stat_str.\n");
            goto cleanupFail;
        }

        memset(gain_stat_str,0,4);

        while(*tp!='\0')
        {
            tp = strchr(tp,'(');
            if(tp == NULL)
                break;
            // Move to the band string
            tp+=2;
            // tp starts with the number
            strncpy(band_index_str,tp,2);

            band_index = get_band_index(band_index_str);
            if ( band_index == -1 )
            {
                FATAL_MSG("Couldn't retrieve the band index.\n");
                goto cleanupFail;
            }
            //skip 6 characters starting from 0 [01", "]
            tp+=6;
            strncpy(gain_stat_str,tp,3);

            /* LTC July 6, 2017: 
             * The VNIR bands (cameras 1, 2, 3N) do not have a "LO1" or "LO2" marker.
             * Instead, they may have "LOW" as a marker. In that case, they need to 
             * be mapped to the "LO1" conversion coefficients because "LO2" does not
             * provide information for them. The band gains can be found here:
             * https://lpdaac.usgs.gov/sites/default/files/public/product_documentation/aster_l1t_product_specification.pdf
             * Page 13.
             */

            if ( (band_index >= 0 && band_index <= 2) && strstr("LOW", gain_stat_str) )
            {
                strncpy(gain_stat_str, "LO1", 3);
            }

            temp_gain_index = get_gain_stat(gain_stat_str);
            if ( temp_gain_index == -1 )
            {
                FATAL_MSG("get_gain_stat failed.\n");
                goto cleanupFail;
            }

            gain[band_index] = temp_gain_index;

        }

        // Always normal gain
        if(band_index <11)
        {
            for (i = 10; i<15; i++)
                gain[i] = 1;
        }

        for(i = 0; i <15; i++)
            gain_index[i] = gain[i];

    }

    if ( 0 )
    {
        cleanupFail:
        ret_value = -1;
    }

    
    if(gain_string) free(gain_string);
    if(band_index_str) free(band_index_str);
    if(gain_stat_str) free(gain_stat_str);
    if(fileattr_data) free (fileattr_data);

    return ret_value;
}

int readThenWrite_ASTER_HR_LatLon(hid_t SWIRgeoGroupID,hid_t TIRgeoGroupID,hid_t VNIRgeoGroupID,char*latname,char*lonname,int32 h4_type,hid_t h5_type,int32 inFileID, hid_t outputFileID, char* granuleAppend )
{

    int retVal = 0;
    int32 latRank,lonRank;
    int32 latDimSizes[DIM_MAX];
    int32 lonDimSizes[DIM_MAX];
    double* latBuffer = NULL;
    double* lonBuffer = NULL;
    hid_t SWIR_ImageLine_DimID = 0;
    hid_t SWIR_ImagePixel_DimID = 0;
    hid_t TIR_ImageLine_DimID = 0;
    hid_t TIR_ImagePixel_DimID = 0;
    hid_t VNIR_ImageLine_DimID = 0;
    hid_t VNIR_ImagePixel_DimID = 0;
    herr_t status;

    int i = 0;
    int nSWIR_ImageLine = 0;
    int nSWIR_ImagePixel = 0;
    int nTIR_ImageLine = 0;
    int nTIR_ImagePixel = 0;
    int nVNIR_ImageLine = 0;
    int nVNIR_ImagePixel = 0;


    double* lat_swir_buffer = NULL;
    double* lon_swir_buffer = NULL;
    double* lat_tir_buffer = NULL;
    double* lon_tir_buffer = NULL;
    double* lat_vnir_buffer = NULL;
    double* lon_vnir_buffer = NULL;

    char* prefix1[2]= {"ImageLine_SWIR_Swath","ImagePixel_SWIR_Swath"};
    char* prefix2[2]= {"ImageLine_TIR_Swath","ImagePixel_TIR_Swath"};
    char* prefix3[2]= {"ImageLine_VNIR_Swath","ImagePixel_VNIR_Swath"};

    char* ll_swir_dimnames[2] = {NULL};
    char* ll_tir_dimnames[2] = {NULL};
    char* ll_vnir_dimnames[2] = {NULL};


    /* Append the granuleAppend string to the prefixes.
     * Not sure if there is a more elegant way to do this besides explicitly passing in all the dimension names,
     * so I'll simply just pass in the granule suffix (granuleAppend) and concatenate it with the dimension prefixes.
     * LTC May 26, 2017
     */

    for ( i = 0; i < 2; i++ )
    {
        ll_swir_dimnames[i] = calloc( strlen(prefix1[i] ) + strlen(granuleAppend) + 1, 1 ); 
        if ( ll_swir_dimnames[i] == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        sprintf(ll_swir_dimnames[i], "%s%s", prefix1[i], granuleAppend);
    }   
    for ( i = 0; i < 2; i++ )
    {
        ll_tir_dimnames[i] = calloc( strlen(prefix2[i] ) + strlen(granuleAppend) + 1, 1 );
        if ( ll_tir_dimnames[i] == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        sprintf(ll_tir_dimnames[i], "%s%s", prefix2[i], granuleAppend);
    }  
    for ( i = 0; i < 2; i++ )
    {
        ll_vnir_dimnames[i] = calloc( strlen(prefix3[i] ) + strlen(granuleAppend) + 1, 1 );
        if ( ll_vnir_dimnames[i] == NULL )
        {
            FATAL_MSG("Failed to allocate memory.\n");
            goto cleanupFail;
        }
        sprintf(ll_vnir_dimnames[i], "%s%s", prefix3[i], granuleAppend);
    }  



    status = H4readData( inFileID, latname,
                         (void**)&latBuffer, &latRank, latDimSizes, h4_type,NULL,NULL,NULL );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read %s data.\n", latname );
        goto cleanupFail;
    }

    status = H4readData( inFileID, lonname,
                         (void**)&lonBuffer, &lonRank, lonDimSizes, h4_type,NULL,NULL,NULL );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read %s data.\n", lonname );
        goto cleanupFail;
    }
    if(latRank !=2 || lonRank!=2)
    {
        FATAL_MSG("The latitude and longitude array rank must be 2.\n");
        goto cleanupFail;
    }
    if(latDimSizes[0]!=lonDimSizes[0] || latDimSizes[1]!=lonDimSizes[1])
    {
        FATAL_MSG("The latitude and longitude array rank must share the same dimension sizes.\n");
        goto cleanupFail;
    }

    /* END READ DATA. BEGIN Computing DATA */

    if ( SWIRgeoGroupID )
    {
        SWIR_ImageLine_DimID = H5Dopen2(outputFileID,ll_swir_dimnames[0],H5P_DEFAULT);
        if ( SWIR_ImageLine_DimID < 0 )
        {
            FATAL_MSG("Failed to open dataset.\n");
            goto cleanupFail;
        }
        nSWIR_ImageLine = obtainDimSize(SWIR_ImageLine_DimID);
        SWIR_ImagePixel_DimID = H5Dopen2(outputFileID,ll_swir_dimnames[1],H5P_DEFAULT);
        if ( SWIR_ImagePixel_DimID < 0 )
        {
            FATAL_MSG("Failed to open dataset.\n");
            goto cleanupFail;
        }
        nSWIR_ImagePixel = obtainDimSize(SWIR_ImagePixel_DimID);

        lat_swir_buffer = (double*)malloc(sizeof(double)*nSWIR_ImageLine*nSWIR_ImagePixel);
        if(lat_swir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lat_swir_buffer.\n");
            goto cleanupFail;
        }

        lon_swir_buffer = (double*)malloc(sizeof(double)*nSWIR_ImageLine*nSWIR_ImagePixel);
        if(lon_swir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lon_swir_buffer.\n");
            goto cleanupFail;
        }

        asterLatLonSpherical(latBuffer,lonBuffer,lat_swir_buffer,lon_swir_buffer,nSWIR_ImageLine,nSWIR_ImagePixel);

        // SWIR Latitude
        if (Generate2D_Dataset(SWIRgeoGroupID,latname,h5_type,lat_swir_buffer,SWIR_ImageLine_DimID,SWIR_ImagePixel_DimID,nSWIR_ImageLine,nSWIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lat_swir_buffer); lat_swir_buffer = NULL;
        if(H5LTset_attribute_string(SWIRgeoGroupID,latname,"units","degrees_north")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }

        //SWIR Longitude
        if (Generate2D_Dataset(SWIRgeoGroupID,lonname,h5_type,lon_swir_buffer,SWIR_ImageLine_DimID,SWIR_ImagePixel_DimID,nSWIR_ImageLine,nSWIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lon_swir_buffer); lon_swir_buffer = NULL;
        if(H5LTset_attribute_string(SWIRgeoGroupID,lonname,"units","degrees_east")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }
        H5Dclose(SWIR_ImageLine_DimID); SWIR_ImageLine_DimID = 0;
        H5Dclose(SWIR_ImagePixel_DimID); SWIR_ImagePixel_DimID = 0;
    }

    // TIR
    if ( TIRgeoGroupID )
    {
        TIR_ImageLine_DimID = H5Dopen2(outputFileID,ll_tir_dimnames[0],H5P_DEFAULT);
        nTIR_ImageLine = obtainDimSize(TIR_ImageLine_DimID);
        TIR_ImagePixel_DimID = H5Dopen2(outputFileID,ll_tir_dimnames[1],H5P_DEFAULT);
        nTIR_ImagePixel = obtainDimSize(TIR_ImagePixel_DimID);


        lat_tir_buffer = (double*)malloc(sizeof(double)*nTIR_ImageLine*nTIR_ImagePixel);
        if(lat_tir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lat_tir_buffer.\n");
            goto cleanupFail;
        }

        lon_tir_buffer = (double*)malloc(sizeof(double)*nTIR_ImageLine*nTIR_ImagePixel);
        if(lon_tir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lon_tir_buffer.\n");
            goto cleanupFail;
        }

        asterLatLonSpherical(latBuffer,lonBuffer,lat_tir_buffer,lon_tir_buffer,nTIR_ImageLine,nTIR_ImagePixel);

        // TIR Latitude
        if (Generate2D_Dataset(TIRgeoGroupID,latname,h5_type,lat_tir_buffer,TIR_ImageLine_DimID,TIR_ImagePixel_DimID,nTIR_ImageLine,nTIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lat_tir_buffer); lat_tir_buffer = NULL;
        if(H5LTset_attribute_string(TIRgeoGroupID,latname,"units","degrees_north")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }

        //TIR Longitude
        if (Generate2D_Dataset(TIRgeoGroupID,lonname,h5_type,lon_tir_buffer,TIR_ImageLine_DimID,TIR_ImagePixel_DimID,nTIR_ImageLine,nTIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lon_tir_buffer); lon_tir_buffer = NULL;
        if(H5LTset_attribute_string(TIRgeoGroupID,lonname,"units","degrees_east")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }
        H5Dclose(TIR_ImageLine_DimID); TIR_ImageLine_DimID = 0;
        H5Dclose(TIR_ImagePixel_DimID); TIR_ImagePixel_DimID = 0;
    }

    // Possible VNIR
    if(VNIRgeoGroupID!=0)
    {

        VNIR_ImageLine_DimID = H5Dopen2(outputFileID,ll_vnir_dimnames[0],H5P_DEFAULT);
        nVNIR_ImageLine = obtainDimSize(VNIR_ImageLine_DimID);
        VNIR_ImagePixel_DimID = H5Dopen2(outputFileID,ll_vnir_dimnames[1],H5P_DEFAULT);
        nVNIR_ImagePixel = obtainDimSize(VNIR_ImagePixel_DimID);


        lat_vnir_buffer = (double*)malloc(sizeof(double)*nVNIR_ImageLine*nVNIR_ImagePixel);
        if(lat_vnir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lat_vnir_buffer.\n");
            goto cleanupFail;
        }

        lon_vnir_buffer = (double*)malloc(sizeof(double)*nVNIR_ImageLine*nVNIR_ImagePixel);
        if(lon_vnir_buffer == NULL)
        {
            FATAL_MSG("Cannot allocate lon_vnir_buffer.\n");
            goto cleanupFail;
        }

        asterLatLonSpherical(latBuffer,lonBuffer,lat_vnir_buffer,lon_vnir_buffer,nVNIR_ImageLine,nVNIR_ImagePixel);

        // VNIR Latitude
        if (Generate2D_Dataset(VNIRgeoGroupID,latname,h5_type,lat_vnir_buffer,VNIR_ImageLine_DimID,VNIR_ImagePixel_DimID,nVNIR_ImageLine,nVNIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lat_vnir_buffer); lat_vnir_buffer = NULL;
        if(H5LTset_attribute_string(VNIRgeoGroupID,latname,"units","degrees_north")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }

        //VNIR Longitude
        if (Generate2D_Dataset(VNIRgeoGroupID,lonname,h5_type,lon_vnir_buffer,VNIR_ImageLine_DimID,VNIR_ImagePixel_DimID,nVNIR_ImageLine,nVNIR_ImagePixel)<0)
        {
            FATAL_MSG("Cannot generate 2-D ASTER lat/lon.\n");
            goto cleanupFail;
        }
        free(lon_vnir_buffer); lon_vnir_buffer = NULL;
        if(H5LTset_attribute_string(VNIRgeoGroupID,lonname,"units","degrees_east")<0)
        {
            FATAL_MSG("Unable to insert ASTER latitude units attribute.\n");
            goto cleanupFail;
        }
        H5Dclose(VNIR_ImageLine_DimID); VNIR_ImageLine_DimID = 0;
        H5Dclose(VNIR_ImagePixel_DimID); VNIR_ImagePixel_DimID = 0;


    }
    
    if ( 0 )
    {
cleanupFail:
        retVal = 1;
    }

    for ( i = 0; i < 2; i++ )
        if ( ll_swir_dimnames[i] ) free(ll_swir_dimnames[i]);
    for ( i = 0; i < 2; i++ )
        if ( ll_tir_dimnames[i] ) free(ll_tir_dimnames[i]);
    for ( i = 0; i < 2; i++ )
        if ( ll_vnir_dimnames[i] ) free(ll_vnir_dimnames[i]);

    if ( latBuffer) free(latBuffer);
    if ( lonBuffer) free(lonBuffer);
    if ( VNIR_ImageLine_DimID ) H5Dclose(VNIR_ImageLine_DimID);
    if ( VNIR_ImagePixel_DimID ) H5Dclose(VNIR_ImagePixel_DimID);
    if ( lon_vnir_buffer ) free(lon_vnir_buffer);
    if ( lat_vnir_buffer) free(lat_vnir_buffer);
    if ( TIR_ImageLine_DimID) H5Dclose(TIR_ImageLine_DimID);
    if ( TIR_ImagePixel_DimID ) H5Dclose(TIR_ImagePixel_DimID);
    if ( lon_tir_buffer ) free(lon_tir_buffer);
    if ( lat_tir_buffer) free(lat_tir_buffer);
    if ( SWIR_ImageLine_DimID ) H5Dclose(SWIR_ImageLine_DimID);
    if ( SWIR_ImagePixel_DimID) H5Dclose(SWIR_ImagePixel_DimID);
    if ( lon_swir_buffer ) free(lon_swir_buffer);
    if ( lat_swir_buffer ) free(lat_swir_buffer);
    if ( retVal ) 
        return EXIT_FAILURE;
    return EXIT_SUCCESS;

}
