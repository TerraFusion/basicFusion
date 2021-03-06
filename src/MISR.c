#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <hdf.h>
#include <hdf5.h>
#include "libTERRA.h"

/* MY(Kent Yang myang6@hdfgroup.org) 2016-12-20, mostly re-write the handling of MISR */
float Obtain_scale_factor(int32 h4_file_id, char* band_name);
herr_t blockCentrTme( int32 inHFileID, hid_t BCTgroupID, hid_t dimGroupID );

/* May provide a list for all MISR group and variable names */

/*  MISR

 DESCRIPTION:
    This function handles the repackaging of the MISR files into the output HDF5 file.
    The input files to this function are specified in the fileList argument.

 ARGUMENTS:
    fileList[0]     -- Program name
    fileList[1]     -- AA file path
    fileList[2]     -- AF file path
    fileList[3]     -- AN file path
    fileList[4]     -- BA file path
    fileList[5]     -- BF file path
    fileList[6]     -- CA file path
    fileList[7]     -- CF file path
    fileList[8]     -- DA file path
    fileList[9]     -- DF file path
    fileList[10]    -- MISR AGP file path
    fileList[11]    -- MISR GP file path
    fileList[12]    -- MISR HRLL file path

    int unpack      -- Determines whether unpacking behavior is enabled.

 EFFECTS:
    Modifies the outputFile HDF5 file to contain the appropriate MISR data.
    Allocates memory as needed.

 RETURN:
    FAIL_OPEN       -- Failed to open a file ( NOT IMPLEMENTED IN THIS FUNCTION )
    FATAL_ERR       -- General error
    RET_SUCCESS     -- Success
*/
    
int MISR( char* fileList[],int unpack )
{
    /****************************************
     *      VARIABLES       *
     ****************************************/

    /* File IDs */

    char *camera_name[9]= {"AA","AF","AN","BA","BF","CA","CF","DA","DF"};
    char *band_name[4]= {"RedBand","BlueBand","GreenBand","NIRBand"};
    char *radiance_name[4]= {"Red Radiance/RDQI","Blue Radiance/RDQI","Green Radiance/RDQI","NIR Radiance/RDQI"};
    char *BRF_CF_name[4] ={"BlueConversionFactor","GreenConversionFactor","RedConversionFactor","NIRConversionFactor"};
    char *band_geom_name[36] =     {"AaAzimuth","AaGlitter","AaScatter","AaZenith",
                                    "AfAzimuth","AfGlitter","AfScatter","AfZenith",
                                    "AnAzimuth","AnGlitter","AnScatter","AnZenith",
                                    "BaAzimuth","BaGlitter","BaScatter","BaZenith",
                                    "BfAzimuth","BfGlitter","BfScatter","BfZenith",
                                    "CaAzimuth","CaGlitter","CaScatter","CaZenith",
                                    "CfAzimuth","CfGlitter","CfScatter","CfZenith",
                                    "DaAzimuth","DaGlitter","DaScatter","DaZenith",
                                    "DfAzimuth","DfGlitter","DfScatter","DfZenith"
                                   };

    char *solar_geom_name[2] = {"SolarAzimuth","SolarZenith"};
    char *geo_name[2] = {"GeoLatitude","GeoLongitude"};
    char *an_rad_dims[9] ={"SOMBlockDim_BlueBand","AN_XDim_BlueBand","AN_YDim_BlueBand",
                           "SOMBlockDim_GreenBand","AN_XDim_GreenBand","AN_YDim_GreenBand",
                           "SOMBlockDim_NIRBand","AN_XDim_NIRBand","AN_YDim_NIRBand"};

    char *BRF_gname="BRF_Conversion_Factors";
    char *solar_geom_gname="Solar_Geometry";
    char *geo_gname="Geolocation";
    char *hgeo_gname="HRGeolocation";
    char *data_gname="Data Fields";
    char *sensor_geom_gname ="Sensor_Geometry";
    char *misr_camera_miss ="MISR_AM1_GRP_MISS";
    char *misr_geom_miss ="MISR_AM1_GP_MISS";
    unsigned short misr_camera_miss_flag = 0;
    unsigned short misr_camera_miss_ID[9] ={0};
    unsigned short misr_geom_miss_flag = 0;
    int misr_1st_valid_index = 0;
    herr_t status = 0;
    int retVal = RET_SUCCESS;
    herr_t errStatus = 0;
    float tempFloat = 0.0;
    double tempDouble = 0.0;
    char* correctedName = NULL;
    char* fileTime = NULL;
    char* tmpCharPtr = NULL;
    char* granList = NULL;
    char* LRgeoLat = NULL;
    char* LRgeoLon = NULL;
    char* LRcoord = NULL;
    size_t granSize = 0;
    ssize_t pathSize = 0;
    int i;
    //int32 status32;
    //intn statusn;
    unsigned short has_LAI_DIM1 = 0;
    /******************
     * geo data files *
     ******************/
    /* File IDs */
    int32 h4FileID[9] = {0};
    int32 inHFileID[9] = {0};
    int32 h4_status = 0;
    int32 geoFileID = 0;
    int32 gmpFileID = 0;
    int32 hgeoFileID = 0;

    /* Group IDs */
    hid_t MISRrootGroupID = 0;
    hid_t geoGroupID = 0;
    hid_t hr_geoGroupID = 0;
    /* Dataset IDs */
    hid_t latitudeID = 0;
    hid_t longitudeID = 0;
    hid_t hr_latitudeID = 0;
    hid_t hr_longitudeID = 0;


    /* Group IDs */
    hid_t gmpSolarGeoGroupID = 0;

    hid_t solarAzimuthID = 0;
    hid_t solarZenithID = 0;

    hid_t h5CameraGroupID = 0;
    hid_t h5DataGroupID = 0;
    hid_t h5BRFGroupID = 0;
    hid_t h5SensorGeomGroupID = 0;

    hid_t h5DataFieldID = 0;
    hid_t h5BRFFieldID = 0;
    hid_t h5SensorGeomFieldID = 0;
    hid_t AnXdimDspaceID = 0;
    hid_t AnYdimDspaceID = 0;
    
    // Check any legal files that can be skipped
    for ( i = 1; i < 13; i++ )
    {

        if ( fileList[i] )
        {
            if(strncmp(fileList[i],misr_camera_miss,strlen(misr_camera_miss))==0){
                misr_camera_miss_flag=1;
                misr_camera_miss_ID[i-1]=1;
                continue;
            }
            else if(strncmp(fileList[i],misr_geom_miss,strlen(misr_geom_miss))==0){
                misr_geom_miss_flag =1;
                continue;
            }
            if(misr_1st_valid_index == 0)
                misr_1st_valid_index = i;
            tmpCharPtr = strrchr(fileList[i], '/');
            if ( tmpCharPtr == NULL )
            {
                FATAL_MSG("Failed to find a specific character within the string.\n");
                goto cleanupFail;
            }
            errStatus = updateGranList(&granList, tmpCharPtr+1, &granSize);
            if ( errStatus == FATAL_ERR )
            {
                FATAL_MSG("Failed to append granule to granule list.\n");
                goto cleanupFail;
            }

        }
    }



    /* Make an attempt to open all legal files. If any fail to open, skip
     * this entire granule.
     */
    short openFail = 0;

    geoFileID = SDstart( fileList[10], DFACC_READ );
    if ( geoFileID == -1 )
    {
        WARN_MSG("Failed to open MISR file.\n\t%s\n", fileList[10]);
        geoFileID = 0;
        openFail = 1;
    }

    if(strncmp(fileList[11],misr_geom_miss,strlen(misr_geom_miss))!=0) { 
    gmpFileID = SDstart( fileList[11], DFACC_READ );
    if ( gmpFileID == -1 )
    {
        WARN_MSG("Failed to open MISR file.\n\t%s\n", fileList[11]);
        gmpFileID = 0;
        openFail = 1;
    }
    }

    hgeoFileID = SDstart( fileList[12], DFACC_READ );
    if ( hgeoFileID == -1 )
    {
        WARN_MSG("Failed to open MISR file.\n\t%s\n", fileList[12]);
        hgeoFileID = 0;
        openFail = 1;
    }


    for ( i = 0; i < 9; i++ )
    { 
        if(misr_camera_miss_ID[i] == 1) 
            continue;
        h4FileID[i] = SDstart(fileList[i+1],DFACC_READ);
        if ( h4FileID[i] < 0 )
        {
            h4FileID[i] = 0;
            WARN_MSG("Failed to open MISR file.\n\t%s\n", fileList[i+1]);
            openFail = 1;
        }
        /*
        *           *    * Open the HDF file for reading.
        *                     *       */

        /* Need to use the H interface to obtain scale_factor */
        inHFileID[i] = Hopen(fileList[i+1],DFACC_READ, 0);
        if(inHFileID[i] <0)
        {
            inHFileID[i] = 0;
            WARN_MSG("Failed to start the H interface.\n");
            openFail = 1;
        }
    }

    // During the validation, we still make this a fatal error since this is unexpected.
    //if ( openFail ) goto cleanupFO;
    if ( openFail ) goto cleanupFail;

    createGroup( &outputFile, &MISRrootGroupID, "MISR" );
    if ( MISRrootGroupID == FATAL_ERR )
    {
        FATAL_MSG("Failed to create MISR root group.\n");
        return FATAL_ERR;
    }

    { // Comments for MISR descriptions.
        char *comment_name ="comment_for_fillvalue";
        char *comment_value = "The radiance value for a pixel is set to -999.0, if the value of its RDQI is 2 or 3"
                              " or if its original dn value is either 16378 or 16380. ";

        char *la_comment_name ="comment_for_low_accuracy_index";
        char *la_comment_value = "When RDQI for a pixel is 1, its radiance value will be flagged as the 'low accuracy'. The "
                              "positions of these low accuracy radiance measurements are indexed and reported in a two dimensional "
                              "array for each radiance band.  The first dimension of the array represents the number of reduced "
                              "accuracy pixels, while the second dimension gives their indexed coordinates in the order of block,"
                              " block-relative line and block-relative sample. For example, if the second dimension for a low accuracy pixel"
                              " in the array contains the values of (57,9,316), the location of the pixel is block 57, line 9 and sample 316." ;


        char *time_comment_name="comment_for_GranuleTime";
        char *time_comment_value = "The attribute GranuleTime is represented by orbit numbers. For example, the value of 040110 "
                                   "indicates the data was acquired for orbit 40110.";
                          

        if(H5LTset_attribute_string(outputFile,"MISR",comment_name,comment_value) <0){
            FATAL_MSG("Failed to add the MISR comment attribute.\n");
            goto cleanupFail;
        }

        if(H5LTset_attribute_string(outputFile,"MISR",la_comment_name,la_comment_value) <0){
            FATAL_MSG("Failed to add the MISR comment attribute.\n");
            goto cleanupFail;
        }


        if(H5LTset_attribute_string(outputFile,"MISR",time_comment_name,time_comment_value) <0){
            FATAL_MSG("Failed to add the MISR time comment attribute.\n");
            goto cleanupFail;
        }
 
    }
    if(H5LTset_attribute_string(outputFile,"MISR","GranuleName",granList)<0)
    {
        FATAL_MSG("Cannot add granule list.\n");
        goto cleanupFail;
    }

    if(misr_1st_valid_index > 9) {
        FATAL_MSG("No any MISR radiance fields in this orbit. \n");
        goto cleanupFail;
    }
    else {
    // Extract the time substring from the file path
    fileTime = getTime( fileList[1], 4 );
    if(H5LTset_attribute_string(outputFile,"MISR","GranuleTime",fileTime)<0)
    {
        FATAL_MSG("Cannot add the time stamp\n");
        goto cleanupFail;
    }
    free(fileTime);
    fileTime = NULL;
    }


    /************************
     * GEOLOCATION DATASETS *
     ************************/

    
    createGroup( &MISRrootGroupID, &geoGroupID, geo_gname );
    if ( geoGroupID == FATAL_ERR )
    {
        FATAL_MSG("Failed to create HDF5 group.\n");
        geoGroupID = 0;
        goto cleanupFail;
    }


    latitudeID  = readThenWrite( NULL,geoGroupID,geo_name[0],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID,1);
    if ( latitudeID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (latitude dataset).\n");
        latitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[0]);
    
    /* Save the path of the latitude dataset */
    pathSize = H5Iget_name( latitudeID, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the latitude path name.\n");
        goto cleanupFail;
    }
    LRgeoLat = calloc(pathSize+1, 1 );
    if ( LRgeoLat == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(latitudeID, LRgeoLat, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve latitude path name.\n");
        goto cleanupFail;
    }

    errStatus = H5LTset_attribute_string(geoGroupID,correctedName,"units","degrees_north");
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    // Copy over the dimensions
    errStatus = copyDimension( NULL, geoFileID, geo_name[0], outputFile, latitudeID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }


    free(correctedName);
    correctedName = NULL;

    longitudeID = readThenWrite( NULL,geoGroupID,geo_name[1],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID,1);
    if ( longitudeID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (longitude dataset).\n");
        longitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[1]);
    
    /* Save the path of the longitude dataset */
    pathSize = H5Iget_name( longitudeID, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the longitude path name.\n");
        goto cleanupFail;
    }
    LRgeoLon = calloc(pathSize+1, 1 );
    if ( LRgeoLat == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(longitudeID, LRgeoLon, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve longitude path name.\n");
        goto cleanupFail;
    }

    errStatus = H5LTset_attribute_string(geoGroupID,(const char*) correctedName,"units","degrees_east");
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    // Copy over the dimensions
    errStatus = copyDimension( NULL, geoFileID, geo_name[1], outputFile, longitudeID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    free(correctedName);
    correctedName = NULL;

    //HR latlon
    createGroup( &MISRrootGroupID, &hr_geoGroupID, hgeo_gname );
    if ( hr_geoGroupID == FATAL_ERR )
    {
        FATAL_MSG("Failed to create HDF5 group.\n");
        hr_geoGroupID = 0;
        goto cleanupFail;
    }


    hr_latitudeID  = readThenWrite( NULL,hr_geoGroupID,geo_name[0],DFNT_FLOAT32,H5T_NATIVE_FLOAT,hgeoFileID,1);
    if ( hr_latitudeID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (latitude dataset).\n");
        hr_latitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[0]);
    errStatus = H5LTset_attribute_string(hr_geoGroupID,correctedName,"units","degrees_north");
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    // Copy over the dimensions
    errStatus = copyDimension( NULL, hgeoFileID, geo_name[0], outputFile, hr_latitudeID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }


    free(correctedName);
    correctedName = NULL;

    hr_longitudeID = readThenWrite( NULL,hr_geoGroupID,geo_name[1],DFNT_FLOAT32,H5T_NATIVE_FLOAT,hgeoFileID,1);
    if ( hr_longitudeID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (longitude dataset).\n");
        hr_longitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[1]);
    errStatus = H5LTset_attribute_string(geoGroupID,(const char*) correctedName,"units","degrees_east");
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    // Copy over the dimensions
    errStatus = copyDimension( NULL, hgeoFileID, geo_name[1], outputFile, hr_longitudeID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    free(correctedName);
    correctedName = NULL;

    if(misr_geom_miss_flag !=1) {
    createGroup( &MISRrootGroupID, &gmpSolarGeoGroupID, solar_geom_gname );
    if ( gmpSolarGeoGroupID == FATAL_ERR )
    {
        FATAL_MSG("Failed to create HDF5 group.\n");
        gmpSolarGeoGroupID = 0;
        goto cleanupFail;
    }

    solarAzimuthID = readThenWrite( NULL,gmpSolarGeoGroupID,solar_geom_name[0],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID,0);
    if ( solarAzimuthID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (solarAzimuth dataset).\n");
        solarAzimuthID = 0;
        goto cleanupFail;
    }
    // Copy over the dimensions
    errStatus = copyDimension( NULL, gmpFileID, solar_geom_name[0], outputFile, solarAzimuthID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    // Read the value of the _FillValue attribute in the SolarAzimuth dataset
    tempDouble = 0.0;
    errStatus = H4readSDSAttr( gmpFileID, solar_geom_name[0], "_FillValue", (void*) &tempDouble );
    if ( errStatus != RET_SUCCESS )
    {
        FATAL_MSG("Failed to read HDF4 attribute.\n");
        goto cleanupFail;
    }

    correctedName = correct_name(solar_geom_name[0]);
    // write this to the corresponding HDF5 dataset as an attribute
    errStatus = H5LTset_attribute_double( gmpSolarGeoGroupID, correctedName,"_Fillvalue",&tempDouble,1);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to write an HDF5 attribute.\n");
        goto cleanupFail;
    }

    free(correctedName);
    correctedName = NULL;

    solarZenithID = readThenWrite( NULL,gmpSolarGeoGroupID,solar_geom_name[1],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID,0);
    if ( solarZenithID == FATAL_ERR )
    {
        FATAL_MSG("MISR readThenWrite function failed (solarZenith dataset).\n");
        solarZenithID = 0;
        goto cleanupFail;
    }

    // Copy over the dimensions
    errStatus = copyDimension( NULL, gmpFileID, solar_geom_name[1], outputFile, solarZenithID);
    if ( errStatus == FAIL )
    {
        FATAL_MSG("Failed to copy dimensions.\n");
        goto cleanupFail;
    }

    correctedName = correct_name(solar_geom_name[1]);
    // write the same tempDouble value into the solarZenith dataset
    errStatus = H5LTset_attribute_double( gmpSolarGeoGroupID, correctedName,"_Fillvalue",&tempDouble,1);
    if ( errStatus < 0 )
    {
        FATAL_MSG("Failed to write an HDF5 attribute.\n");
        goto cleanupFail;
    }

    free(correctedName);
    correctedName = NULL;

    }

    /* Concatenate the geolocation paths into one string */
    LRcoord = calloc( strlen(LRgeoLon) + strlen(LRgeoLat) + 2, 1);
    strcpy(LRcoord,LRgeoLon);
    strcat(LRcoord, " ");
    strcat(LRcoord, LRgeoLat);

    free(LRgeoLon); LRgeoLon = NULL;
    free(LRgeoLat); LRgeoLat = NULL;

    /************************************************************
     *                  RADIANCE DATASETS                       *
     ************************************************************/

     //AN Dimension sizes are always 180*512*2048. So we need to handle the dimensions differently.
     // Create data spaces for the dimensions.
     hsize_t an_xdim_dsize = 512;
     hsize_t an_ydim_dsize = 2048;
     AnXdimDspaceID  = H5Screate_simple(1,&an_xdim_dsize,NULL);
     AnYdimDspaceID  = H5Screate_simple(1,&an_ydim_dsize,NULL);


    /* Loop all 9 cameras */
    for( i = 0; i<9; i++)
    {

        // Skip the missing cameras
        if(misr_camera_miss_ID[i]==1) 
            continue;
        /*
         *          *    * Initialize the V interface.
         *                   *       */
        h4_status = Vstart (inHFileID[i]);
        if (h4_status < 0)
        {
            FATAL_MSG("Failed to start the V interface.\n");
            goto cleanupFail;
        }

        createGroup(&MISRrootGroupID, &h5CameraGroupID, camera_name[i]);
        if ( h5CameraGroupID == FATAL_ERR )
        {
            h5CameraGroupID = 0;
            FATAL_MSG("Failed to create an HDF5 group.\n");
            goto cleanupFail;
        }

        createGroup(&h5CameraGroupID,&h5DataGroupID,data_gname);
        if ( h5DataGroupID == FATAL_ERR )
        {
            h5DataGroupID = 0;
            FATAL_MSG("Failed to create an HDF5 group.\n");
            goto cleanupFail;
        }

        /* Need to get all the four band data */
        for (int j = 0; j<4; j++)
        {

            // If we choose to unpack the data.
            if(unpack == 1)
            {

                float scale_factor = -1.;
                scale_factor = Obtain_scale_factor(inHFileID[i],band_name[j]);
                if ( scale_factor < 0.0 )
                {
                    FATAL_MSG("Failed to obtain scale factor for MISR.\n)");
                    goto cleanupFail;
                }
                if(has_LAI_DIM1 == 0) {
                    has_LAI_DIM1 = H5Lexists(outputFile,"/MISR_LA_POS_DIM",H5P_DEFAULT);
                    if(has_LAI_DIM1 <0) {
                        FATAL_MSG("Failed to check MISR Low Accuracy Position dimension .\n)");
                        goto cleanupFail;
                    }
                    
                }
                // Pass LAI_DIM1
                h5DataFieldID =  readThenWrite_MISR_Unpack( h5DataGroupID, camera_name[i],radiance_name[j],  &correctedName,DFNT_UINT16,
                                 h4FileID[i],scale_factor,&has_LAI_DIM1);
                if ( h5DataFieldID == FATAL_ERR )
                {
                    FATAL_MSG("MISR readThenWrite Unpacking function failed.\n");
                    h5DataFieldID = 0;
                    goto cleanupFail;
                }


            }
            else
            {
                //correctedName = correct_name(radiance_name[j]);

                h5DataFieldID =  readThenWrite( NULL, h5DataGroupID, radiance_name[j], DFNT_UINT16,
                                                H5T_NATIVE_USHORT,h4FileID[i],1);
                if ( h5DataFieldID == FATAL_ERR )
                {
                    h5DataFieldID = 0;
                    FATAL_MSG("MISR readThenWrite function failed.\n");
                    goto cleanupFail;
                }
            }



            tempFloat = -999.0f;
            if ( correctedName == NULL )
                correctedName = correct_name(radiance_name[j]);

          if(unpack == 1) {
            status = H5LTset_attribute_string( h5DataGroupID, correctedName, "coordinates", LRcoord );
            if ( status < 0 )
            {
                FATAL_MSG("Failed to set coordinates attribute.\n");
                goto cleanupFail;
            }
            // Set the units attribute
            status = H5LTset_attribute_string( h5DataGroupID, correctedName, "units", "Watts/m^2/micrometer/steradian");
            if ( status < 0 )
            {
                FATAL_MSG("Failed to set string attribute. i = %d j = %d\n", i, j);
                goto cleanupFail;
            }

            errStatus = H5LTset_attribute_float( h5DataGroupID, correctedName,"_FillValue",&tempFloat,1);

            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write the radiance_FillValue attribute.\n");
                goto cleanupFail;
            }

            tempFloat = 0.0;

            errStatus = H5LTset_attribute_float( h5DataGroupID, correctedName,"valid_min",&tempFloat,1);

            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write the radiance valid_min attribute.\n");
                goto cleanupFail;
            }

            tempFloat = 800.0;

            errStatus = H5LTset_attribute_float( h5DataGroupID, correctedName,"valid_max",&tempFloat,1);

            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write the radiance valid_max attribute.\n");
                goto cleanupFail;
            }
        }

            // Copy over the dimensions
            // AN Band GR,BR and NIR are all 180*512*2048 
            // i =2 is the AN band, we only need to change Blue,Green and NIR dimension names, that is j >0)
            if(i==2 && j>0){ 

                // We have to create our own dimensions. Here just use the hard-coded dimensions to avoid a massive dimension retrieval routine calls.
                // No need to create the the first dimension since the block number is always 180
                if(attachDimension(outputFile,an_rad_dims[(j-1)*3],h5DataFieldID,0)!=RET_SUCCESS) {
                   FATAL_MSG("Error attaching MISR AN SOM dimenson.  \n");
                   goto cleanupFail;
                }
                hid_t an_xdim_id = 0;
                if(makePureDim(outputFile,an_rad_dims[(j-1)*3+1],AnXdimDspaceID,H5T_NATIVE_INT,&an_xdim_id)!=RET_SUCCESS) {
                   FATAL_MSG("Error creating MISR AN X dimenson.  \n");
                   goto cleanupFail;
                }
                H5Dclose(an_xdim_id);
                if(attachDimension(outputFile,an_rad_dims[(j-1)*3+1],h5DataFieldID,1)!=RET_SUCCESS) {
                   FATAL_MSG("Error attaching MISR AN X dimenson.  \n");
                   goto cleanupFail;
                }

                hid_t an_ydim_id = 0;
                if(makePureDim(outputFile,an_rad_dims[(j-1)*3+2],AnYdimDspaceID,H5T_NATIVE_INT,&an_ydim_id)!=RET_SUCCESS) {
                   FATAL_MSG("Error creating MISR AN X dimenson.  \n");
                   goto cleanupFail;
                }
                H5Dclose(an_ydim_id);
                if(attachDimension(outputFile,an_rad_dims[(j-1)*3+2],h5DataFieldID,2)!=RET_SUCCESS) {
                   FATAL_MSG("Error attaching MISR AN X dimenson.  \n");
                   goto cleanupFail;
                }

            }
            else {
                errStatus = copyDimension( NULL, h4FileID[i], radiance_name[j], outputFile, h5DataFieldID);
                if ( errStatus == FAIL )
                {
                    FATAL_MSG("Failed to copy dimensions.\n");
                    goto cleanupFail;
                }
            }

            H5Dclose(h5DataFieldID);
            h5DataFieldID = 0;
            if(correctedName!=NULL)
                free(correctedName);
            correctedName = NULL;
        } // End for (first inner j loop)

        createGroup(&h5CameraGroupID,&h5BRFGroupID,BRF_gname);
        if ( h5BRFGroupID == FATAL_ERR )
        {
            h5BRFGroupID = 0;
            FATAL_MSG("Failed to create an HDF5 group.\n");
            goto cleanupFail;
        }

        /* Inserting the "BlueConversionFactor... etc. */
        for (int j = 0; j<4; j++)
        {
            h5BRFFieldID = readThenWrite( NULL,h5BRFGroupID,BRF_CF_name[j],DFNT_FLOAT32,
                                                 H5T_NATIVE_FLOAT,h4FileID[i],0);
            if ( h5BRFFieldID == FATAL_ERR )
            {
                FATAL_MSG("MISR readThenWrite function failed.\n");
                h5BRFFieldID = 0;
                goto cleanupFail;
            }

            // Copy over the dimensions
            errStatus = copyDimension( NULL, h4FileID[i], BRF_CF_name[j], outputFile, h5BRFFieldID);
            if ( errStatus == FAIL )
            {
                FATAL_MSG("Failed to copy dimensions.\n");
                goto cleanupFail;
            }

            tempFloat = -555.0;
            //correctedName = correct_name(band_geom_name[i*4+j]);
            errStatus = H5LTset_attribute_float( h5BRFGroupID, BRF_CF_name[j],"_Fillvalue",&tempFloat,1);
            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write an HDF5 attribute.\n");
                goto cleanupFail;
            }
            status = H5Dclose(h5BRFFieldID);
            h5BRFFieldID = 0;
        }
        if(misr_geom_miss_flag !=1) {
        createGroup(&h5CameraGroupID,&h5SensorGeomGroupID,sensor_geom_gname);
        if ( h5SensorGeomGroupID == FATAL_ERR )
        {
            FATAL_MSG("Failed to create an HDF5 group.\n");
            h5SensorGeomGroupID = 0;
            goto cleanupFail;
        }

        /* Inserting the "AaAzimuth", "AaGlitter", "AaScatter"... etc. */
        for (int j = 0; j<4; j++)
        {
            h5SensorGeomFieldID = readThenWrite( NULL,h5SensorGeomGroupID,band_geom_name[i*4+j],DFNT_FLOAT64,
                                                 H5T_NATIVE_DOUBLE,gmpFileID,0);
            if ( h5SensorGeomFieldID == FATAL_ERR )
            {
                FATAL_MSG("MISR readThenWrite function failed.\n");
                h5SensorGeomFieldID = 0;
                goto cleanupFail;
            }

            // Copy over the dimensions
            errStatus = copyDimension( NULL, gmpFileID, band_geom_name[i*4+j], outputFile, h5SensorGeomFieldID);
            if ( errStatus == FAIL )
            {
                FATAL_MSG("Failed to copy dimensions.\n");
                goto cleanupFail;
            }

            tempDouble = -555.0;
            correctedName = correct_name(band_geom_name[i*4+j]);
            errStatus = H5LTset_attribute_double( h5SensorGeomGroupID, correctedName,"_Fillvalue",&tempDouble,1);
            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write an HDF5 attribute.\n");
                goto cleanupFail;
            }
            errStatus = H5LTset_attribute_string ( h5SensorGeomGroupID, correctedName, "units", "degree" );
            if ( errStatus < 0 )
            {
                FATAL_MSG("Failed to write an HDF5 attribute.\n");
                goto cleanupFail;
            }

            status = H5Dclose(h5SensorGeomFieldID);
            h5SensorGeomFieldID = 0;
            if ( status ) WARN_MSG("H5Dclose\n");
            free(correctedName);
            correctedName = NULL;


        } // End for (second inner j loop)
        status = H5Gclose(h5SensorGeomGroupID);
        h5SensorGeomGroupID = 0;
 
        }//If GMP file exists


        /******************************************************/
        /* Insert the "perBlockMetadataTime" into output file */
        /******************************************************/
        status = blockCentrTme( inHFileID[i], h5CameraGroupID, outputFile);    
        if ( status == FATAL_ERR )
        {
            FATAL_MSG("Failed to create the BlockCenterTime dataset.\n");
            goto cleanupFail;
        }
#if 0
        for ( int x = 0; x < n_read; x++ )
        {
            for ( int y = 0; y < 28; y++ )
                printf("%d: %c\n", x*28+y, (char)(perBlockMetaBuf[x*28 + y]) );
        }
#endif 

        Vend(inHFileID[i]);
        Hclose(inHFileID[i]);
        inHFileID[i] = 0;
        SDend(h4FileID[i]);
        h4FileID[i] = 0;
        status = H5Gclose(h5DataGroupID);
        h5DataGroupID = 0;
        status = H5Gclose(h5BRFGroupID);
        h5BRFGroupID = 0;
        //status = H5Gclose(h5SensorGeomGroupID);
        //h5SensorGeomGroupID = 0;
        status = H5Gclose(h5CameraGroupID);
        h5CameraGroupID = 0;
    } // end loop all cameras


    if ( 0 )
    {
cleanupFail:
        retVal = FATAL_ERR;
    }

    if ( 0 )
    {
cleanupFO:
        retVal = FAIL_OPEN;
    }



    if (MISRrootGroupID)        H5Gclose(MISRrootGroupID);
    if ( geoFileID )            SDend(geoFileID);
    if ( hgeoFileID )           SDend(hgeoFileID);
    if ( gmpFileID )            SDend(gmpFileID);

    for ( i = 0; i < 9; i++ )
    { 
        if ( h4FileID[i] )             SDend(h4FileID[i]);
        Vend(inHFileID[i]);
        if ( inHFileID[i] )     Hclose(inHFileID[i]);
    }
    if ( h5CameraGroupID )      H5Gclose(h5CameraGroupID);
    if ( h5DataGroupID )        H5Gclose(h5DataGroupID);
    if ( h5BRFGroupID)          H5Gclose(h5BRFGroupID);
    if ( h5DataFieldID )        H5Dclose(h5DataFieldID);
    if ( h5SensorGeomGroupID )  H5Gclose(h5SensorGeomGroupID);
    if ( h5SensorGeomFieldID )  H5Dclose(h5SensorGeomFieldID);
    if ( geoGroupID )           H5Gclose(geoGroupID);
    if ( latitudeID )           H5Dclose(latitudeID);
    if ( longitudeID )          H5Dclose(longitudeID);
    if ( hr_geoGroupID )        H5Gclose(hr_geoGroupID);
    if ( hr_latitudeID )        H5Dclose(hr_latitudeID);
    if ( hr_longitudeID )       H5Dclose(hr_longitudeID);
    if ( gmpSolarGeoGroupID )   H5Gclose(gmpSolarGeoGroupID);
    if ( solarAzimuthID )       H5Dclose(solarAzimuthID);
    if ( solarZenithID )        H5Dclose(solarZenithID);
    if (AnXdimDspaceID)         H5Sclose(AnXdimDspaceID);
    if (AnYdimDspaceID)         H5Sclose(AnYdimDspaceID);
    if ( correctedName )        free(correctedName);
    if ( fileTime )             free(fileTime);
    if ( granList )             free(granList);
    if ( LRgeoLat )             free(LRgeoLat);
    if ( LRgeoLon )             free(LRgeoLon);
    if ( LRcoord )              free(LRcoord);
    return retVal;
}


float Obtain_scale_factor(int32 h4_file_id, char* band_name)
{

    int32 band_group_ref = -1;
    int32 sub_group_ref = -1;
    int32 sub_group_tag = -1;
    int32 sub_group_obj_ref = -1;
    int32 sub_group_obj_tag = -1;

    double sc = 0;
    int num_gobjects = 0;
    int32 band_group_id  = 0;
    int32 sub_group_id =0;
    char* sub_group_name = NULL;
    char* grid_attr_group_name = "Grid Attributes";
    char* scale_factor_name = "Scale factor";
    uint16 name_len = 0;

    int32 status = -1;

    /* Obtain the vgroup reference number of this band */
    band_group_ref = H4ObtainLoneVgroupRef(h4_file_id,band_name);
    assert(band_group_ref >0);


    band_group_id = Vattach(h4_file_id, band_group_ref, "r");
    num_gobjects = Vntagrefs(band_group_id);
    assert(num_gobjects >0);


    for (int i = 0; i < num_gobjects; i++)
    {

        if (Vgettagref (band_group_id, i, &sub_group_tag, &sub_group_ref) == FAIL)
        {
            FATAL_MSG("Vgetagref failed.\n");
            Vdetach (band_group_id);
            return -1.0;
        }

        if (Visvg (band_group_id, sub_group_ref) == TRUE)
        {

            sub_group_id = Vattach(h4_file_id, sub_group_ref, "r");
            status = Vgetnamelen(sub_group_id, &name_len);
            sub_group_name = (char *) HDmalloc(sizeof(char *) * (name_len+1));
            if (sub_group_name == NULL)
            {
                FATAL_MSG("Not enough memory for vgroup_name!\n");
                Vdetach(band_group_id);
                return -1.0;
            }
            status = Vgetname (sub_group_id, sub_group_name);
            if(status == FAIL)
            {
                FATAL_MSG("Vgetname failed !\n");
                Vdetach(sub_group_id);
                Vdetach(band_group_id);
                free(sub_group_name);
                return -1.0;
            }
            if(strncmp(sub_group_name,grid_attr_group_name,strlen(grid_attr_group_name))==0)
            {

                int num_sgobjects = Vntagrefs(sub_group_id);
                assert(num_sgobjects >0);

                for(int j = 0; j<num_sgobjects; j++)
                {
                    if (Vgettagref (sub_group_id, j, &sub_group_obj_tag, &sub_group_obj_ref) == FAIL)
                    {
                        FATAL_MSG("Vgetagref failed.\n");
                        Vdetach (sub_group_id);
                        Vdetach(band_group_id);
                        free(sub_group_name);
                        return -1.0;
                    }

                    if(Visvs(sub_group_id,sub_group_obj_ref)==TRUE)
                    {

                        char vdata_name[VSNAMELENMAX];
                        int32 vdata_id = -1;

                        vdata_id = VSattach (h4_file_id, sub_group_obj_ref, "r");
                        if (vdata_id == FAIL)
                        {
                            FATAL_MSG("VSattach failed.\n");
                            Vdetach (sub_group_id);
                            Vdetach(band_group_id);
                            free(sub_group_name);
                            return -1.0;
                        }

                        if (VSgetname (vdata_id, vdata_name) == FAIL)
                        {
                            VSdetach (vdata_id);
                            Vdetach (sub_group_id);
                            Vdetach(band_group_id);
                            free(sub_group_name);
                            FATAL_MSG("VSgetname failed.\n");
                            return -1.0;
                        }

                        if(strncmp(scale_factor_name,vdata_name,strlen(scale_factor_name)) == 0)
                        {

                            int32 fieldsize=-1;
                            int32 nelms = -1;
                            int32 fieldtype=-1;

                            // Obtain field size
                            fieldsize = VFfieldesize (vdata_id, 0);
                            if (fieldsize == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VFfieldesize failed.\n");
                                return -1.0;
                            }
                            fieldtype = VFfieldtype(vdata_id,0);
                            if(fieldtype == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VFfieldesize failed.\n");
                                return -1.0;
                            }

                            // Obtain number of elements
                            nelms = VSelts (vdata_id);
                            if (nelms == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VSelts failed.\n");
                                return -1.0;
                            }

                            // Initialize the seeking process
                            if (VSseek (vdata_id, 0) == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VSseek failed.\n");
                                return -1.0;
                            }

// The field to seek is CERE_META_FIELD_NAME
                            if (VSsetfields (vdata_id, "AttrValues") == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VSsetfields failed.\n");
                                return -1.0;
                            }

                            // Read this vdata field value
                            if (VSread(vdata_id, (uint8 *) &sc, 1,FULL_INTERLACE)
                                    == FAIL)
                            {
                                VSdetach (vdata_id);
                                Vdetach (sub_group_id);
                                Vdetach(band_group_id);
                                free(sub_group_name);
                                FATAL_MSG("VSread failed.\n");
                                return -1.0;
                            }

                        } // end if
                        VSdetach(vdata_id);

                    } // end if

                } // end for

                free(sub_group_name);
                Vdetach(sub_group_id);
                break;
            } // end if

            free(sub_group_name);
            Vdetach(sub_group_id);

        } // end if

    } // end for
    Vdetach(band_group_id);

    return (float)sc;
}

/*
        blockCentrTme()

 DESCRIPTION:
    This function copies the Vdata "BlockCenterTime" inside the MISR metadata "PerBlockMetadataTime" from the input file
    inSDID and copies it over to the HDF5 groupID group. It also creates the appropriate dimension for the new dataset
    and attaches the dimension to the dataset. Both the dimension and the dataset created will be of size 180. Fill values
    will be inserted into the BlockCenterTime for blocks that do not contain valid data. Information on which blocks are
    valid come from the "Start_block" and "End_block" attributes in the root HDF object of the input MISR granules.

    The attributes "units" and "fill_value" are added to the BlockCenterTime dataset.

 ARGUMENTS:
    IN
        int32 inHFileID     -- The input HDF4 H identifier where the BlockCenterTime Vdata will be found
        hid_t BCTgroupID    -- Where the output BlockCenterTime dataset will go
        hid_t dimGroupID    -- Where the corresponding dimension will be placed (if it does not already exist)

 EFFECTS:
    Adds 1 dataset and 1 dimension to the output HDF5 file "outputFile" (which is a global variable)

 RETURN:
    RET_SUCCESS
    FATAL_ERR
*/
herr_t blockCentrTme( int32 inHFileID, hid_t BCTgroupID, hid_t dimGroupID )
{
    
    const char* perBlockMet = "PerBlockMetadataTime";
    const char* blockCent   = "BlockCenterTime";
    int32* vdata_size = NULL; 
    int32 vdataID = 0;
    char* perBlockMetaBuf = NULL;
    herr_t retVal = RET_SUCCESS;
    hid_t dimID = 0;
    intn statusn = 0;
    hid_t stringType = 0;
    hid_t perBlockMetaDspace = 0;
    herr_t status = 0;
    hid_t perBlockMetaDset = 0;
    //int *dimBuf = NULL;
    hid_t dSpaceID = 0;
    const char* dimName = "SOMBlock_Time";
    const hsize_t BCTsize = 180;
    long startBlock = 0;
    long endBlock = 0;
    char* BCTbuf = NULL;
    const char *fillVal = "0000-00-00T00:00:00.000000Z";
    int32 inSDID = 0;

    // Get the filename
    char *fileName = NULL;
    intn dummy1;
    intn dummy2;
    statusn = Hfidinquire( inHFileID, &fileName, &dummy1, &dummy2 );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to inquire the HDF4 filename.\n");
        goto cleanupFail;
    }

    // Start the SD interface
    inSDID = SDstart( fileName, DFACC_READ );
    if ( inSDID == FAIL )
    {
        FATAL_MSG("Failed to start the SD interface.\n");
        inSDID = 0;
        goto cleanupFail;
    }

    // Get the vdata reference number
    int32 vdataRef = VSfind( inHFileID, perBlockMet );
    if ( vdataRef == 0 )
    {
        FATAL_MSG("Failed to find the reference number for %s.\n", perBlockMet ); 
        goto cleanupFail;
    }
    
    vdataID = VSattach( inHFileID, vdataRef, "r" );
    if ( vdataID == FAIL )
    {
        FATAL_MSG("Failed to attach to the vdataset %s.\n", perBlockMet); 
        goto cleanupFail;
    }

    // Find how many records are in this vdata
    int32 n_records;
    statusn = VSinquire( vdataID, &n_records, NULL, NULL, NULL, NULL );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to retrieve information about Vdata %s.\n", perBlockMet);
        goto cleanupFail;
    }
    vdata_size = calloc ( n_records, sizeof(int32) );

    /* Find the size of each record */
    statusn = VSinquire( vdataID, NULL, NULL, NULL, vdata_size, NULL );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to retrieve information about Vdata %s.\n", perBlockMet);
        goto cleanupFail;
    }


    /* Allocate the perBlockMetaBuf. We can safely assume the size of all the records is the size
       of the first record ( vdata_size[0] )
     */
    int perBMB_size = n_records * vdata_size[0];
    perBlockMetaBuf = calloc ( perBMB_size, sizeof(char));

    // Set the fields for reading (there is only one field name)
    // blockCent = "BlockCenterTime"
    statusn = VSsetfields( vdataID, blockCent );    
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to set the V fields for reading.\n");
        goto cleanupFail;
    }

    // Read the data
    int32 n_read = VSread( vdataID, (uint8*) perBlockMetaBuf, n_records, FULL_INTERLACE );
    if ( n_read == FAIL )
    {
        FATAL_MSG("Failed to read the V data %s.\n", perBlockMet);
        goto cleanupFail;
    }

    /* perBlockMetaBuf now contains the vdata. They are a contiguous array of vdata_size[0] number of null-terminated
       strings. Even empty entries into the vdataset are of size vdata_size[0].
     */
    
    // First need to create the string datatype
    // copy the atomic 1-character string type
    stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        FATAL_MSG("Failed to copy the datatype.\n");
        stringType = 0;
        goto cleanupFail;
    }
    // Set the length to vdata_size[0]
    status = H5Tset_size( stringType, (size_t) vdata_size[0] );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set the size of the string datatype.\n");
        goto cleanupFail;
    }            

    // Create the dataspace for BlockCenterTime

    perBlockMetaDspace = H5Screate_simple( 1, &BCTsize, NULL );
    if ( perBlockMetaDspace < 0 )
    {
        perBlockMetaDspace = 0;
        FATAL_MSG("Failed to create the dataspace.\n");
        goto cleanupFail;
    }

    perBlockMetaDset = H5Dcreate2(BCTgroupID, blockCent, stringType, perBlockMetaDspace, H5P_DEFAULT,
                                 H5P_DEFAULT, H5P_DEFAULT );
    if ( perBlockMetaDset < 0 )
    {
        perBlockMetaDset = 0;
        FATAL_MSG("Failed to create the dataset.\n");
        goto cleanupFail;
    }

    // Set the attributes for the BlockCenterTime dataset
    status = H5LTset_attribute_string( BCTgroupID, blockCent, "units", "UTC" );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set attribute.\n");
        goto cleanupFail;
    }
    status = H5LTset_attribute_string( BCTgroupID, blockCent, "_FillValue", fillVal );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set attribute.\n");
        goto cleanupFail;
    }


    /* The HDF5 dataset has been created, and we have read the BlockCenterTime into a buffer. There is now a mismatch
       of size... the perBlockMetaDset is of size BCTsize, and buffer is of size n_records. We need to move perBlockMetaBuf
       to a new buffer of size BCTsize, adding the fill values as required. The elements that will be filled are determined
       by the Start_block and End_block attributes found in the root HDF4 object of the MISR input file.
    */
    BCTbuf = calloc ( BCTsize * vdata_size[0], sizeof(char) );

    /* BCTbuf is allocated, so now retrieve the Start_block and End_block attributes from the input MISR file */
    // Start_block
    int32 startAttrIdx = SDfindattr( inSDID, "Start_block");
    if ( startAttrIdx == FAIL )
    {
        FATAL_MSG("Failed to find the Start_block attribute.\n");
        goto cleanupFail;
    }
    statusn = SDreadattr( inSDID, startAttrIdx, &startBlock );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to read Start_block attribute.\n");
        goto cleanupFail;
    }

    // End_block (note: End_block is actually named "End block" in MISR file, with no underscore. This is strange?!)
    int32 endAttrIdx = SDfindattr( inSDID, "End block");
    if ( endAttrIdx == FAIL )
    {
        FATAL_MSG("Failed to find the End_block attribute.\n");
        goto cleanupFail;
    }
    statusn = SDreadattr( inSDID, endAttrIdx, &endBlock );
    if ( statusn == FAIL )
    {
        FATAL_MSG("Failed to read End_block attribute.\n");
        goto cleanupFail;
    }

    startBlock--;
    endBlock--;

    // Add the fill values to all of the non-valid blocks
    int i;
    for ( i = 0; i < 180; i++ )
    {
        if ( i < startBlock || i > endBlock )
            strncpy( &(BCTbuf[ i * vdata_size[0]]), fillVal, vdata_size[0] - 1 );

        /* TODO
            Landon Clipp Aug 18 2017
            I don't like this else statement. The boundaries seem too error-prone. Perhaps rework to make it more robust.
            This code would fail if the input MISR Start_block and End_block values are incorrect.
        */

        else if ( i == startBlock )
        {
            // Find the first valid entry in perBlockMetaBuf
            int j;
            for ( j = 0; j < n_records * vdata_size[0]; j++ )
                if ( perBlockMetaBuf[j] != '\0' )
                    break;
            
            // Copy the entire valid data in perBlockMetaBuf
            memcpy( (void*) &(BCTbuf[ i * vdata_size[0]]), (void*) &(perBlockMetaBuf[j]), 
                    (size_t) ( (endBlock-startBlock + 1) * vdata_size[0] ) );
            // Skip i to the end of the valid data
            i = endBlock;
        }
        else
        {
            // Either the if or the else if should always catch. If they don't, something bad happened.
            FATAL_MSG("Something funky just happened. Please debug to figure out what went wrong!\n");
        }
    }
    

    // Write to the dataset our buffer
    status = H5Dwrite(perBlockMetaDset, stringType, perBlockMetaDspace, perBlockMetaDspace, H5P_DEFAULT, 
                      (void*) BCTbuf);
    if ( status < 0 )
    {
        FATAL_MSG("Failed to write to the dataset.\n");
        goto cleanupFail;
    }

    /* We need to create a dimension specifically for the BlockCenterTime dataset. It will be a pure dimension. */
    htri_t linkExists = H5Lexists( outputFile, dimName, H5P_DEFAULT);
    if ( linkExists < 0 )
    {
        FATAL_MSG("Failed to determine if the dimension %s exists.\n", dimName);
        goto cleanupFail;
    }
    if ( linkExists )
    {
        dimID = H5Dopen2( outputFile, dimName, H5P_DEFAULT);
        if ( dimID < 0 )
        {
            FATAL_MSG("Failed to open the dimension.\n");
            dimID = 0;
            goto cleanupFail;
        }        
    }
    else
    {
        // No need to allocate buffer for pure dimension
        //dimBuf = calloc( BCTsize, sizeof(int) );
        // Create the dataspace for the dimension
        // Dimension will always be of size 180
        
        dSpaceID = H5Screate_simple( 1, &BCTsize, NULL );
        if ( dSpaceID < 0 )
        {
            FATAL_MSG("Failed to create a dataspace.\n");
            dSpaceID = 0;
            goto cleanupFail;
        }

        status = makePureDim( dimGroupID, dimName, dSpaceID, H5T_NATIVE_INT, &dimID );
        if ( status != RET_SUCCESS )
        {
            FATAL_MSG("Failed to create dimension.\n");
            goto cleanupFail;
        }
    }

    if ( H5DSattach_scale( perBlockMetaDset, dimID, 0 ) < 0 )
    {
        FATAL_MSG("Failed to attach dimension.\n");
        goto cleanupFail;
    }

    if ( 0 )
    {
cleanupFail:
        retVal = FATAL_ERR;
    }

    if ( vdataID )              VSdetach(vdataID);
    if ( perBlockMetaBuf )      free(perBlockMetaBuf);
    if ( vdata_size)            free( vdata_size );
    if ( perBlockMetaDset )     H5Dclose(perBlockMetaDset);
    if ( dimID )                H5Dclose(dimID); 
    if ( stringType )           H5Tclose(stringType);
    if ( perBlockMetaDspace )   H5Sclose(perBlockMetaDspace);
    //if ( dimBuf )               free(dimBuf);
    if ( dSpaceID )             H5Sclose(dSpaceID);
    if ( BCTbuf )               free(BCTbuf);
    if ( inSDID )               SDend(inSDID);
    return retVal; 
}

