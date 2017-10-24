#include "libTERRA.h"
#include <stdio.h>
#include <hdf5.h>

#define RADIANCE "HDFEOS/SWATHS/MOP01/Data Fields/MOPITTRadiances"
#define LATITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Latitude"
#define LONGITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Longitude"
#define TIME "HDFEOS/SWATHS/MOP01/Geolocation Fields/Time"
#define RADIANCE_UNIT "Watts/m^2/steradian"
#define LAT_LON_UNIT "degree"
#define TIME_UNIT "seconds since 1993-01-01"


/*  MOPITT
 
 DESCRIPTION:
    This function handles the MOPITT data repacking into the output Fusion file
    given by the global hid_t variable outputFile. The MOPITT HDF5 file passed 
    in inputFile will be read into the outputFile.

 ARGUMENTS:
    inputFile   -- Input MOPITT file path

 EFFECTS:
    Modifies the output fusion file to contain the appropriate MOPITT data.

 RETURN:
    FAIL_ERR        -- A general error occured
    FAIL_OPEN       -- Failed to open the inputFile file
    RET_SUCCESS     -- Success
*/

int MOPITT( char* inputFile, OInfo_t cur_orbit_info )
{

    hid_t file = 0;

    hid_t MOPITTroot = 0;
    hid_t radianceGroup = 0;
    hid_t geolocationGroup = 0;
    hid_t radianceDataset = 0;
    hid_t latitudeDataset = 0;
    hid_t longitudeDataset = 0;
    hid_t timeDataset = 0;
    hid_t attrID = 0;       // attribute ID
    hid_t inputAttrID = 0;
    hid_t stringType = 0;
    hid_t tempGroupID = 0;
    hid_t datatypeID = 0;
    hid_t tempSpace = 0;
    hid_t ntrackID = 0;
    hid_t nstareID = 0;
    hid_t npixelsID = 0;
    hid_t nchanID = 0;
    hid_t nstateID = 0;
    hid_t granuleID = 0;

    herr_t status = 0;

    hsize_t* dims = NULL;
    hsize_t dimSize = 0;


    int tempInt = 0;
    int retVal = RET_SUCCESS;
    int* intArray = NULL;
    unsigned int startIdx = 0;
    unsigned int endIdx = 0;
    unsigned int bound[2];
    float tempFloat = 0.0f;
    char* correctName = NULL;
    char* fileTime = NULL;
    char* tmpCharPtr = NULL;
    char* coordinatePath = NULL;
    char* lonPath = NULL;
    char* latPath = NULL;

    
    // Other field ids.
    hid_t calibDataset = 0;
    hid_t dailyGainDataset = 0;
    hid_t dailyMNDataset = 0;
    hid_t dailyMPDataset = 0;
    hid_t engDataset = 0;
    hid_t level0StdDataset = 0;
    hid_t PacketPosDataset = 0;
    hid_t SaZenithDataset = 0;
    hid_t SaAzimuthDataset = 0;
    hid_t SoZenithDataset = 0;
    hid_t SoAzimuthDataset = 0;
    hid_t SecCalibDataset = 0;
    hid_t SQualityDataset = 0;

    // Other field additional dimension ids.
    hid_t ncalibID  = 0;
    hid_t npchanID  = 0;
    hid_t npositionID = 0;
    hid_t nengpointsID = 0;
    hid_t ncalibID = 0;
    


    // open the input file
    if ( openFile( &file, inputFile, H5F_ACC_RDONLY ) )
    {
        WARN_MSG("Unable to open MOPITT file.\n\t%s\n", inputFile);
        file = 0;
        goto cleanupFO;
    }

    /* Extract the file time from the file name */
    fileTime = getTime( inputFile, 0 );

    /* LandonClipp Apr 7 2017. Before inserting datasets, we need to find the starting and ending indices for subsetting. */
    status = MOPITT_OrbitInfo ( file, cur_orbit_info, TIME, &startIdx, &endIdx );
    if ( status == 1 )
    {
        /* This granule is out of bounds */
        goto cleanup;
    }
    else if ( status == 2 )
    {
        FATAL_MSG("Failed to obtain MOPITT subsetting information.\n");
        goto cleanupFail;
    }
    bound[0] = startIdx;
    bound[1] = endIdx;



    /* If it's the first granule, create the root MOPITT group */
    htri_t exists = H5Lexists( outputFile, "MOPITT", H5P_DEFAULT);
    if ( exists == 0 )
    {
        // create the root group
        if ( createGroup( &outputFile, &MOPITTroot, "MOPITT" ) == FATAL_ERR )
        {
            FATAL_MSG("Unable to create MOPITT root group.\n");
            MOPITTroot = 0;
            goto cleanupFail;
        }
    }
    else if ( exists < 0 )
    {
        FATAL_MSG("Failed to determine if the MOPITT group exists for granule %s.\n", fileTime);
        goto cleanupFail;
    }
    else
    {
        MOPITTroot = H5Gopen2( outputFile, "MOPITT", H5P_DEFAULT );
        if ( MOPITTroot < 0 )
        {
            MOPITTroot = 0;
            FATAL_MSG("Failed to open MOPITT group for the creation of granule %s.\n", fileTime);
            goto cleanupFail;
        }
    }

    /* MOPITT should have at most 2 granules. If 2 granules are currently present, we should throw an error.
     * The presence of 2 granules means that a third will be attempted to be added.
     */
    H5G_info_t group_info;
    status = H5Gget_info( MOPITTroot, &group_info );
    if ( group_info.nlinks >= 2 )  
    {
        FATAL_MSG("MOPITT can't have more than two granules!\n");
        goto cleanupFail;
    }

    /* Create the granule group */
    char granName[17] = {'\0'};

    /* The granule group will be named "granule_[fileTime]" */
    sprintf(granName,"granule_%s", fileTime);
    status = createGroup( &MOPITTroot, &granuleID, granName );
    if ( status == FATAL_ERR )
    {
        FATAL_MSG("Failed to create granule group.\n");
        granuleID = 0;
        goto cleanupFail;
    }

    /* Set the attributes for the granule group indicating information about the original file */

    tmpCharPtr = strrchr(inputFile, '/');
    if ( tmpCharPtr == NULL )
    {
        FATAL_MSG("Failed to find a specific character within a string.\n");
        goto cleanupFail;
    }
    if(H5LTset_attribute_string(MOPITTroot,granName,"GranuleName", tmpCharPtr+1 )<0)
    {
        FATAL_MSG("Unable to set attribute string.\n");
        goto cleanupFail;
    }

    if(H5LTset_attribute_string(MOPITTroot,granName,"GranuleTime",fileTime)<0)
    {
        FATAL_MSG("Unable to set attribute string.\n");
        goto cleanupFail;
    }
    free(fileTime);
    fileTime = NULL;


    // create the radiance group
    if ( createGroup ( &granuleID, &radianceGroup, "Data Fields" ) )
    {
        FATAL_MSG("Unable to create MOPITT radiance group.\n");
        radianceGroup = 0;
        goto cleanupFail;
    }


    // create the geolocation group
    if ( createGroup ( &granuleID, &geolocationGroup, "Geolocation" ) )
    {
        FATAL_MSG("Unable to create MOPITT geolocation group.\n");
        geolocationGroup = 0;
        goto cleanupFail;
    }


    /********************
     * RADIANCE DATASET *
     ********************/

    // insert the radiance dataset
    radianceDataset = MOPITTinsertDataset( &file, &radianceGroup, RADIANCE, "MOPITTRadiances", H5T_NATIVE_FLOAT, 1, bound );
    if ( radianceDataset == FATAL_ERR )
    {
        FATAL_MSG("Unable to insert MOPITT radiance dataset.\n");
        radianceDataset = 0;
        goto cleanupFail;
    }

    /* Add dimensions to this dataset

       Based on StructMetadata.0 in the input MOPITT file, the radiance dataset has the dimensions:
       ntrack, nstare, npixels, nchan, nstate.

       These must be hard copied over. The input HDF5 file does not have the dimensions attached to the objects.
    */

    // Add the necessary dimensions to the output file

    /* calloc initializes to 0. No scale information will be present, so keep array as 0's (this is desired behavior) */
    dimSize = bound[1] - bound[0] + 1;
    intArray = calloc ( dimSize, sizeof(int) );

    /* group_info.nlinks can give us the granule's index */
    // The logic here is not strightforward and it is dangerous. It is better to use a flag to distinguish different groups.
    // KY 2017-10-24
    if ( group_info.nlinks == 0 )
    {
        ntrackID = MOPITTaddDimension( outputFile, "ntrack_1", dimSize, intArray, H5T_NATIVE_INT );
        if ( ntrackID == FAIL )
        {
            ntrackID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }


        /* Reallocate the empty array to a size of 29. This array is just used to create pure dim scales
           in the HDF5 file. Setting to size 29 because that is the largest size needed for our dimensions
           from here on out.
        */
        intArray = realloc( intArray, 29 * sizeof(int) );
        if ( intArray == NULL )
        {
            FATAL_MSG("Failed to reallocate array.\n");
            goto cleanupFail;
        }

        memset(intArray, 0, 29);

        dimSize = 29;
        nstareID = MOPITTaddDimension( outputFile, "nstare", dimSize, intArray, H5T_NATIVE_INT );
        if ( nstareID == FAIL )
        {
            nstareID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }
        if ( change_dim_attr_NAME_value(nstareID) == FAIL )
        {
            FATAL_MSG("Failed to change the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }

        dimSize = 4;
        npixelsID = MOPITTaddDimension( outputFile, "npixels", dimSize, intArray, H5T_NATIVE_INT );
        if ( npixelsID == FAIL )
        {
            npixelsID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }
        if ( change_dim_attr_NAME_value(npixelsID) == FAIL )
        {
            FATAL_MSG("Failed to change the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }


        dimSize = 8;
        nchanID = MOPITTaddDimension( outputFile, "nchan", dimSize, intArray, H5T_NATIVE_INT );
        if ( nchanID == FAIL )
        {
            nchanID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }
        if ( change_dim_attr_NAME_value(nchanID) == FAIL )
        {
            FATAL_MSG("Failed to change the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }


        dimSize = 2;
        nstateID = MOPITTaddDimension( outputFile, "nstate", dimSize, intArray, H5T_NATIVE_INT );
        if ( nstateID == FAIL )
        {
            nstateID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }
        if ( change_dim_attr_NAME_value(nstateID) == FAIL )
        {
            FATAL_MSG("Failed to change the NAME attribute for a dimension.\n");
            goto cleanupFail;
        }
    }
    else if ( group_info.nlinks == 1 )
    {
        ntrackID = MOPITTaddDimension( outputFile, "ntrack_2", dimSize, intArray, H5T_NATIVE_INT );
        if ( ntrackID == FAIL )
        {
            ntrackID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }

        /* We don't need to create these dimensions again (they were initially created in the case where group_info.nlinks
           equaled 0). So, just open them.
        */

        nstareID = H5Dopen2( outputFile, "nstare", H5P_DEFAULT);
        if ( nstareID < 0 )
        {
            FATAL_MSG("Failed to open nstare dimension.\n");
            nstareID = 0;
            goto cleanupFail;
        }

        npixelsID = H5Dopen2( outputFile, "npixels", H5P_DEFAULT);
        if ( npixelsID < 0 )
        {
            FATAL_MSG("Failed to open npixels dimension.\n");
            npixelsID = 0;
            goto cleanupFail;
        }

        nchanID = H5Dopen2( outputFile, "nchan", H5P_DEFAULT);
        if ( nchanID < 0 )
        {
            FATAL_MSG("Failed to open nchan dimension.\n");
            nchanID = 0;
            goto cleanupFail;
        }

        nstateID = H5Dopen2( outputFile, "nstate", H5P_DEFAULT);
        if ( nstateID < 0 )
        {
            FATAL_MSG("Failed to open nstate dimension.\n");
            nstateID = 0;
            goto cleanupFail;
        }
    }
    else
    {
        FATAL_MSG("Something strange happened.\n");
        goto cleanupFail;
    }


    /* Change the NAME attribute for this dimension */
    if ( change_dim_attr_NAME_value(ntrackID) == FAIL )
    {
        FATAL_MSG("Failed to change the NAME attribute for a dimension.\n");
        goto cleanupFail;
    }

    /* Attach these dimensions to the dataset */
    
    status = H5DSattach_scale( radianceDataset, ntrackID, 0 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( radianceDataset, nstareID, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( radianceDataset, npixelsID, 2 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( radianceDataset, nchanID, 3 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( radianceDataset, nstateID, 4 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }



    /*********************
     * LONGITUDE DATASET *
     *********************/

    // insert the longitude dataset
    longitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LONGITUDE, "Longitude", H5T_NATIVE_FLOAT, 1, bound );
    if ( longitudeDataset == FATAL_ERR )
    {
        FATAL_MSG("Unable to insert MOPITT longitude dataset.\n");
        longitudeDataset = 0;
        goto cleanupFail;
    }

    /* Save the HDF5 path of the longitude dataset */
    ssize_t pathSize = H5Iget_name( longitudeDataset, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the longitude path name.\n");
        goto cleanupFail;
    } 
    latPath = calloc(pathSize+1, 1 );
    if ( latPath == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(longitudeDataset, latPath, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve longitude path name.\n");
        goto cleanupFail;
    } 

    // MY 2017-01-26: Add the CF longitude units here
    if(H5LTset_attribute_string(geolocationGroup,"Longitude","units","degrees_east")<0)
    {

        FATAL_MSG("Unable to insert MOPITT longitude units attribute.\n");
        goto cleanupFail;

    }
    {
        float temp_fvalue = -9999.0;
        // MY 2017-01-26: Add the CF _FillValue attribure here, I found -9999.0 inside the Latitude field for file MOP01-20070703-L1V3.50.0.he5
        if(H5LTset_attribute_float(geolocationGroup,"Longitude","_FillValue",&temp_fvalue,1)<0)
        {

            FATAL_MSG("Unable to insert MOPITT longitude _FillValue attribute.\n");
            goto cleanupFail;
        }

        // MY 2017-01-26: Add the CF valid_range attribure here. According to CF, longitude should always be from -180 to 180.
        float temp_lon_valid_range[2] = {-180.0,180.0};
        if(H5LTset_attribute_float(geolocationGroup,"Longitude","valid_range",temp_lon_valid_range,2)<0)
        {

            FATAL_MSG("Unable to insert MOPITT longitude valid_range attribute.\n");
            goto cleanupFail;
        }

    }

    /* Add the following dimension scales to the dataset:
        ntrack, nstare, npixels
    */

    status = H5DSattach_scale( longitudeDataset, ntrackID, 0 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( longitudeDataset, nstareID, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( longitudeDataset, npixelsID, 2 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }


    if ( H5Dclose(longitudeDataset) < 0 )
        WARN_MSG("Failed to close dataset.\n");

    longitudeDataset = 0;


    /********************
     * LATITUDE DATASET *
     ********************/

    // insert the latitude dataset
    latitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LATITUDE, "Latitude", H5T_NATIVE_FLOAT, 1, bound );
    if ( latitudeDataset == FATAL_ERR )
    {
        FATAL_MSG("Unable to insert MOPITT latitude dataset.\n");
        latitudeDataset = 0;
        goto cleanupFail;
    }

    pathSize = H5Iget_name( latitudeDataset, NULL, 0 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to get the size of the latitude path name.\n");
        goto cleanupFail;
    }
    lonPath = calloc(pathSize+1, 1 );
    if ( lonPath == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }

    pathSize = H5Iget_name(latitudeDataset, lonPath, pathSize+1 );
    if ( pathSize < 0 )
    {
        FATAL_MSG("Failed to retrieve latitude path name.\n");
        goto cleanupFail;
    }

    // MY 2017-01-26: Add the CF longitude units here
    if(H5LTset_attribute_string(geolocationGroup,"Latitude","units","degrees_north")<0)
    {

        FATAL_MSG("Unable to insert MOPITT latitude units attribute.\n");
        goto cleanupFail;

    }
    {
        float temp_fvalue = -9999.0;
        // MY 2017-01-26: Add the CF _FillValue attribure here, I found -9999.0 inside the Latitude field for file MOP01-20070703-L1V3.50.0.he5
        if(H5LTset_attribute_float(geolocationGroup,"Latitude","_FillValue",&temp_fvalue,1)<0)
        {

            FATAL_MSG("Unable to insert MOPITT latitude _FillValue attribute.\n");
            goto cleanupFail;
        }

        // MY 2017-01-26: Add the CF valid_range attribure here. According to CF, longitude should always be from -180 to 180.
        float temp_lat_valid_range[2] = {-90.0,90.0};
        if(H5LTset_attribute_float(geolocationGroup,"Latitude","valid_range",temp_lat_valid_range,2)<0)
        {

            FATAL_MSG("Unable to insert MOPITT latitude valid_range attribute.\n");
            goto cleanupFail;
        }

    }

    /* Add the following dimension scales to the dataset:
        ntrack, nstare, npixels
    */

    status = H5DSattach_scale( latitudeDataset, ntrackID, 0 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( latitudeDataset, nstareID, 1 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }
    status = H5DSattach_scale( latitudeDataset, npixelsID, 2 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }


    if ( H5Dclose(latitudeDataset) < 0)
        WARN_MSG("Failed to close dataset.\n");

    latitudeDataset = 0;



    /* Attach the coordinates attribute to the radiance dataset (latitude and longitude HDF5 paths) */

    coordinatePath = calloc( strlen(latPath) + strlen(lonPath) + 2, 1 );
    if ( coordinatePath == NULL )
    {
        FATAL_MSG("Failed to allocate memory.\n");
        goto cleanupFail;
    }
    strcpy(coordinatePath, lonPath);
    strcat(coordinatePath, " ");
    strcat(coordinatePath, latPath);
    
    status = H5LTset_attribute_string( radianceGroup, "MOPITTRadiances", "coordinates", coordinatePath);
    if ( status < 0 )
    {
        FATAL_MSG("Failed to set string attribute.\n");
        goto cleanupFail;
    }

    free( coordinatePath); coordinatePath = NULL;
    free( latPath ); latPath = NULL;
    free( lonPath ); lonPath = NULL;
    
    /****************
     * TIME DATASET *
     ****************/
    // insert the time dataset
    // Note that the time dataset is converted from TAI93 time to UTC time in this function.
    timeDataset = MOPITTinsertDataset( &file, &geolocationGroup, TIME, "Time", H5T_NATIVE_DOUBLE, 1, bound);
    if ( timeDataset < 0 )
    {
        FATAL_MSG("Unable to insert MOPITT time dataset.\n");
        timeDataset = 0;
        goto cleanupFail;
    }

    status = H5DSattach_scale( timeDataset, ntrackID, 0 );
    if ( status < 0 )
    {
        FATAL_MSG("Failed to attach dimension scale.\n");
        goto cleanupFail;
    }



    /***************************************************************
     * Add the appropriate attributes to the groups we just created*
     ***************************************************************/

    /* !!! FOR RADIANCE FIELD !!! */

    /*#############*/
    /* TrackCount. */
    /*#############*/

    attrID = attributeCreate( radianceGroup, "TrackCount", H5T_NATIVE_INT );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create MOPITT TrackCout attribute.\n");
        attrID = 0;
        goto cleanupFail;
    }


    /* We need to get the value for TrackCount. This is simply the first dimension in the MOPITTRadiances
     * dataset
     */
    /* First get the rank */
    tempSpace = H5Dget_space( radianceDataset );
    tempInt = H5Sget_simple_extent_ndims( tempSpace );

    if ( tempInt < 0 )
    {
        FATAL_MSG("Unable to get dataset rank for MOPITT.\n");
        goto cleanupFail;
    }

    /* Now that we have rank, allocate space for the array to store the size of each dimension */
    dims = calloc( sizeof(hsize_t) * tempInt, 1 );
    /* Get the size of each dimension */
    if ( H5Sget_simple_extent_dims( tempSpace, dims, NULL ) < 0 )
    {
        FATAL_MSG("Unable to get dataset dimension sizes for MOPITT.\n");
        goto cleanupFail;
    }
    H5Sclose(tempSpace);
    tempSpace = 0;

    /* write the value of the first element in dims to the TrackCount attribute */
    status = H5Awrite( attrID, H5T_NATIVE_INT, dims );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to TrackCount attribute (radiance dataset).\n");
        goto cleanupFail;
    }

    /* free the dims array */
    free( dims );
    dims = NULL;
    /* close the attribute */
    status = H5Aclose( attrID );
    attrID = 0;
    if ( status < 0 )
    {
        FATAL_MSG("Unable to close TrackCount attribute.\n");
    }

    /*#################*/
    /* missing_invalid */
    /*#################*/

    attrID = attributeCreate( radianceDataset, "missing_invalid", H5T_NATIVE_FLOAT );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create missing_invalid attribute.\n");
        goto cleanupFail;
    }


    /* we need to get the value of missing_invalid from the input dataset */
    /* first however, we need to open this attribute from the input dataset */
    tempGroupID = H5Gopen( file, "HDFEOS/SWATHS/MOP01", H5P_DEFAULT );
    if ( tempGroupID < 0 )
    {
        FATAL_MSG("Unable to open group from input file.\n");
        tempGroupID = 0;
        goto cleanupFail;
    }

    /* open the missing invalid attribute */
    inputAttrID = H5Aopen_name(tempGroupID, "missing_invalid" );
    if ( inputAttrID < 0 )
    {
        FATAL_MSG("Unable to open attribute from input file.\n");
        inputAttrID = 0;
        goto cleanupFail;
    }

    /* get the datatype stored at this attribute */
    datatypeID = H5Aget_type( inputAttrID );
    if ( datatypeID < 0 )
    {
        FATAL_MSG("Unable to get datatype from attribute.\n");
        datatypeID = 0;
        goto cleanupFail;
    }

    /* read the value stored here */
    status = H5Aread( inputAttrID, datatypeID, &tempFloat );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read data from attribute.\n");
        goto cleanupFail;
    }

    /* now we can write the value of missing_invalid to our output attribute */
    status = H5Awrite( attrID, H5T_NATIVE_FLOAT, (const void *) &tempFloat );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read data from attribute.\n");
        goto cleanupFail;
    }

    /* close all related identifiers */

    H5Aclose( attrID );
    attrID = 0;
    H5Aclose( inputAttrID );
    inputAttrID = 0;
    H5Tclose( datatypeID );
    datatypeID = 0;


    /*################*/
    /* missing_nodata */
    /*################*/

    attrID = attributeCreate( radianceDataset, "missing_nodata", H5T_NATIVE_FLOAT );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create missing_invalid attribute.\n");
        attrID = 0;
        goto cleanupFail;
    }

    /* open the missing invalid attribute */
    inputAttrID = H5Aopen_name(tempGroupID, "missing_nodata" );
    if ( inputAttrID < 0 )
    {
        FATAL_MSG("Unable to open attribute from input file.\n");
        inputAttrID = 0;
        goto cleanupFail;
    }

    /* get the datatype stored at this attribute */
    datatypeID = H5Aget_type( inputAttrID );
    if ( datatypeID < 0 )
    {
        FATAL_MSG("Unable to get datatype from attribute.\n");
        datatypeID = 0;
        goto cleanupFail;
    }

    /* read the value stored here */
    status = H5Aread( inputAttrID, datatypeID, &tempFloat );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read data from attribute.\n");
        goto cleanupFail;
    }

    /* now we can write the value of missing_nodata to our output attribute */
    status = H5Awrite( attrID, H5T_NATIVE_FLOAT, (const void *) &tempFloat );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to read data from attribute.\n");
        goto cleanupFail;
    }

    /* close all related identifiers */

    H5Aclose( attrID );
    attrID = 0;
    H5Gclose( tempGroupID );
    tempGroupID = 0;
    H5Aclose( inputAttrID );
    inputAttrID = 0;
    H5Tclose( datatypeID );
    datatypeID = 0;


    /*###############*/
    /* radiance_unit */
    /*###############*/

    // to store a string in HDF5, we need to create our own special datatype from a character type.
    // Our "base type" is H5T_C_S1, a single byte null terminated string
    stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        FATAL_MSG("Unable to copy datatype.\n");
        stringType = 0;
        goto cleanupFail;
    }

    status = H5Tset_size( stringType, strlen(RADIANCE_UNIT)+1);
    if ( status < 0 )
    {
        FATAL_MSG("Unable to set the size of a datatype.\n");
        goto cleanupFail;
    }

    status = H5Tset_strpad( stringType, H5T_STR_NULLTERM);
    if ( status < 0 )
    {
        FATAL_MSG("Unable to set the pad of an HDF5 string datatype.\n");
        goto cleanupFail;
    }


    attrID = attributeCreate( radianceDataset, "units", stringType );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create radiance_unit attribute.\n");
        attrID = 0;
        goto cleanupFail;
    }

    status = H5Awrite( attrID, stringType, RADIANCE_UNIT );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to attribute.\n");
        goto cleanupFail;
    }

    H5Aclose( attrID );
    attrID = 0;
    H5Tclose(stringType);
    stringType = 0;
    H5Gclose(radianceGroup);
    radianceGroup = 0;

    /* ### END OF RADIANCE FIELD ATTRIBUTE CREATION ### */

    /* !!! FOR GEOLOCATION FIELD !!! */
    /* Latitude_unit */
    // to store a string in HDF5, we need to create our own special datatype from a character type.
    // Our "base type" is H5T_C_S1, a single byte null terminated string
    // MY 2017-01-26: The following attributes are not needed. Don't know why they are added at first place.
    //
#if 0
    stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        FATAL_MSG("Unable to copy datatype.\n",__FILE__,__func__,__LINE__);
        stringType = 0;
        goto cleanupFail;
    }

    status = H5Tset_size( stringType, strlen(LAT_LON_UNIT) );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to set the size of a datatype.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }


    attrID = attributeCreate( geolocationGroup, "Latitude_unit", stringType );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create attribute.\n",__FILE__,__func__,__LINE__);
        attrID = 0;
        goto cleanupFail;
    }


    status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }

    H5Aclose( attrID );
    attrID = 0;
    /* Longitude_unit */

    attrID = attributeCreate( geolocationGroup, "Longitude_unit", stringType );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create attribute.\n",__FILE__,__func__,__LINE__);
        attrID = 0;
        goto cleanupFail;
    }

    status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        goto cleanupFail;
    }

    H5Aclose( attrID );
    attrID = 0;
    H5Tclose(stringType);
    stringType = 0;
#endif
    /* Time_unit */
    stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        FATAL_MSG("Unable to copy datatype.\n");
        stringType = 0;
        goto cleanupFail;
    }
    status = H5Tset_size( stringType, strlen(TIME_UNIT)+1 );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to set the size of an HDF5 string datatype.\n");
        goto cleanupFail;
    }

    status = H5Tset_strpad( stringType, H5T_STR_NULLTERM);
    if ( status < 0 )
    {
        FATAL_MSG("Unable to set the pad of an HDF5 string datatype.\n");
        goto cleanupFail;
    }



    //attrID = attributeCreate( geolocationGroup, "units", stringType );
    // The units should be for Time rather than for the whole group MY 2017-06-15
    attrID = attributeCreate( timeDataset, "units", stringType );
    if ( attrID == FATAL_ERR )
    {
        FATAL_MSG("Unable to create attribute.\n");
        attrID = 0;
        goto cleanupFail;
    }

    status = H5Awrite( attrID, stringType, TIME_UNIT );
    if ( status < 0 )
    {
        FATAL_MSG("Unable to write to attribute.\n");
        goto cleanupFail;
    }
    
    // Adding other fields,see if we can make this a little bit easier. All 
    // the code for other fields except the ID declaration will be in this {} section. KY 2017-10-24
    {

        // For fields other than radiance,latitude,longitude and time.
        // We don't know which other field is not useful. So we convert them all. 
        // If a field has the ntrack dimension, it is also subsetted. The sizes of
        // these fields are small, so it is not an issue. KY 2017-10-24
        char *CalibData="/HDFEOS/SWATHS/MOP01/Data Fields/CalibrationData";
        char *DailyGainMean[2] = {"/HDFEOS/SWATHS/MOP01/Data Fields/DailyGainDev",
                                  "/HDFEOS/SWATHS/MOP01/Data Fields/DailyMeanNoise"
                                 };
        char *DMPosNoise="/HDFEOS/SWATHS/MOP01/Data Fields/DailyMeanPositionNoise";
        char *EngData="/HDFEOS/SWATHS/MOP01/Data Fields/EngineeringData";
        char *L0StdDev="/HDFEOS/SWATHS/MOP01/Data Fields/Level0StdDev";
        char *PacPos="/HDFEOS/SWATHS/MOP01/Data Fields/PacketPositions";
        char *SecCalData="/HDFEOS/SWATHS/MOP01/Data Fields/SectorCalibrationData";
        char *SwathQual="/HDFEOS/SWATHS/MOP01/Data Fields/SwathQuality";
        char *mop_geom[4] = {"/HDFEOS/SWATHS/MOP01/Data Fields/SatelliteAzimuth",
                             "/HDFEOS/SWATHS/MOP01/Data Fields/SatelliteZenith",
                             "/HDFEOS/SWATHS/MOP01/Data Fields/SolarAzimuth",
                             "/HDFEOS/SWATHS/MOP01/Data Fields/SolarZenith"
                             };
 
// TODO
#if 0
        
        radianceDataset = MOPITTinsertDataset( &file, &radianceGroup, RADIANCE, "MOPITTRadiances", H5T_NATIVE_FLOAT, 1, bound );
    if ( radianceDataset == FATAL_ERR )
    {
        FATAL_MSG("Unable to insert MOPITT radiance dataset.\n");
        radianceDataset = 0;
        goto cleanupFail;
    }

#endif








    }
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

cleanup:

    if ( attrID )           H5Aclose( attrID );
    if ( inputAttrID )      H5Aclose(inputAttrID);
    if ( latitudeDataset )  H5Dclose(latitudeDataset);
    if ( longitudeDataset ) H5Dclose(longitudeDataset);
    if ( radianceDataset )  H5Dclose(radianceDataset);
    if ( timeDataset )      H5Dclose(timeDataset);
    if ( file )             H5Fclose(file);
    if ( MOPITTroot )       H5Gclose(MOPITTroot);
    if ( geolocationGroup ) H5Gclose(geolocationGroup);
    if ( radianceGroup )    H5Gclose(radianceGroup);
    if ( tempGroupID )      H5Gclose(tempGroupID);
    if ( tempSpace )        H5Sclose(tempSpace);
    if ( datatypeID )       H5Tclose(datatypeID);
    if ( stringType )       H5Tclose(stringType);
    if ( dims )             free(dims);
    if ( correctName )      free(correctName);
    if ( fileTime )         free(fileTime);
    if ( intArray != NULL ) free(intArray);
    if ( ntrackID)          H5Dclose(ntrackID);
    if ( nstareID )         H5Dclose(nstareID);
    if ( npixelsID )        H5Dclose( npixelsID );
    if ( nchanID )          H5Dclose(nchanID);
    if ( nstateID )         H5Dclose(nstateID);
    if ( granuleID )        H5Gclose(granuleID);
    if ( coordinatePath )   free(coordinatePath);
    if ( latPath )          free(latPath);
    if ( lonPath )          free(lonPath);
    
    return retVal;

}
