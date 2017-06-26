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

/*
    NOTE: argv[1] = INPUT_FILE
          argv[2] = OUTPUT_FILE
*/

hid_t outputFile;

int MOPITT( char* argv[], OInfo_t cur_orbit_info, int* granuleNum )
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
    int fail = 0;
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

    // open the input file
    if ( openFile( &file, argv[1], H5F_ACC_RDONLY ) )
    {
        FATAL_MSG("Unable to open MOPITT file.\n\t%s\n", argv[1]);
        file = 0;
        goto cleanupFail;
    }

    /* LandonClipp Apr 7 2017. Before inserting datasets, we need to find the starting and ending indices for subsetting. */
    status = MOPITT_OrbitInfo ( file, cur_orbit_info, TIME, &startIdx, &endIdx );
    if ( status == 1 )
    {
        /* LandonClipp Apr 7 2017
           Decrement the granuleNum variable in the calling function. Calling function increments naively
           to keep track of how many granules have been inserted, but if no granule is inserted (in the case
           where the status == 1), we'll need to modify this variable to account for that.
        */
        *granuleNum += -1;
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
    if ( *granuleNum == 1 )
    {
        // create the root group
        if ( createGroup( &outputFile, &MOPITTroot, "MOPITT" ) == EXIT_FAILURE )
        {
            FATAL_MSG("Unable to create MOPITT root group.\n");
            MOPITTroot = 0;
            goto cleanupFail;
        }
    }
    else
    {
        MOPITTroot = H5Gopen2( outputFile, "MOPITT", H5P_DEFAULT );
        if ( MOPITTroot < 0 )
        {
            MOPITTroot = 0;
            FATAL_MSG("Failed to open MOPITT group for the creation of granule %d.\n", *granuleNum );
            goto cleanupFail;
        }
    }

    /* Create the granule group */
    if ( *granuleNum == 1 || *granuleNum == 2 )
    {
        char granName[9] = {'\0'};
        sprintf(granName,"granule%d",*granuleNum);
        status = createGroup( &MOPITTroot, &granuleID, granName );
        if ( status == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create granule group number %d.\n", *granuleNum );
            granuleID = 0;
            goto cleanupFail;
        }

        /* Set the attributes for the granule group indicating information about the original file */

        tmpCharPtr = strrchr(argv[1], '/');
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

        fileTime = getTime( argv[1], 0 );
        if(H5LTset_attribute_string(MOPITTroot,granName,"GranuleTime",fileTime)<0)
        {
            FATAL_MSG("Unable to set attribute string.\n");
            goto cleanupFail;
        }
        free(fileTime);
        fileTime = NULL;
    }
    else
    {
        FATAL_MSG("MOPITT can't have more than 2 granules in one orbit!\n");
        goto cleanupFail;
    }


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
    if ( radianceDataset == EXIT_FAILURE )
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

    if ( *granuleNum == 1 )
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
    else if ( *granuleNum == 2 )
    {
        ntrackID = MOPITTaddDimension( outputFile, "ntrack_2", dimSize, intArray, H5T_NATIVE_INT );
        if ( ntrackID == FAIL )
        {
            ntrackID = 0;
            FATAL_MSG("Failed to add MOPITT dimension.\n");
            goto cleanupFail;
        }

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
        FATAL_MSG("MOPITT can't have more than 2 granules in one orbit!\n");
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
    if ( longitudeDataset == EXIT_FAILURE )
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
    if ( latitudeDataset == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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
    if ( attrID == EXIT_FAILURE )
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

    if ( 0 )
    {
cleanupFail:
        fail = 1;
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
    if ( fail )
        return EXIT_FAILURE;

    return EXIT_SUCCESS;


}
