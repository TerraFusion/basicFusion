#include "libTERRA.h"
#include <stdio.h>
#include <hdf5.h>

#define RADIANCE "HDFEOS/SWATHS/MOP01/Data Fields/MOPITTRadiances"
#define LATITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Latitude"
#define LONGITUDE "HDFEOS/SWATHS/MOP01/Geolocation Fields/Longitude"
#define TIME "HDFEOS/SWATHS/MOP01/Geolocation Fields/Time"
#define RADIANCE_UNIT "Watts meter-2 Sr-1"
#define LAT_LON_UNIT "degree"
#define TIME_UNIT "seconds since 12 AM 1-1-13 UTC"

/*
	NOTE: argv[1] = INPUT_FILE
		  argv[2] = OUTPUT_FILE
*/

hid_t outputFile;

int MOPITT( char* argv[] )
{
	
    hid_t file;
	
    hid_t MOPITTroot;
    hid_t radianceGroup;
    hid_t geolocationGroup;
    hid_t radianceDataset;
    hid_t latitudeDataset;
    hid_t longitudeDataset;
    hid_t timeDataset;
    hid_t attrID;		// attribute ID
    hid_t inputAttrID;
    hid_t stringType;
    hid_t tempGroupID;
    hid_t datatypeID;
	
    herr_t status;
    
    hsize_t* dims;
	
    int tempInt;
    float tempFloat;
	
    // open the input file
    if ( openFile( &file, argv[1], H5F_ACC_RDONLY ) )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to open MOPITT file.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
    // create the root group
    if ( createGroup( &outputFile, &MOPITTroot, "MOPITT" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create MOPITT root group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
    if(H5LTset_attribute_string(MOPITTroot,"/MOPITT","GranuleTime",argv[1])<0)
    {
        fprintf( stderr, "[%s:%s:%d] Unable to set attribute string.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    // create the radiance group
    if ( createGroup ( &MOPITTroot, &radianceGroup, "Data Fields" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create MOPITT radiance group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    // create the geolocation group
    if ( createGroup ( &MOPITTroot, &geolocationGroup, "Geolocation" ) )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create MOPITT geolocation group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    // insert the radiance dataset
    radianceDataset = MOPITTinsertDataset( &file, &radianceGroup, RADIANCE, "MOPITTRadiances", H5T_NATIVE_FLOAT, 1 );
    if ( radianceDataset == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to insert MOPITT radiance dataset.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    // insert the longitude dataset
    longitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LONGITUDE, "Longitude", H5T_NATIVE_FLOAT, 1 );
    if ( longitudeDataset == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to insert MOPITT longitude dataset.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    // insert the latitude dataset
    latitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LATITUDE, "Latitude", H5T_NATIVE_FLOAT, 1 );
    if ( latitudeDataset == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to insert MOPITT latitude dataset.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    // insert the time dataset
    timeDataset = MOPITTinsertDataset( &file, &geolocationGroup, TIME, "Time", H5T_NATIVE_DOUBLE, 1);
    if ( timeDataset < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to insert MOPITT time dataset.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
    /***************************************************************
     * Add the appropriate attributes to the groups we just created*
     ***************************************************************/
	 
                    /* !!! FOR RADIANCE FIELD !!! */
	
                            /*#############*/
                            /* TrackCount. */
                            /*#############*/ 
	
    attrID = attributeCreate( radianceGroup, "TrackCount", H5T_NATIVE_UINT );
    if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create MOPITT TrackCout attribute.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }


    /* We need to get the value for TrackCount. This is simply the first dimension in the MOPITTRadiances
     * dataset
     */
    /* First get the rank */
    hid_t tempSpace = H5Dget_space( radianceDataset );
    tempInt = H5Sget_simple_extent_ndims( tempSpace );

    if ( tempInt < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to get dataset rank for MOPITT.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* Now that we have rank, allocate space for the array to store the size of each dimension */
    dims = malloc( sizeof(hsize_t) * tempInt );
    /* Get the size of each dimension */
    if ( H5Sget_simple_extent_dims( tempSpace, dims, NULL ) < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to get dataset dimension sizes for MOPITT.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
    H5Sclose(tempSpace);

    /* write the value of the first element in dims to the TrackCount attribute */
    status = H5Awrite( attrID, H5T_NATIVE_UINT, dims );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close TrackCount attribute.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* free the dims array */
    free( dims );
	/* close the attribute */
    status = H5Aclose( attrID );
    if ( status < 0 ) 
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close TrackCount attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	                        
                            /*#################*/
	                        /* missing_invalid */
                            /*#################*/

	attrID = attributeCreate( radianceGroup, "missing_invalid", H5T_NATIVE_FLOAT );
	if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create missing_invalid attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
	
    /* we need to get the value of missing_invalid from the input dataset */
    /* first however, we need to open this attribute from the input dataset */
    tempGroupID = H5Gopen( file, "HDFEOS/SWATHS/MOP01", H5P_DEFAULT );
    if ( tempGroupID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to open group from input file.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* open the missing invalid attribute */
    inputAttrID = H5Aopen_name(tempGroupID, "missing_invalid" );
    if ( inputAttrID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to open attribute from input file.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* get the datatype stored at this attribute */
    datatypeID = H5Aget_type( inputAttrID );
    if ( datatypeID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to get datatype from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* read the value stored here */
    status = H5Aread( inputAttrID, datatypeID, &tempFloat );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read data from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* now we can write the value of missing_invalid to our output attribute */
	status = H5Awrite( attrID, H5T_NATIVE_UINT, (const void *) &tempFloat );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read data from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* close all related identifiers */

	status = H5Aclose( attrID );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    status = H5Aclose( inputAttrID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close output file attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    status = H5Tclose( datatypeID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close input file attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	                        /*################*/
	                        /* missing_nodata */
                            /*################*/

	attrID = attributeCreate( radianceGroup, "missing_nodata", H5T_NATIVE_FLOAT );
    if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create missing_invalid attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* open the missing invalid attribute */
    inputAttrID = H5Aopen_name(tempGroupID, "missing_nodata" );
    if ( inputAttrID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to open attribute from input file.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* get the datatype stored at this attribute */
    datatypeID = H5Aget_type( inputAttrID );
    if ( datatypeID < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to get datatype from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* read the value stored here */
    status = H5Aread( inputAttrID, datatypeID, &tempFloat );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read data from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* now we can write the value of missing_nodata to our output attribute */
    status = H5Awrite( attrID, H5T_NATIVE_UINT, (const void *) &tempFloat );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to read data from attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    /* close all related identifiers */

    status = H5Aclose( attrID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close output file attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    status = H5Gclose( tempGroupID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close group.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    status = H5Aclose( inputAttrID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close input file attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    status = H5Tclose( datatypeID );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

                                    /*###############*/
                                    /* radiance_unit */
                                    /*###############*/

	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to copy datatype.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Tset_size( stringType, strlen(RADIANCE_UNIT));
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to set the size of a datatype.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
	attrID = attributeCreate( radianceGroup, "radiance_unit", stringType );
    if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create radiance_unit attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Awrite( attrID, stringType, RADIANCE_UNIT );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Aclose( attrID );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Tclose(stringType);
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close datatype identifier.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

								/* ### END OF RADIANCE FIELD ATTRIBUTE CREATION ### */
								
								/* !!! FOR GEOLOCATION FIELD !!! */
	/* Latitude_unit */
	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
    if ( stringType < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to copy datatype.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Tset_size( stringType, strlen(LAT_LON_UNIT) );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to set the size of a datatype.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
	attrID = attributeCreate( geolocationGroup, "Latitude_unit", stringType );
	if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
	status = H5Aclose( attrID );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    

	
	/* Longitude_unit */
	
	attrID = attributeCreate( geolocationGroup, "Longitude_unit", stringType );
	if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	status = H5Aclose( attrID );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
	status = H5Tclose(stringType);
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close datatype identifier.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	/* Time_unit */
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(TIME_UNIT) );
	
	
	attrID = attributeCreate( geolocationGroup, "Time_unit", stringType );
    if ( attrID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to create attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
	status = H5Awrite( attrID, stringType, TIME_UNIT );
    if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to write to attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
	
	status = H5Aclose( attrID );
	if ( status < 0 )
    {
        fprintf( stderr, "[%s:%s:%d] Unable to close attribute.\n",__FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	
					/* ### END OF GEOLOCATION FIELD ATTRIBUTE CREATION ### */
	
	/*
	 * Free all associated memory
	 */
	 
	H5Dclose(radianceDataset);
	H5Dclose(latitudeDataset);
	H5Dclose(longitudeDataset);
	H5Dclose(timeDataset);
	H5Fclose(file);
	
	H5Gclose(MOPITTroot);
	H5Gclose(radianceGroup);
	H5Gclose(geolocationGroup);
	H5Tclose(stringType);
	
	
	
	
	return 0;

}
