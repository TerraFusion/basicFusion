#include "libTERRA.h"
#include <stdio.h>
#include <hdf5.h>

/*
	NOTE: argv[1] = INPUT_FILE
		  argv[2] = OUTPUT_FILE
*/

hid_t outputFile;

int MOPITTmain( int argc, char* argv[] )
{
	if ( argc != 3 )
	{
		
		fprintf( stderr, "Usage: %s [input HDF5 file] [output HDF5 file]\n\n", argv[0] );
		return EXIT_FAILURE;
	
	}
	
	hid_t file;
	
	hid_t MOPITTroot;
	hid_t radianceGroup;
	hid_t geolocationGroup;
	hid_t radianceDataset;
	hid_t latitudeDataset;
	hid_t longitudeDataset;
	hid_t timeDataset;
	hid_t attrID;		// attribute ID
	hid_t stringType;
	
	herr_t status;
	
	int tempInt;
	float tempFloat;
	
	
	/* remove output file if it already exists. Note that no conditional statements are used. If file does not exist,
	 * this function will throw an error but we do not care.
	 */
	 
	remove( argv[2] );
	
	// open the input file
	if ( openFile( &file, argv[1], H5F_ACC_RDONLY ) ) return EXIT_FAILURE;
	
	// create the output file or open it if it exists
	if ( createOutputFile( &outputFile, argv[2] )) return EXIT_FAILURE;
	
	// create the root group
	if ( createGroup( &outputFile, &MOPITTroot, "MOPITT" ) ) return EXIT_FAILURE;
	
	// create the radiance group
	if ( createGroup ( &MOPITTroot, &radianceGroup, "Data Fields" ) ) return EXIT_FAILURE;
	
	// create the geolocation group
	if ( createGroup ( &MOPITTroot, &geolocationGroup, "Geolocation" ) ) return EXIT_FAILURE;
	
	// insert the radiance dataset
	radianceDataset = MOPITTinsertDataset( &file, &radianceGroup, RADIANCE, "Radiance", 1 );
	if ( radianceDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the longitude dataset
	longitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LONGITUDE, "Longitude", 1 );
	if ( longitudeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the latitude dataset
	latitudeDataset = MOPITTinsertDataset( &file, &geolocationGroup, LATITUDE, "Latitude", 1 );
	if ( latitudeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	// insert the time dataset
	timeDataset = MOPITTinsertDataset( &file, &geolocationGroup, TIME, "Time", 1);
	if ( timeDataset == EXIT_FAILURE ) return EXIT_FAILURE;
	
	/***************************************************************
	 * Add the appropriate attributes to the groups we just created*
	 ***************************************************************/
	 
						/* !!! FOR RADIANCE FIELD !!! */
	
	/* TrackCount.  */
	
	attrID = attributeCreate( radianceGroup, "TrackCount", H5T_NATIVE_UINT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempInt = 6350;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempInt );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close TrackCount attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* missing_invalid */
	attrID = attributeCreate( radianceGroup, "missing_invalid", H5T_NATIVE_FLOAT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempFloat = -8888.0;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempFloat );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close missing_invalid attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* missing_nodata */
	attrID = attributeCreate( radianceGroup, "missing_nodata", H5T_NATIVE_FLOAT );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	tempFloat = -9999.0;
	status = H5Awrite( attrID, H5T_NATIVE_UINT, &tempFloat );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close missing_nodata attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* radiance_unit */
	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(RADIANCE_UNIT));
	
	
	attrID = attributeCreate( radianceGroup, "radiance_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, RADIANCE_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close radiance_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	H5Tclose(stringType);
	
								/* ### END OF RADIANCE FIELD ATTRIBUTE CREATION ### */
								
								/* !!! FOR GEOLOCATION FIELD !!! */
	/* Latitude_unit */
	// to store a string in HDF5, we need to create our own special datatype from a character type.
	// Our "base type" is H5T_C_S1, a single byte null terminated string
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(LAT_LON_UNIT) );
	
	
	attrID = attributeCreate( geolocationGroup, "Latitude_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Latitude_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	/* Longitude_unit */
	
	attrID = attributeCreate( geolocationGroup, "Longitude_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, LAT_LON_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Longitude_unit attribute. Exiting program.\n", __func__);
		return EXIT_FAILURE;
	}
	
	H5Tclose(stringType);
	
	/* Time_unit */
	stringType = H5Tcopy(H5T_C_S1);
	H5Tset_size( stringType, strlen(TIME_UNIT) );
	
	
	attrID = attributeCreate( geolocationGroup, "Time_unit", stringType );
	if ( attrID == EXIT_FAILURE ) return EXIT_FAILURE;
	
	status = H5Awrite( attrID, stringType, TIME_UNIT );
	
	status = H5Aclose( attrID );
	if ( status < 0 ) 
	{
		fprintf( stderr, "[%s]: H5Aclose -- Unable to close Time_unit attribute. Exiting program.\n", __func__);
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
	
	/* NOTE!!! outputFile will not be closed by MOPITTmain.c
	 * It should be closed by the last program to use it.
	 */
	//H5Fclose(outputFile);
	H5Gclose(MOPITTroot);
	H5Gclose(radianceGroup);
	H5Gclose(geolocationGroup);
	H5Tclose(stringType);
	
	
	
	
	return 0;

}
