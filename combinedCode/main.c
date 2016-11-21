#include "libTERRA.h"
#include <stdio.h>
#define STR_LEN 500
#define IN_FILE "inputFiles.txt"

/*
	
*/

void getNextLine ( char* string, FILE* const inputFile );

int main( int argc, char* argv[] )
{
	if ( argc != 2 )
	{
		fprintf( stderr, "Usage: %s [outputFile]\n", argv[0] );
		return EXIT_FAILURE;
	}
	
	FILE* const inputFile = fopen( IN_FILE, "r" );
	char string[STR_LEN];
	
	if ( inputFile == NULL )
	{
		fprintf( stderr, "Fatal error: file \"inputFiles.txt\" does not exist. Exiting program.\n" );
		
		return EXIT_FAILURE;
	}
	
	char* MOPITTargs[3];
	char* CERESargs[3];
	char* MODISargs[5];
	
	/**********
	 * MOPITT *
	 **********/
	/* get the MOPITT input file paths from our inputFiles.txt */
	getNextLine( string, inputFile);
	
	MOPITTargs[0] = argv[0];
	
	
	/* allocate space for the argument */
	MOPITTargs[1] = malloc( strlen(string) );
	/* copy the data over */
	strncpy( MOPITTargs[1], string, strlen(string) );
	MOPITTargs[2] = argv[1];
	
	/*********
	 * CERES *
	 *********/
	/* get the CERES input files */
	getNextLine( string, inputFile);
	
	
	CERESargs[0] = argv[0];
	/* allocate memory for the argument */
	CERESargs[1] = malloc( strlen(string ));
	strncpy( CERESargs[1], string, strlen(string) );
	CERESargs[2] = argv[1];
	
	/*********
	 * MODIS *
	 *********/
	 MODISargs[0] = argv[0];
	/* get the MODIS 1KM file */
	getNextLine( string, inputFile);

	/* allocate memory for the argument */
	MODISargs[1] = malloc ( strlen(string ) );
	strncpy( MODISargs[1], string, strlen(string));
	
	/* get the MODIS 500m file */
	getNextLine( string, inputFile );
	/* allocate memory */
	MODISargs[2] = malloc( strlen(string) );
	strncpy( MODISargs[2], string, strlen(string));
	
	/* get the MODIS 250m file */
	getNextLine( string, inputFile );
	MODISargs[3] = malloc( strlen(string) );
	strncpy( MODISargs[3], string, strlen(string) );
	
	/* get the MODIS MOD03 file */
	getNextLine( string, inputFile );
	MODISargs[4] = malloc( strlen( string ) );
	strncpy( MODISargs[4], string, strlen(string) );
	
	
	MODISargs[5] = argv[1];
	
	
	if ( MOPITTmain( 3, MOPITTargs ) == EXIT_FAILURE )
	{
		fprintf( stderr, "Error: MOPITT section failed.\n" );
		return -1;
	}
	
	if ( CERESmain( 3, CERESargs) == EXIT_FAILURE )
	{
		fprintf( stderr, "Error: CERES section failed.\n" );
		return -1;
	}
	
	if ( MODISmain( 5, MODISargs) == EXIT_FAILURE )
	{
		fprintf( stderr, "Error: MODIS section failed.\n" );
		return -1;
	}
	
	/* free all memory */
	fclose( inputFile );
	free( MOPITTargs[1] );
	free( CERESargs[1] );
	free( MODISargs[1] );
	free( MODISargs[2] );
	free( MODISargs[3] );
	free( MODISargs[4] );
	
	H5Fclose(outputFile);
	
	return 0;
}

void getNextLine ( char* string, FILE* const inputFile )
{
	//printf("%p\n", inputFile );
	do
	{
		/* if the end of the file has been reached, return */
		if ( feof(inputFile) != 0 )
			return;
			
		if ( fgets( string, 500, inputFile ) == NULL )
		{
			fprintf( stderr, "[%s:%s]: Unable to get next line. Exiting program.\n", __FILE__, __func__ );
			
			exit (EXIT_FAILURE);
		}
		
		
	} while ( string[0] == '#' || string[0] == '\n' );
	
	/* remove the newline character from the buffer if it exists */
	size_t len = strlen( string )-1;
	if ( string[len] == '\n' )
		string[len] = '\0';
	
}


