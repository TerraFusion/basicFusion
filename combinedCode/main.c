#include "libTERRA.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#define STR_LEN 500
#define IN_FILE "inputFiles.txt"

void getNextLine ( char* string, FILE* const inputFile );

int main( int argc, char* argv[] )
{
	if ( argc != 2 )
	{
		fprintf( stderr, "Usage: %s [outputFile]\n", argv[0] );
		return EXIT_FAILURE;
	}
	
	/* open the input file list */
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
	char* ASTERargs[3];
	char* MISRargs[11];
	
	/* Main will assert that the lines found in the inputFiles.txt file match
	 * the expected input files. This will be done using string.h's strstr() function.
	 * This will halt the execution of the program if the line recieved by getNextLine
	 * does not match the expected input. In other words, only input files meant for
	 * MOPITT, CERES, ASERT etc will pass this error check.
	 */
	char MOPITTcheck[] = "MOP01";
	char CEREScheck[] = "CER_BDS_Terra";
	char MODIScheck1[] = "MOD021KM";
	char MODIScheck2[] = "MOD02HKM";
	char MODIScheck3[] = "MOD02QKM";
	char MODIScheck4[] = "MOD03";
	char ASTERcheck[] = "AST_L1T_";
	char MISRcheck1[] = "MISR_AM1_GRP";
	char MISRcheck2[] = "MISR_AM1_AGP";
	
	/**********
	 * MOPITT *
	 **********/
	/* get the MOPITT input file paths from our inputFiles.txt */
	getNextLine( string, inputFile);
	/* assert will fail if unexpected input is present */
	assert( strstr( string, MOPITTcheck ) != NULL );
	
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
	assert( strstr( string, CEREScheck ) != NULL );
	
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
	assert( strstr( string, MODIScheck1 ) != NULL );

	/* allocate memory for the argument */
	MODISargs[1] = malloc ( strlen(string ) );
	strncpy( MODISargs[1], string, strlen(string));
	
	/* get the MODIS 500m file */
	getNextLine( string, inputFile );
	assert( strstr( string, MODIScheck2 ) != NULL );
	
	/* allocate memory */
	MODISargs[2] = malloc( strlen(string) );
	strncpy( MODISargs[2], string, strlen(string));
	
	/* get the MODIS 250m file */
	getNextLine( string, inputFile );
	assert( strstr( string, MODIScheck3 ) != NULL );
	
	MODISargs[3] = malloc( strlen(string) );
	strncpy( MODISargs[3], string, strlen(string) );
	
	/* get the MODIS MOD03 file */
	getNextLine( string, inputFile );
	assert( strstr( string, MODIScheck4 ) != NULL );
	
	MODISargs[4] = malloc( strlen( string ) );
	strncpy( MODISargs[4], string, strlen(string) );
	
	
	MODISargs[5] = argv[1];
	
	/*********
	 * ASTER *
	 *********/
	
	/* get the ASTER input files */
	getNextLine( string, inputFile );
	assert( strstr( string, ASTERcheck ) != NULL );
	
	ASTERargs[0] = argv[0];
	/* allocate memory for the argument */
	ASTERargs[1] = malloc( strlen(string ));
	strncpy( ASTERargs[1], string, strlen(string) );
	ASTERargs[2] = argv[1];
	
	/********
	 * MISR *
	 ********/
	MISRargs[0] = argv[0];
	
	for ( int i = 1; i < 10; i++ )
	{
		/* get the next MISR input file */
		getNextLine( string, inputFile );
		assert( strstr( string, MISRcheck1 ) != NULL );
		MISRargs[i] = malloc ( strlen( string ) );
		MISRargs[i] = string;
		printf("\n%s\n", MISRargs[i] );
	}
	
	getNextLine( string, inputFile );
	assert( strstr( string, MISRcheck2 ) != NULL );
	MISRargs[10] = malloc ( strlen( string ) );
	MISRargs[10] = string;
	
	
	printf("Transferring MOPITT...");
	assert ( MOPITT( MOPITTargs ) != EXIT_FAILURE );
	#if 1
	printf("MOPITT done.\nTransferring CERES...");
	assert ( CERES( CERESargs) != EXIT_FAILURE );
	printf("CERES done.\nTransferring MODIS...");
	assert ( MODIS( MODISargs) != EXIT_FAILURE );
	printf("MODIS done.\nTransferring ASTER...");
	assert ( ASTER( ASTERargs) != EXIT_FAILURE );
	printf("ASTER done.\nTransferring MISR...");
	#endif
	assert ( MISR( MISRargs) != EXIT_FAILURE );
	printf("MISR done.\n");
	/* free all memory */
	fclose( inputFile );
	free( MOPITTargs[1] );
	free( CERESargs[1] );
	free( MODISargs[1] );
	free( MODISargs[2] );
	free( MODISargs[3] );
	free( MODISargs[4] );
	free( ASTERargs[1] );
	for ( int i = 1; i < 11; i++ ) free( MISRargs[i] );
	
	H5Fclose(outputFile);
	
	return 0;
}

void getNextLine ( char* string, FILE* const inputFile )
{
	
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
		
		
	} while ( string[0] == '#' || string[0] == '\n' || string[0] == ' ' );
	
	/* remove the trailing newline or space character from the buffer if it exists */
	size_t len = strlen( string )-1;
	if ( string[len] == '\n' || string[len] == ' ')
		string[len] = '\0';
	
}
