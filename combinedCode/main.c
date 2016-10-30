#include "libTERRA.h"

/*
	argv is of format:
		0: ./programName
		1: [MOPITT input HDF file]
		2: [CERES input HDF file]
		3: [output HDF file]
		4: [time start]
		5: [time end]
*/

int main( int argc, char* argv[] )
{
	if ( argc != 6 )
	{
		fprintf( stderr, "Usage: %s [MOPITT input HDF file] [CERES input HDF file] [output HDF file] [time start] [time end]\nTime arguments are given in minutes where 0 is the beginning of the record.\nIf entire granule is desired, give \"0 0\" for the two time arguments.\n", argv[0] );
		return EXIT_FAILURE;
	}
	
	if ( argv[4] < 0 || argv[5] < 0 || argv[4] > argv[5] )
	{
		fprintf( stderr, "Error! Invalid time interval. Exiting program.\n" );
		return EXIT_FAILURE;
	}
	
	char* MOPITTargs[3];
	MOPITTargs[0] = argv[0];
	MOPITTargs[1] = argv[1];
	MOPITTargs[2] = argv[3];
	
	char* CERESargs[3];
	CERESargs[0] = argv[0];
	CERESargs[1] = argv[2];
	CERESargs[2] = argv[3];
	
	if ( MOPITTmain( 3, MOPITTargs ) == EXIT_FAILURE )
	{
		fprintf( stderr, "Error: MOPITT section failed.\n" );
		return EXIT_FAILURE;
	}
	
	if ( CERESmain( 3, CERESargs) == EXIT_FAILURE )
	{
		fprintf( stderr, "Error: CERES section failed.\n" );
		return EXIT_FAILURE;
	}
}
