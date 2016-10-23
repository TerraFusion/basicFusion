#include "libTERRA.h"

/*
	argv is of format:
		0: ./programName
		1: [MOPITT input HDF file]
		2: [CERES input HDF file]
		3: [output HDF file]
*/

int main( int argc, char* argv[] )
{
	if ( argc != 4 )
	{
		fprintf( stderr, "Usage: %s [MOPITT input HDF file] [CERES input HDF file] [output HDF file]\n", argv[0] );
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
