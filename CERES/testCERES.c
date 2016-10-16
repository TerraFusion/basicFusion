#include <stdio.h>
#include "HDFread.h"

int main( int argc, char* argv[] )
{
	if ( argc != 2 )
	{
		fprintf( stderr, "Usage: %s HDFfilename\n", argv[0] );
		return EXIT_FAILURE;
	}
	
	printf("\nName of HDF file to be read: \n< %s\n", argv[1] );
	
	if (!Hishdf(argv[1]))
	{
		fprintf( stderr, "ERROR - specified file is not an HDF file.\n");
		return EXIT_FAILURE;
	}
	
	
	
	int32 num_fields;
	int32 vfield_status;
	int32 vdata_status;
	int32 vfield_types[MAX_VDATA_FIELDS];
	int32 vfield_order[MAX_VDATA_FIELDS] = {0};
	int32 vfield_index[MAX_VDATA_FIELDS];
	int32 k;
	int32 rd_num_fields;
	int32 rd_num_recs;
	
	void *vdata_data[MAX_VDATA_FIELDS];
	
	char *vdata_name;
	char *vfield_name;
	char *HDF_File = argv[1];
	
	float64 sat_vel[10][3];
	
	num_fields = 18;
	vdata_name = "Satellite-Celestial Data";
	vfield_name = "Satellite Velocity at record start";
	vfield_status = get_vfield_types( HDF_File, vdata_name, num_fields, vfield_types,
									  vfield_order);
									  
	if ( vfield_status != 0 )
     {
        printf ("\nError in getting Vdata field types or orders\n");
        exit(1);
     }
     
	vdata_data[0]=sat_vel;
	rd_num_fields = 1;
	rd_num_recs = 7;
	vdata_status = read_vdata(HDF_File, vdata_name, rd_num_fields,
				   rd_num_recs, vfield_name, vfield_types, vfield_order,
				   vfield_index, vdata_data);

	if ( vdata_status != 0 )
     {
        printf ("\nError in reading Vdata: \"%s\" ", vdata_name);
        return EXIT_FAILURE;
     }
     
	fprintf (stdout,"\nVdata name: \"%s\" \n", vdata_name);
	fprintf (stdout,"\n Vdata field name: \"%s\" \n\n", vfield_name);
	for (k=0; k<7; k++)
    {
        printf ("k=%i\nsat_vel = %f   %f   %f\n",
                 k, sat_vel[k][0], sat_vel[k][1], sat_vel[k][2]);
    }
    
    printf("%d\n", vfield_order[99] );
    
    return 0;
}
