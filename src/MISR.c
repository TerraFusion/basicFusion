#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <hdf.h>
#include <hdf5.h>
#include "libTERRA.h"

/* MT 2016-12-20, mostly re-write the handling of MISR */
float Obtain_scale_factor(int32 h4_file_id, char* band_name);
/* May provide a list for all MISR group and variable names */

int MISR( char* argv[],int unpack )
{
	/****************************************
	 *		VARIABLES		*
	 ****************************************/

		/* File IDs */

        char *camera_name[9]={"AA","AF","AN","BA","BF","CA","CF","DA","DF"};
        char *band_name[4]={"RedBand","BlueBand","GreenBand","NIRBand"};
        char *radiance_name[4]={"Red Radiance/RDQI","Blue Radiance/RDQI","Green Radiance/RDQI","NIR Radiance/RDQI"};
        char *band_geom_name[36] =     {"AaAzimuth","AaGlitter","AaScatter","AaZenith",
                                        "AfAzimuth","AfGlitter","AfScatter","AfZenith",
                                        "AnAzimuth","AnGlitter","AnScatter","AnZenith",
                                        "BaAzimuth","BaGlitter","BaScatter","BaZenith",
                                        "BfAzimuth","BfGlitter","BfScatter","BfZenith",
                                        "CaAzimuth","CaGlitter","CaScatter","CaZenith",
                                        "CfAzimuth","CfGlitter","CfScatter","CfZenith",
                                        "DaAzimuth","DaGlitter","DaScatter","DaZenith",
                                        "DfAzimuth","DfGlitter","DfScatter","DfZenith"};

        char *solar_geom_name[2] = {"SolarAzimuth","SolarZenith"};
        char *geo_name[2] = {"GeoLatitude","GeoLongitude"};

        char *solar_geom_gname="Solar_Geometry";
        char *geo_gname="Geolocation";
        char *data_gname="Data Fields";
        char *sensor_geom_gname ="Sensor_Geometry";


	/******************
	 * geo data files *
	 ******************/
		/* File IDs */
    int32 h4FileID;
    int32 inHFileID;
    int32 h4_status;
	int32 geoFileID;
	int32 gmpFileID;

		/* Group IDs */
	hid_t MISRrootGroupID;
	hid_t geoGroupID;
		/* Dataset IDs */
	hid_t latitudeID;
	hid_t longitudeID;

		/* Group IDs */
	hid_t gmpSolarGeoGroupID;

    hid_t solarAzimuthID;
    hid_t solarZenithID;

    hid_t h5GroupID;
    hid_t h5DataGroupID;
    hid_t h5SensorGeomGroupID;

    hid_t h5DataFieldID;
    hid_t h5SensorGeomFieldID;
        
	createGroup( &outputFile, &MISRrootGroupID, "MISR" );
	if ( MISRrootGroupID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create MISR root group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    if(H5LTset_attribute_string(outputFile,"MISR","GranuleTime",argv[1])<0) {
        fprintf(stderr, "[%s:%s:%d] Cannot add the time stamp\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	geoFileID = SDstart( argv[10], DFACC_READ );
    if ( geoFileID == -1 )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to open the SD interface.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

	gmpFileID = SDstart( argv[11], DFACC_READ );
    if ( gmpFileID == -1 )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to open the SD interface.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

        /* Loop all the 0 cameras */
    for(int i = 0; i<9;i++) {

        h4FileID = SDstart(argv[i+1],DFACC_READ);
        /*
        *           *    * Open the HDF file for reading.
        *                     *       */

        /* Need to use the H interface to obtain scale_factor */
        inHFileID = Hopen(argv[i+1],DFACC_READ, 0);
        if(inHFileID <0) {
            fprintf( stderr, "[%s:%s:%d] Failed to open the HDF file.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

         /*
          *          *    * Initialize the V interface.
          *                   *       */
        h4_status = Vstart (inHFileID);
        assert(h4_status != -1);


        createGroup(&MISRrootGroupID, &h5GroupID, camera_name[i]);
        if ( h5GroupID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create an HDF5 group.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        createGroup(&h5GroupID,&h5DataGroupID,data_gname);
        if ( h5GroupID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create an HDF5 group.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        /* Need to get all the four band data */
        for (int j = 0; j<4;j++) {

            // If we choose to unpack the data.
            if(unpack == 1) {

                float scale_factor = -1.;
                scale_factor = Obtain_scale_factor(inHFileID,band_name[j]);

                h5DataFieldID =  readThenWrite_MISR_Unpack( h5DataGroupID, radiance_name[j],DFNT_UINT16,
                                                h4FileID,scale_factor);
                if ( h5DataFieldID == EXIT_FAILURE )
                {
                    fprintf( stderr, "[%s:%s:%d] MISR readThenWrite Unpacking function failed.\n", __FILE__,__func__,__LINE__);
                    H5Dclose(h5DataFieldID);
                    return EXIT_FAILURE;
                }
                H5Dclose(h5DataFieldID);

            }
            else {
                h5DataFieldID =  readThenWrite( h5DataGroupID, radiance_name[j],DFNT_UINT16,
                                                H5T_NATIVE_USHORT,h4FileID);
                if ( h5DataFieldID == EXIT_FAILURE )
                {
                    fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed.\n", __FILE__,__func__,__LINE__);
                    H5Dclose(h5DataFieldID);
                    return EXIT_FAILURE;
                }
                H5Dclose(h5DataFieldID);
            }
            
        }

        createGroup(&h5GroupID,&h5SensorGeomGroupID,sensor_geom_gname);
        if ( h5SensorGeomGroupID == EXIT_FAILURE )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to create an HDF5 group.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        for (int j = 0; j<4;j++) {
            h5SensorGeomFieldID = readThenWrite(h5SensorGeomGroupID,band_geom_name[i*4+j],DFNT_FLOAT64,
                                                H5T_NATIVE_DOUBLE,gmpFileID);
            if ( h5SensorGeomGroupID == EXIT_FAILURE )
            {   
                fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed.\n", __FILE__,__func__,__LINE__);
                return EXIT_FAILURE;
            }
            H5Dclose(h5SensorGeomFieldID);
        }

        SDend(h4FileID);
         /* No need inHFileID, close H and V interfaces */
        h4_status = Vend(inHFileID);
        if ( h4_status == -1 )
        {   
            fprintf( stderr, "[%s:%s:%d] Failed to end V interface.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        h4_status = Hclose(inHFileID);
        if ( h4_status == -1 )
        {
            fprintf( stderr, "[%s:%s:%d] Failed to end H interface.\n", __FILE__,__func__,__LINE__);
            return EXIT_FAILURE;
        }

        H5Gclose(h5DataGroupID);
        H5Gclose(h5SensorGeomGroupID);
        H5Gclose(h5GroupID);
    
    }

    createGroup( &MISRrootGroupID, &geoGroupID, geo_gname );
    if ( geoGroupID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create HDF5 group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }


    latitudeID  = readThenWrite(geoGroupID,geo_name[0],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID);
    if ( latitudeID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed (latitude dataset).\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    longitudeID = readThenWrite(geoGroupID,geo_name[1],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID);
    if ( longitudeID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed (longitude dataset).\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    createGroup( &MISRrootGroupID, &gmpSolarGeoGroupID, solar_geom_gname );
    if ( gmpSolarGeoGroupID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] Failed to create HDF5 group.\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }


    solarAzimuthID = readThenWrite(gmpSolarGeoGroupID,solar_geom_name[0],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID);
    if ( solarAzimuthID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed (solarAzimuth dataset).\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }
    solarZenithID = readThenWrite(gmpSolarGeoGroupID,solar_geom_name[1],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID);
    if ( solarZenithID == EXIT_FAILURE )
    {
        fprintf( stderr, "[%s:%s:%d] MISR readThenWrite function failed (solarZenith dataset).\n", __FILE__,__func__,__LINE__);
        return EXIT_FAILURE;
    }

    SDend(geoFileID);
    SDend(gmpFileID);
    
    H5Dclose(latitudeID);
    H5Dclose(longitudeID);

    H5Dclose(solarAzimuthID);
    H5Dclose(solarZenithID);
  
    H5Gclose(geoGroupID);
    H5Gclose(gmpSolarGeoGroupID);

    H5Gclose(MISRrootGroupID);
	return EXIT_SUCCESS;
}


float Obtain_scale_factor(int32 h4_file_id, char* band_name) {

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

    for (int i = 0; i < num_gobjects; i++) {


        if (Vgettagref (band_group_id, i, &sub_group_tag, &sub_group_ref) == FAIL) {
            Vdetach (band_group_id);
            return -1.0;
        }


        if (Visvg (band_group_id, sub_group_ref) == TRUE) {

            sub_group_id = Vattach (h4_file_id, sub_group_ref, "r");
            status = Vgetnamelen(sub_group_id, &name_len);
            sub_group_name = (char *) HDmalloc(sizeof(char *) * (name_len+1));
            if (sub_group_name == NULL)
            {
             fprintf(stderr, "Not enough memory for vgroup_name!\n");
             return -1.0;
            }
            status = Vgetname (sub_group_id, sub_group_name);
            if(strncmp(sub_group_name,grid_attr_group_name,strlen(grid_attr_group_name))==0) {
                
                 int num_sgobjects = Vntagrefs(sub_group_id);
                 assert(num_sgobjects >0);
                 for(int j = 0; j<num_sgobjects;j++) {
                    if (Vgettagref (sub_group_id, j, &sub_group_obj_tag, &sub_group_obj_ref) == FAIL) {
                          Vdetach (sub_group_id);
                          Vdetach(band_group_id);
                          return -1.0;
                    }

                    if(Visvs(sub_group_id,sub_group_obj_ref)==TRUE) {

                        char vdata_name[VSNAMELENMAX];
                        int32 vdata_id = -1;

                        vdata_id = VSattach (h4_file_id, sub_group_obj_ref, "r");
                        if (vdata_id == FAIL) 
                            return -1.0;

                        if (VSgetname (vdata_id, vdata_name) == FAIL) {
                            VSdetach (vdata_id);
                            return -1.0;
                        }


                        if(strncmp(scale_factor_name,vdata_name,strlen(scale_factor_name)) == 0){

                            int32 fieldsize=-1;
                            int32 nelms = -1;
                            int32 fieldtype=-1;

                            // Obtain field size
                            fieldsize = VFfieldesize (vdata_id, 0);
                            if (fieldsize == FAIL) {
                                VSdetach (vdata_id);
                                return -1.0;
                            }
                            fieldtype = VFfieldtype(vdata_id,0);

                            // Obtain number of elements
                            nelms = VSelts (vdata_id);
                            if (nelms == FAIL) {
                                VSdetach (vdata_id);
                                 return -1.0;
                            }


                            // Initialize the seeking process
                            if (VSseek (vdata_id, 0) == FAIL) {
                                VSdetach (vdata_id);
                                return -1.0;
                            }

 // The field to seek is CERE_META_FIELD_NAME
                            if (VSsetfields (vdata_id, "AttrValues") == FAIL) {
                                VSdetach (vdata_id);
                                return -1.0;
                            }

                            // Read this vdata field value
                            if (VSread(vdata_id, (uint8 *) &sc, 1,FULL_INTERLACE)
                                == FAIL) {
                                VSdetach (vdata_id);
                                return -1.0;
                            }



                        }
                        VSdetach(vdata_id);

                    }

                 }


#if 0
printf("grid_attr_group name is %s\n",grid_attr_group_name);
                int n_attrs = Vnattrs(sub_group_id);
printf("number of attributes is %d\n",n_attrs);
                sc_attr_index = Vfindattr(sub_group_id,scale_factor_name);
                assert(sc_attr_index !=FAIL);
                status = Vgetattr(sub_group_id,sc_attr_index,&sc);
                assert(status!=FAIL);
#endif
                free(sub_group_name);
                break;
            }

            free(sub_group_name);
            Vdetach(sub_group_id);

        }

    }
    Vdetach(band_group_id);

    return (float)sc;
}

#if 0
//float Obtain_scale_factor(int32 h4_file_id, char* band_name, char *radiance_name) {
float Obtain_scale_factor(int32 h4_file_id, char* band_name) {
 
    int32 band_group_ref = -1;
    int32 sub_group_ref = -1;
    int32 sub_group_tag = -1;
    float sc = 0;
    int num_gobjects = 0;
    int32 band_group_id  = 0;
    int32 sub_group_id =0;
    char* sub_group_name = NULL;
    char* grid_attr_group_name = "Grid Attributes";
    char* scale_factor_name = "Scale factor";
    intn  sc_attr_index = -1;
    uint16 name_len = 0;

    int32 status = -1;
    
    /* Obtain the vgroup reference number of this band */
    band_group_ref = H4ObtainLoneVgroupRef(h4_file_id,band_name);
    assert(band_group_ref >0);

    band_group_id = Vattach(h4_file_id, band_group_ref, "r");
    num_gobjects = Vntagrefs(band_group_id);
    assert(num_gobjects >0);
    
    for (int i = 0; i < num_gobjects; i++) {
    
        
        if (Vgettagref (band_group_id, i, &sub_group_tag, &sub_group_ref) == FAIL) {
            Vdetach (band_group_id);
            return -1.0;
        }

        if (Visvg (band_group_id, sub_group_ref) == TRUE) {
            
            sub_group_id = Vattach (h4_file_id, sub_group_ref, "r");
            status = Vgetnamelen(sub_group_id, &name_len);
            sub_group_name = (char *) HDmalloc(sizeof(char *) * (name_len+1));
            if (sub_group_name == NULL)
            {
             fprintf(stderr, "Not enough memory for vgroup_name!\n");
             return -1.0;
            }
            status = Vgetname (sub_group_id, sub_group_name);
            if(strncmp(sub_group_name,grid_attr_group_name,strlen(grid_attr_group_name))==0) {
                sc_attr_index = Vfindattr(sub_group_id,scale_factor_name);
                assert(sc_attr_index !=FAIL);
                status = Vgetattr(sub_group_id,sc_attr_index,&sc);
                assert(status!=FAIL);
            }

            free(sub_group_name);
            Vdetach(sub_group_id);

            break;
        }

    }
    Vdetach(band_group_id);

    return sc;
}
#endif




