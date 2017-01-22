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
     *      VARIABLES       *
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
    hid_t status = 0;
    int32 statusn = 0;
    int fail = 0;
    herr_t errStat = 0;
    float tempFloat = 0.0;
    double tempDouble = 0.0;
    char* correctedName = NULL;
    char* fileTime = NULL;
    /******************
     * geo data files *
     ******************/
        /* File IDs */
    int32 h4FileID = 0;
    int32 inHFileID = 0;
    int32 h4_status = 0;
    int32 geoFileID = 0;
    int32 gmpFileID = 0;

        /* Group IDs */
    hid_t MISRrootGroupID = 0;
    hid_t geoGroupID = 0;
        /* Dataset IDs */
    hid_t latitudeID = 0;
    hid_t longitudeID = 0;

        /* Group IDs */
    hid_t gmpSolarGeoGroupID = 0;

    hid_t solarAzimuthID = 0;
    hid_t solarZenithID = 0;

    hid_t h5GroupID = 0;
    hid_t h5DataGroupID = 0;
    hid_t h5SensorGeomGroupID = 0;

    hid_t h5DataFieldID = 0;
    hid_t h5SensorGeomFieldID = 0;
        
    createGroup( &outputFile, &MISRrootGroupID, "MISR" );
    if ( MISRrootGroupID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to create MISR root group.\n");
        return EXIT_FAILURE;
    }

    if(H5LTset_attribute_string(outputFile,"MISR","FilePath",argv[1])<0) {
        FATAL_MSG("Cannot add the time stamp\n");
        goto cleanupFail;
    }

    // Extract the time substring from the file path
    fileTime = getTime( argv[1], 4 );
    if(H5LTset_attribute_string(outputFile,"MISR","GranuleTime",fileTime)<0) {
        FATAL_MSG("Cannot add the time stamp\n");
        goto cleanupFail;
    }
    free(fileTime); fileTime = NULL;

    geoFileID = SDstart( argv[10], DFACC_READ );
    if ( geoFileID == -1 )
    {
        FATAL_MSG("Failed to open the SD interface.\n");
        geoFileID = 0;
        goto cleanupFail;
    }

    gmpFileID = SDstart( argv[11], DFACC_READ );
    if ( gmpFileID == -1 )
    {
        FATAL_MSG("Failed to open the SD interface.\n");
        gmpFileID = 0;
        goto cleanupFail;
    }

        /* Loop all 9 cameras */
    for(int i = 0; i<9;i++) {

        h4FileID = SDstart(argv[i+1],DFACC_READ);
        if ( h4FileID < 0 )
        {
            h4FileID = 0;
            FATAL_MSG("Failed to open the HDF file.\n");
            goto cleanupFail;
        }
        /*
        *           *    * Open the HDF file for reading.
        *                     *       */

        /* Need to use the H interface to obtain scale_factor */
        inHFileID = Hopen(argv[i+1],DFACC_READ, 0);
        if(inHFileID <0) {
            inHFileID = 0;
            FATAL_MSG("Failed to start the H interface.\n");
            goto cleanupFail;
        }
         /*
          *          *    * Initialize the V interface.
          *                   *       */
        h4_status = Vstart (inHFileID);
        if (h4_status < 0)
        {
            FATAL_MSG("Failed to start the H interface.\n");
            goto cleanupFail;
        }


        createGroup(&MISRrootGroupID, &h5GroupID, camera_name[i]);
        if ( h5GroupID == EXIT_FAILURE )
        {
            h5GroupID = 0;
            FATAL_MSG("Failed to create an HDF5 group.\n");
            goto cleanupFail;
        }

        createGroup(&h5GroupID,&h5DataGroupID,data_gname);
        if ( h5DataGroupID == EXIT_FAILURE )
        {
            h5DataGroupID = 0;
            FATAL_MSG("Failed to create an HDF5 group.\n");
            goto cleanupFail;
        }

        /* Need to get all the four band data */
        for (int j = 0; j<4;j++) {

            // If we choose to unpack the data.
            if(unpack == 1) {

                float scale_factor = -1.;
                scale_factor = Obtain_scale_factor(inHFileID,band_name[j]);
                if ( scale_factor < 0.0 )
                {
                    FATAL_MSG("Failed to obtain scale factor for MISR.\n)");
                    goto cleanupFail;
                }
                h5DataFieldID =  readThenWrite_MISR_Unpack( h5DataGroupID, radiance_name[j],DFNT_UINT16,
                                                h4FileID,scale_factor);
                if ( h5DataFieldID == EXIT_FAILURE )
                {
                    FATAL_MSG("MISR readThenWrite Unpacking function failed.\n");
                    h5DataFieldID = 0;
                    goto cleanupFail;
                }
                

            }
            else {
                h5DataFieldID =  readThenWrite( h5DataGroupID, radiance_name[j],DFNT_UINT16,
                                                H5T_NATIVE_USHORT,h4FileID);
                if ( h5DataFieldID == EXIT_FAILURE )
                {
                    h5DataFieldID = 0;
                    FATAL_MSG("MISR readThenWrite function failed.\n");
                    goto cleanupFail;
                }
            }
            
            tempFloat = -999.0;
            correctedName = correct_name(radiance_name[j]);
            errStat = H5LTset_attribute_float( h5DataGroupID, correctedName,"_FillValue",&tempFloat,1);
            if ( errStat < 0 )
            {
                FATAL_MSG("Failed to write an HDF5 attribute.\n");
                goto cleanupFail;
            }
            
                        
            H5Dclose(h5DataFieldID); h5DataFieldID = 0;
            free(correctedName); correctedName = NULL;
        } // End for (first inner j loop)

        createGroup(&h5GroupID,&h5SensorGeomGroupID,sensor_geom_gname);
        if ( h5SensorGeomGroupID == EXIT_FAILURE )
        {
            FATAL_MSG("Failed to create an HDF5 group.\n");
            h5SensorGeomGroupID = 0;
            goto cleanupFail;
        }

        /* Inserting the "AaAzimuth", "AaGlitter", "AaScatter"... etc. */
        for (int j = 0; j<4;j++) {
            h5SensorGeomFieldID = readThenWrite(h5SensorGeomGroupID,band_geom_name[i*4+j],DFNT_FLOAT64,
                                                H5T_NATIVE_DOUBLE,gmpFileID);
            if ( h5SensorGeomFieldID == EXIT_FAILURE )
            {   
                FATAL_MSG("MISR readThenWrite function failed.\n");
                h5SensorGeomFieldID = 0;
                goto cleanupFail;
            }

            tempDouble = -555.0;
            correctedName = correct_name(band_geom_name[i*4+j]);
            errStat = H5LTset_attribute_double( h5SensorGeomGroupID, correctedName,"_Fillvalue",&tempDouble,1);
            if ( errStat < 0 )
            {
                FATAL_MSG("Failed to write an HDF5 attribute.\n");
                goto cleanupFail;
            }        
            status = H5Dclose(h5SensorGeomFieldID); h5SensorGeomFieldID = 0;
            if ( status ) WARN_MSG("H5Dclose\n");
            free(correctedName); correctedName = NULL;
        } // End for (second inner j loop)

        statusn = SDend(h4FileID); h4FileID = 0;
        if ( statusn ) WARN_MSG("SDend.\n");
         /* No need inHFileID, close H and V interfaces */
        h4_status = Vend(inHFileID);
        if ( h4_status ) WARN_MSG("Vend\n");
        h4_status = Hclose(inHFileID);
        if ( h4_status ) WARN_MSG("Hclose\n");
        status = H5Gclose(h5DataGroupID); h5DataGroupID = 0;
        if ( status ) WARN_MSG("H5Gclose\n");
        status = H5Gclose(h5SensorGeomGroupID); h5SensorGeomGroupID = 0;
        if ( status ) WARN_MSG("H5Gclose\n");
        status = H5Gclose(h5GroupID); h5GroupID = 0;
        if ( status ) WARN_MSG("H5Gclose\n");
    }

    createGroup( &MISRrootGroupID, &geoGroupID, geo_gname );
    if ( geoGroupID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to create HDF5 group.\n");
        geoGroupID = 0;
        goto cleanupFail;
    }


    latitudeID  = readThenWrite(geoGroupID,geo_name[0],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID);
    if ( latitudeID == EXIT_FAILURE )
    {
        FATAL_MSG("MISR readThenWrite function failed (latitude dataset).\n");
        latitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[0]);
    errStat = H5LTset_attribute_string(geoGroupID,correctedName,"units","degrees_north");
    if ( errStat < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    free(correctedName); correctedName = NULL;

    longitudeID = readThenWrite(geoGroupID,geo_name[1],DFNT_FLOAT32,H5T_NATIVE_FLOAT,geoFileID);
    if ( longitudeID == EXIT_FAILURE )
    {
        FATAL_MSG("MISR readThenWrite function failed (longitude dataset).\n");
        longitudeID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(geo_name[1]);
    errStat = H5LTset_attribute_string(geoGroupID,(const char*) correct_name,"units","degrees_east");
    if ( errStat < 0 )
    {
        FATAL_MSG("Failed to create HDF5 attribute.\n");
        goto cleanupFail;
    }

    createGroup( &MISRrootGroupID, &gmpSolarGeoGroupID, solar_geom_gname );
    if ( gmpSolarGeoGroupID == EXIT_FAILURE )
    {
        FATAL_MSG("Failed to create HDF5 group.\n");
        gmpSolarGeoGroupID = 0;
        goto cleanupFail;
    }


    solarAzimuthID = readThenWrite(gmpSolarGeoGroupID,solar_geom_name[0],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID);
    if ( solarAzimuthID == EXIT_FAILURE )
    {
        FATAL_MSG("MISR readThenWrite function failed (solarAzimuth dataset).\n");
        solarAzimuthID = 0;
        goto cleanupFail;
    }
    // Read the value of the _FillValue attribute in the SolarAzimuth dataset
    tempDouble = 0.0;
    errStat = H4readSDSAttr( gmpFileID, solar_geom_name[0], "_FillValue", (void*) &tempDouble );
    if ( errStat != EXIT_SUCCESS )
    {
        FATAL_MSG("Failed to read HDF4 attribute.\n");
        goto cleanupFail;
    }

    correctedName = correct_name(solar_geom_name[0]);
    // write this to the corresponding HDF5 dataset as an attribute
    errStat = H5LTset_attribute_double( gmpSolarGeoGroupID, correctedName,"_Fillvalue",&tempDouble,1);
    if ( errStat < 0 )
    {
        FATAL_MSG("Failed to write an HDF5 attribute.\n");
        goto cleanupFail;
    }

    free(correctedName); correctedName = NULL;

    solarZenithID = readThenWrite(gmpSolarGeoGroupID,solar_geom_name[1],DFNT_FLOAT64,H5T_NATIVE_DOUBLE,gmpFileID);
    if ( solarZenithID == EXIT_FAILURE )
    {
        FATAL_MSG("MISR readThenWrite function failed (solarZenith dataset).\n");
        solarZenithID = 0;
        goto cleanupFail;
    }

    correctedName = correct_name(solar_geom_name[1]);
    // write the same tempDouble value into the solarZenith dataset
    errStat = H5LTset_attribute_double( gmpSolarGeoGroupID, correctedName,"_Fillvalue",&tempDouble,1);
    if ( errStat < 0 )
    {
        FATAL_MSG("Failed to write an HDF5 attribute.\n");
        goto cleanupFail;
    }

    free(correctedName); correctedName = NULL;

    if ( 0 )
    {
        cleanupFail:
        fail = 1;
    }

    
    if (MISRrootGroupID) status = H5Gclose(MISRrootGroupID);
    if ( geoFileID ) statusn = SDend(geoFileID);
    if ( gmpFileID ) statusn = SDend(gmpFileID);
    if ( h4FileID ) statusn = SDend(h4FileID);
    statusn = Vend(inHFileID);
    if ( inHFileID ) statusn = Hclose(inHFileID);
    if ( h5GroupID ) status = H5Gclose(h5GroupID);
    if ( h5DataGroupID ) status = H5Gclose(h5DataGroupID);
    if ( h5DataFieldID ) status = H5Dclose(h5DataFieldID);
    if ( h5SensorGeomGroupID ) status = H5Gclose(h5SensorGeomGroupID);
    if ( h5SensorGeomFieldID ) status = H5Dclose(h5SensorGeomFieldID);
    if ( geoGroupID ) status = H5Gclose(geoGroupID);
    if ( latitudeID ) status = H5Dclose(latitudeID);
    if ( longitudeID ) status = H5Dclose(longitudeID);
    if ( gmpSolarGeoGroupID ) status = H5Gclose(gmpSolarGeoGroupID);
    if ( solarAzimuthID ) status = H5Dclose(solarAzimuthID);
    if ( solarZenithID ) status = H5Dclose(solarZenithID);
    if ( correctedName ) free(correctedName);    
    if ( fileTime ) free(fileTime);
    if ( fail ) return EXIT_FAILURE;

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
                FATAL_MSG("Not enough memory for vgroup_name!\n");
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
                          FATAL_MSG("Vgetagref failed.\n");
                          return -1.0;
                    }

                    if(Visvs(sub_group_id,sub_group_obj_ref)==TRUE) {

                        char vdata_name[VSNAMELENMAX];
                        int32 vdata_id = -1;

                        vdata_id = VSattach (h4_file_id, sub_group_obj_ref, "r");
                        if (vdata_id == FAIL){ 
                            FATAL_MSG("VSattach failed.\n");
                            return -1.0;
                        }
                        if (VSgetname (vdata_id, vdata_name) == FAIL) {
                            VSdetach (vdata_id);
                            FATAL_MSG("VSgetname failed.\n");
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
                                FATAL_MSG("VFfieldesize failed.\n");
                                return -1.0;
                            }
                            fieldtype = VFfieldtype(vdata_id,0);

                            // Obtain number of elements
                            nelms = VSelts (vdata_id);
                            if (nelms == FAIL) {
                                VSdetach (vdata_id);
                                FATAL_MSG("VSelts failed.\n");
                                 return -1.0;
                            }


                            // Initialize the seeking process
                            if (VSseek (vdata_id, 0) == FAIL) {
                                VSdetach (vdata_id);
                                FATAL_MSG("VSseek failed.\n");
                                return -1.0;
                            }

 // The field to seek is CERE_META_FIELD_NAME
                            if (VSsetfields (vdata_id, "AttrValues") == FAIL) {
                                VSdetach (vdata_id);
                                FATAL_MSG("VSsetfields failed.\n");
                                return -1.0;
                            }

                            // Read this vdata field value
                            if (VSread(vdata_id, (uint8 *) &sc, 1,FULL_INTERLACE)
                                == FAIL) {
                                VSdetach (vdata_id);
                                FATAL_MSG("VSread failed.\n");
                                return -1.0;
                            }



                        } // end if
                        VSdetach(vdata_id);

                    } // end if

                 } // end for


                free(sub_group_name);
                break;
            } // end if

            free(sub_group_name);
            Vdetach(sub_group_id);

        } // end if

    } // end for
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




