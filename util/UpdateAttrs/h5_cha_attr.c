/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 *  This program changes the comment_for_GranuleTime attribute and also
 *  adds "units" attribute to ASTER geometry variables.
 *  An input file list is used to provide the HDF5 TF file names that need to  be processed.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdf5.h"
#define STR_LEN 500

static herr_t grp_info(hid_t loc_id, const char *name, const H5L_info_t *linfo,
    void *opdata);              /* Link iteration operator function */

static herr_t grp2_info(hid_t loc_id, const char *name, const H5L_info_t *linfo,
    void *opdata);              /* Link iteration operator function */

int getNextLine(char* string, FILE* const inputFile );

int main (int argc, char*argv[])
{

    FILE* inputFile = NULL;
    char inputLine[STR_LEN];
    hid_t   file,aster_group;       /* File and dataset identifiers */
    int status = 0;
    char *time_comment_name="comment_for_GranuleTime";
    char *time_comment_value = "Under each ASTER granule group, the GranuleTime attribute represents "
                              "the time of data acquisition in UTC with the MMDDYYYYhhmmss format. "
                              "D: day. M: month. Y: year. h: hour. m: minute s:second. "
                              "For example, 01112010002054 represents January 11th, 2010, "
                              "at the 0 hour, the 20th minute, the 54th second UTC.";

    if(argc!=2) {
        fprintf( stderr, "Usage: %s [inputFiles.txt] \n", argv[0] );
        return -1;     
    }

    /* open the input file list */
    inputFile = fopen( argv[1], "r" );

    if ( inputFile == NULL )
    {
        fprintf( stderr, "Usage: %s [inputFiles.txt] \n", argv[1] );
        return -1;
    }

    /* Get the next line from our inputFiles.txt */
    status = getNextLine( inputLine, inputFile);

    while (status == 1) {
        file = H5Fopen(inputLine, H5F_ACC_RDWR, H5P_DEFAULT);
        if(file <0) {
            fprintf( stderr, "Cannot open HDF5 file %s \n", inputLine );
            fclose(inputFile);
            return -1;
        }

        if(H5Lexists(file,"/ASTER",H5P_DEFAULT)>0) {
            aster_group = H5Gopen2(file,"/ASTER",H5P_DEFAULT);
            if(aster_group <0) {
                fprintf( stderr, "Cannot open Group /ASTER . \n");
                H5Fclose(file);
                fclose(inputFile);
                return -1;
            }
            if(H5Adelete(aster_group,time_comment_name)<0) {
                fprintf( stderr, "Cannot delete attribute %s . \n",time_comment_name);
                H5Gclose(aster_group);
                H5Fclose(file);
                fclose(inputFile);
                return -1;
            }
            if(H5LTset_attribute_string(file,"ASTER",time_comment_name,time_comment_value) <0) {
                fprintf( stderr, "Cannot set attribute %s for group ASTER . \n",time_comment_name);
                H5Gclose(aster_group);
                H5Fclose(file);
                fclose(inputFile);
                return -1;
            }
            if(H5Literate(aster_group,H5_INDEX_NAME,H5_ITER_INC,NULL,grp_info,NULL)<0) {
                fprintf( stderr, "Cannot iterate group ASTER . \n");
                H5Gclose(aster_group);
                H5Fclose(file);
                fclose(inputFile);
                return -1;
            }
            
            H5Gclose(aster_group);
        }
   
        H5Fclose(file);
        inputLine[0]='\0';
        status = getNextLine(inputLine,inputFile);

    }
    if(status == 0) {
        printf("Finish updating the ASTER attributes for BF files.\n");
        fflush(stdout);
    }
    else if(status == -1)
        fprintf(stderr,"Wrong input HDF5 file names\n");
   
    fclose(inputFile);
    return status;
}

/*
 *  * Operator function, loop through the group Solar_Geometry and PointingAngle.
 *   */
static herr_t
grp_info(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata)
{
    hid_t gid;  /* dataset identifier  */
    int idx_g;

    /* avoid warnings */
    opdata = opdata;
    linfo = linfo;

    /*
     * Open the datasets using their names.
     */
    gid = H5Gopen2(loc_id, name, H5P_DEFAULT);
   
    //printf("\nName : %s\n", name);
    if(H5Literate_by_name(gid,"Solar_Geometry",H5_INDEX_NAME,H5_ITER_INC,NULL,grp2_info,NULL,H5P_DEFAULT)<0) {
        H5Gclose(gid);
        return -1;
    }
    if(H5Literate_by_name(gid,"PointingAngle",H5_INDEX_NAME,H5_ITER_INC,NULL,grp2_info,NULL,H5P_DEFAULT)<0) {
        H5Gclose(gid);
	    return -1;
    }
    H5Gclose(gid);

    return 0;

}
    
/* Adding units for the geometry fields */
static herr_t
grp2_info(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata)
{

    /* avoid warnings */
    opdata = opdata;
    linfo = linfo;
    if(H5LTset_attribute_string(loc_id,name,"units","degree")<0)
        return -1;

    return 0;

}

/*  getNextLine
 *
 *   DESCRIPTION:
 *   This function grabs the next valid file path in inputFile. It skips any line that starts with '#', '\n', or ' '.
 *   The result is stored in the string argument. It is the duty of the caller to ensure that string argument has
 *   enough space to hold any string in the inputFile. If the string is larger than the macro STR_LEN, the string
 *   will be cut off.
 *
 *   ARGUMENTS:
 *      char* string            -- The pointer to a string array where the resultant string will be stored.
 *      FILE* const inputFile   -- The file to fetch the next string from.
 *
 *      EFFECTS:
 *          Modifies the memory pointed to by string.
 *
 *      RETURN:
 *         0: end of file
 *         1: successfully obtain a line 
 *         -1: error   
 *
 *          
 **/

int getNextLine ( char* string, FILE* const inputFile )
{

    do
    {
        if ( fgets( string, STR_LEN, inputFile ) == NULL )
        {
            if ( feof(inputFile) != 0 )
                return 0;
            fprintf(stderr, "Unable to get next line.\n");
            return -1;
        }
    } while ( string[0] == '#' || string[0] == '\n' || string[0] == ' ' );

    /* remove the trailing newline or space character from the buffer if it exists */
    size_t len = strlen( string )-1;
    if ( string[len] == '\n' || string[len] == ' ')
        string[len] = '\0';

    return 1;
}

