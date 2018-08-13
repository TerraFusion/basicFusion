/*

Modified by: Shashank Bansal
Email: sbansal6@illinois.edu

Date: July 14, 2017

*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#define STR_LEN 54
#define TIME_STR_LEN 20

#define FATAL_MSG( ... ) \
do { \
    fprintf(stderr,"[%s:%d] Fatal error: ",__FILE__,__LINE__); \
    fprintf(stderr, __VA_ARGS__); \
    } while(0)

#define WARN_MSG( ... ) \
do { \
    fprintf(stderr,"[%s:%d] Warning: ",__FILE__,__LINE__); \
    fprintf(stderr, __VA_ARGS__); \
    } while(0)


typedef struct OInfo {
        
    unsigned int orbit_number;
    unsigned short start_year;
    unsigned char  start_month;
    unsigned char start_day;
    unsigned char start_hour;
    unsigned char start_minute;
    unsigned char start_second;
    unsigned short end_year;
    unsigned char  end_month;
    unsigned char end_day;
    unsigned char end_hour;
    unsigned char end_minute;
    unsigned char end_second;

} OInfo_t;

void usage() {
    printf("\nUSAGE:  gcc -o read_time_new.o read_time_new.c\n");
    printf("\nOptions:\n\n");
    printf("-h                      More information about the different functions and how they work\n");
    printf("-a                      More information about the arguments and the format to run the program\n");
    printf("-r [orbitfile]          Run the program\n\n");

    return;

}

void help() {
    printf("\nDESCRIPTION:\nThere is 1 main functions in this program -\n");
    printf("1. covert2binary(String orbitfile):\n");
    printf("This function reads all the data from the orbitinfo textfile and stores it in a OInfo_t object. It then ");
    printf("writes all that information to a binary file.\n\n");

    return;

}

void arguments() {
    char info;
    
    printf("\nARGUMENTS TO RUN PROGRAM:\n\n./read_time_new.o -r [absolute/path/to/orbitfile]\n\n");
    printf("For more information on each argument, type i.\n");
    scanf("%c", &info);

    if(info == 'i' || info == 'I') {
    //if(strcmp(info, ) == 0 || strcmp(info, 'I') == 0) {
        
        printf("MORE INFORMATION: \n\nFor this program to run properly, you need only 1 argument.\n");
        printf("1. You need to give the path to the orbitfile. For example, it should look something like:");
        printf("\npath/to/orbitfile or /projects/TDataFus/gyzhao/TF/data/orbits/orbitfile.txt\n");
        printf("In order for the program to run smoothly, you need to provide the absolute path to the file.\n\n");

        return;

    }

    else return;
}

/* /home/sbansal6/File_Verification_Scripts/scripts/Orbit_Path_Time.txt  */ 



/* This is the function that generates the orbit_info.bin file */
int convert2binary(char* orbitfile) {

    OInfo_t cur_o_info;
    OInfo_t* o_info_ptr;
    
    int i =0;
    int o_total_number = 0;
    int t_index = 0;
    int orbit_num_pos = 0;
    int orbit_start_time_pos = 0;
    int orbit_end_time_pos = 0;

    char* temp_orbit_number;
    char sy_str[5];
    char smo_str[3];
    char sd_str[3];
    char sh_str[3];
    char smi_str[3];
    char ss_str[3];

    char ey_str[5];
    char emo_str[3];
    char ed_str[3];
    char eh_str[3];
    char emi_str[3];
    char es_str[3];
    
    char string[STR_LEN]; //this stores the orbit number from the orbitfile


    
    /* This opens the orbittotimelist.txt file that contains the start and 
    end date of all the orbits. The path to the orbitfile is given as the 
    first argument */
    
    FILE *const t_orbit_info = fopen(orbitfile, "r");

    
    /* error handling for orbittotimelist.txt */
    if(t_orbit_info == NULL) {
        
        FATAL_MSG("The orbit info does not exist. Check filename or path\n");
        return 0;
    }

    
   /* o_total_number counts the total number of orbits in the text file */
    while(fgets(string, STR_LEN, t_orbit_info)) {
        o_total_number++;
    }

    fclose(t_orbit_info);


    /* o_total_number number of objects allocated each of size OInfo_t */
    o_info_ptr = (OInfo_t*) calloc(o_total_number, sizeof(OInfo_t));

    
    FILE*const orbit_info = fopen(orbitfile, "r");
    
    char orbit_start_time[TIME_STR_LEN + 1];
    char orbit_end_time[TIME_STR_LEN + 1];
    
    char* temp_str = NULL;
    char* temp_time = NULL;

    memset(string, 0, STR_LEN);
    
    o_total_number = 0;
    
    

    /* this gets the orbit number from the text file and sets the length of the
    orbit number */
    while(fgets(string, STR_LEN, orbit_info)) {

        for(i = 0; i < STR_LEN; i++) {
            if(string[i] == ' '){
                orbit_num_pos = i;
               break;
            }
    
        }


        /* This allocates the length of the orbit number to temp_orbit_number */
        temp_orbit_number = malloc(orbit_num_pos + 1);
        
       
        /* Copies the orbit number into temp_orbit_number */
        strncpy(temp_orbit_number, string, orbit_num_pos);
        temp_orbit_number[orbit_num_pos] = '\0';


        /* This converts the orbit number from a string to the integer value */
        o_info_ptr[t_index].orbit_number = (unsigned int)strtol(temp_orbit_number, NULL, 10);


        free(temp_orbit_number);
        

        /* Setting the Orbit_Start_Time position which is the second ' ' */
        for(i = orbit_num_pos + 1; i< STR_LEN; i++) {
            if(string[i] == ' ') {
                orbit_start_time_pos = i;
                break;
            }
        }


        /* ERROR HANDLING FOR START_TIME AND END_TIME
        This is to make sure that the position of the pointers are correct for 
           start_time and end_time */

        if(string[orbit_start_time_pos + 1] != '2') {
            fclose(orbit_info);
            FATAL_MSG("The orbit start-time position is not right\n");
            return -1;
        }

        /* Setting up the pointer position for the orbit_end_time */

        for(i = orbit_start_time_pos + 1; i< STR_LEN; i++) {
            if(string[i] == ' ') {
                orbit_end_time_pos = i;
                break;
            }
        }

        if(string[orbit_end_time_pos + 1] != '2') {
            fclose(orbit_info);
            FATAL_MSG("The orbit end-time position is not right\n");
            return -1;
        }

        temp_str = string + orbit_start_time_pos + 1;

        strncpy(orbit_start_time, (const char*)temp_str, TIME_STR_LEN);
        orbit_start_time[TIME_STR_LEN] = '\0';
        


        /* ADDING INFO TO THE OINFO Object */

        /* THE FOLLOWING GETS ALL THE STARTING TIME DATA */
        
        /* Gets the starting year */   
        strncpy(sy_str, (const char*)orbit_start_time, 4);    
        sy_str[4] = '\0';
        o_info_ptr[t_index].start_year = (unsigned short)strtol(sy_str, NULL, 10);
        //printf("starting year is %d\n",(unsigned short)strtol(sy_str,NULL,10));
        
        
        /* Gets the starting month */
        temp_time = orbit_start_time + 5;
        strncpy(smo_str,(const char*)temp_time, 2);
        smo_str[2] = '\0';
        o_info_ptr[t_index].start_month = (unsigned char)strtol(smo_str, NULL, 10);
        //printf("starting month is %d\n",(unsigned char)strtol(smo_str,NULL,0));

        
        /* Gets the starting date*/
        temp_time = orbit_start_time + 8;
        strncpy(sd_str, (const char*)temp_time, 2); 
        sd_str[2] = '\0';
        o_info_ptr[t_index].start_day = (unsigned char)strtol(sd_str, NULL, 10);
        //printf("starting date is %d\n",(unsigned char)strtol(sd_str,NULL,0));

        
        /* Gets the starting time*/
        temp_time = temp_time + 3;
        strncpy(sh_str, (const char*)temp_time, 2); 
        sh_str[2] = '\0';
        o_info_ptr[t_index].start_hour = (unsigned char)strtol(sh_str, NULL, 10);
        //printf("starting hour is %d\n",(unsigned char)strtol(sh_str,NULL,0));


        /* Gets the starting minute*/
        temp_time = temp_time + 3;
        strncpy(smi_str, (const char*)temp_time, 2); 
        smi_str[2] = '\0';
        o_info_ptr[t_index].start_minute = (unsigned char)strtol(smi_str, NULL, 10);


        
        /* Gets the starting second*/
        temp_time = temp_time + 3;
        strncpy(ss_str, (const char*)temp_time, 2); 
        ss_str[2] = '\0';
        o_info_ptr[t_index].start_second = (unsigned char)strtol(ss_str, NULL, 10);



       
        /* THE FOLLOWING GETS ALL THE ENDING TIME DATA */

        temp_str = string + orbit_end_time_pos + 1;
        strncpy(orbit_end_time, (const char*)temp_str, TIME_STR_LEN);
        orbit_end_time[TIME_STR_LEN]='\0';
        

        /* Gets the ending year */
        strncpy(ey_str, (const char*)orbit_end_time, 4);
        ey_str[4] = '\0';
        o_info_ptr[t_index].end_year = (unsigned short)strtol(ey_str, NULL, 10);
        

        
        /* Gets the ending month */
        temp_time = orbit_end_time + 5;
        strncpy(emo_str, (const char*)temp_time, 2);
        emo_str[2] = '\0';
        o_info_ptr[t_index].end_month = (unsigned char)strtol(emo_str, NULL, 10);

        

        /* Gets the ending day */
        temp_time = orbit_end_time + 8;
        strncpy(ed_str, (const char*)temp_time, 2); 
        ed_str[2] = '\0';
        o_info_ptr[t_index].end_day = (unsigned char)strtol(ed_str,NULL,10);



        /* Gets the ending hour*/
        temp_time = temp_time + 3;
        strncpy(eh_str,(const char*)temp_time,2); 
        eh_str[2]='\0';
        o_info_ptr[t_index].end_hour = (unsigned char)strtol(eh_str,NULL,10);


        
        /* Gets the ending minute */
        temp_time = temp_time + 3;
        strncpy(emi_str,(const char*)temp_time,2); 
        emi_str[2]='\0';
        o_info_ptr[t_index].end_minute = (unsigned char)strtol(emi_str,NULL,10);



        /* Gets the ending second */
        temp_time = temp_time + 3;
        strncpy(es_str,(const char*)temp_time,2); 
        es_str[2]='\0';
        o_info_ptr[t_index].end_second = (unsigned char)strtol(es_str,NULL,10);



        //o_info_ptr
        memset(string, 0, STR_LEN);
        
        t_index++;
    
    } //while loop ends here


    // for(i = 0; i < 10000; i++) {
        
    //     printf("orbit info[%d].orbit_number=%d\n",i,o_info_ptr[i].orbit_number);

    //     printf("orbit info[%d].start_year=%d\n",i,(int)(o_info_ptr[i].start_year));
    //     printf("orbit info[%d].start_month=%d\n",i,(int)(o_info_ptr[i].start_month));
    //     printf("orbit info[%d].start_day=%d\n",i,(int)(o_info_ptr[i].start_day));
    //     printf("orbit info[%d].start_hour=%d\n",i,(int)(o_info_ptr[i].start_hour));
    //     printf("orbit info[%d].start_minute=%d\n",i,(int)(o_info_ptr[i].start_minute));
    //     printf("orbit info[%d].start_second=%d\n",i,(int)(o_info_ptr[i].start_second));

    //     printf("orbit info[%d].end_year=%d\n",i,(int)(o_info_ptr[i].end_year));
    //     printf("orbit info[%d].end_month=%d\n",i,(int)(o_info_ptr[i].end_month));
    //     printf("orbit info[%d].end_day=%d\n",i,(int)(o_info_ptr[i].end_day));
    //     printf("orbit info[%d].end_hour=%d\n",i,(int)(o_info_ptr[i].end_hour));
    //     printf("orbit info[%d].end_minute=%d\n",i,(int)(o_info_ptr[i].end_minute));
    //     printf("orbit info[%d].end_second=%d\n",i,(int)(o_info_ptr[i].end_second));
    // }


    /* Writing all the information to a binary file*/
    FILE * orbit_info_b = fopen("orbit_info.bin", "w+");
    fwrite(o_info_ptr, sizeof(OInfo_t), (size_t)t_index, orbit_info_b);
    fclose(orbit_info_b);

    
    /* Checking the binary file size */
    FILE* new_orbit_info_b = fopen("orbit_info.bin", "r");
    long fSize;
    fseek(new_orbit_info_b, 0, SEEK_END);
    fSize = ftell(new_orbit_info_b);
    rewind(new_orbit_info_b);


    OInfo_t* test_orbit_ptr = NULL;
    test_orbit_ptr = calloc(fSize/sizeof(OInfo_t), sizeof(OInfo_t));
    size_t result = fread(test_orbit_ptr, sizeof(OInfo_t), fSize/sizeof(OInfo_t), new_orbit_info_b);
    
    if(result != fSize/sizeof(OInfo_t)) {
        free(test_orbit_ptr);
        free(o_info_ptr);
        fclose(orbit_info);
        fclose(new_orbit_info_b);
        WARN_MSG("Memory deallocation was un-successful.\n");
        return -1;

    }

    // for(i = 0; i<fSize/sizeof(OInfo_t);i++) {

    // printf("orbit info[%d].orbit_number=%d\n",i,test_orbit_ptr[i].orbit_number);

    // printf("orbit info[%d].start_year=%d\n",i,(int)(test_orbit_ptr[i].start_year));
    // printf("orbit info[%d].start_month=%d\n",i,(int)(test_orbit_ptr[i].start_month));
    // printf("orbit info[%d].start_day=%d\n",i,(int)(test_orbit_ptr[i].start_day));
    // printf("orbit info[%d].start_hour=%d\n",i,(int)(test_orbit_ptr[i].start_hour));
    // printf("orbit info[%d].start_minute=%d\n",i,(int)(test_orbit_ptr[i].start_minute));
    // printf("orbit info[%d].start_second=%d\n",i,(int)(test_orbit_ptr[i].start_second));


    // printf("orbit info[%d].end_year=%d\n",i,(int)(test_orbit_ptr[i].end_year));
    // printf("orbit info[%d].end_month=%d\n",i,(int)(test_orbit_ptr[i].end_month));
    // printf("orbit info[%d].end_day=%d\n",i,(int)(test_orbit_ptr[i].end_day));
    // printf("orbit info[%d].end_hour=%d\n",i,(int)(test_orbit_ptr[i].end_hour));
    // printf("orbit info[%d].end_minute=%d\n",i,(int)(test_orbit_ptr[i].end_minute));
    // printf("orbit info[%d].end_second=%d\n",i,(int)(test_orbit_ptr[i].end_second));


    // }

    free(test_orbit_ptr);
    free(o_info_ptr);
    fclose(orbit_info);
    fclose(new_orbit_info_b);

    return 0;
}

int main(int argc, char *argv[]) {

    //convert2binary(argv[1]);

    if(argc == 1) 
        usage();
    
    else if(strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "-H") == 0)
        help();

    else if(strcmp(argv[1], "-a") == 0 || strcmp(argv[1], "-A") == 0)
        arguments();

    else if(argc == 3) {
        if(strcmp(argv[1], "-r") == 0 || strcmp(argv[1], "-R") == 0)
            convert2binary(argv[2]);
        else
            usage();
    }

    else
        usage();


}