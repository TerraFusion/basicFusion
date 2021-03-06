"""
@author : Yat Long Lo
@email : yllo2@illinois.edu

PROGRAM DESCRIPTION:

    This is the validation module for the TERRA Basic Fusion project. Validation functionalities include:
        1. MODIS continuity check
        2. CERES continuity check
        3. CERES orbit-based file location check
        4. MOPITT orbit-based file location check

"""
import os
import sys
from datetime import datetime
from datetime import timedelta

#Constants for different checks
MODIS_TIME_SEP = 5
MODIS_1KM_NAME = "MOD021KM"
CERES_FM1_NAME = "CERES/FM1"
CERES_FM2_NAME = "CERES/FM2"

#Check continuity of MODIS files
def modis_cont_check(ipaths_file, modis_fs_list = None):  
    
    """
                    modis_cont_check
    DESCRIPTION:
        This function takes the BasicFusion input files list and MODIS file system list and looks for discontinuity
        in the input files list. If there's discontinuity, it will look for the file in the MODIS file system list.
        If that file exists in the file system, the distontinuity is valid and would be reported. If the file system
        list is provided, it would use the list. Else, it would look at the actual file system. The first argument is
        a file pointer to the BasicFusion input files which SHOULD not be closed.
        
    ARGUMENTS:
        0. ipaths_file -- This argument is the file pointer to the BasicFusion input files list
        1. modis_fs_list -- This argument is a list of MODIS 1KM files that exist in the file system
        
    EFFECTS:
        The ipath_file variable would point to the end of the file after this function. Any potential MODIS distontinuity would
        be printed out.
        
    RETURN:
        Nothing will be returned
        
    """    

   #List of modis file paths
    modis_filenames = []
    modis_dict = {}
    modis_head_dict = {}
    missing_dict = {}
    #Get modis filenames from input files list
    for ipath in ipaths_file:
        input_file = open(ipath.replace("\n", ""), 'r')
        for file_path in input_file:
            if(MODIS_1KM_NAME in file_path):
                tokens = file_path.split('/')
                #Truncate absolute path to filename
                modis_filenames.append(tokens[len(tokens)-1])
                fname = tokens[len(tokens) - 1]
                #Store the absolute path of the input file that the file is in, with the filename as key for the dictionary
                modis_dict[fname] = ipath
                del tokens[-1]
                #Store the absolute path in the dictionary with filename as key
                modis_head_dict[fname] = "/".join(tokens)
        input_file.close()
    
    #Remove duplicated filenames and store filenames in order
    modis_unique_files = []
    for file in modis_filenames:
        if(file not in modis_unique_files):
            modis_unique_files.append(file)
    
    print("Number of MODIS files found: " + str(len(modis_unique_files)))
    
    #Check filenames 2 by 2 to make sure they are 5 minutes apart. Head is the first file, tail is the second file
    head_index = 0
    tail_index = 1
    while(tail_index < len(modis_unique_files)):
        #Get filenames and timestamps
        head_file = modis_unique_files[head_index]
        tail_file = modis_unique_files[tail_index]
        hfn_tokens = head_file.split('.')
        hf_time_string = hfn_tokens[1].replace('A','') + '.' + hfn_tokens[2]
        tfn_tokens = tail_file.split('.')
        tf_time_string = tfn_tokens[1].replace('A','') + '.' + tfn_tokens[2]
        head_time = datetime.strptime(hf_time_string,"%Y%j.%H%M")
        tail_time = datetime.strptime(tf_time_string,"%Y%j.%H%M")
        #If they are more than 5 minutes apart, dicontinuity is found
        if(head_time + timedelta(minutes=5) != tail_time):
            moving_head_time = head_time + timedelta(minutes=5)
            #keep adding to moving head time until it reaches tail time, which indicates the end of discontinuity
            while(moving_head_time != tail_time):
                #search whether file in any of the path
                candidate_time_string = moving_head_time.strftime("%Y%j.%H%M")
                #Form the filename string of the missing candidate for file system search
                candidate_filename = MODIS_1KM_NAME + '.' + 'A' + candidate_time_string
                if(modis_fs_list is None):
                    #Search from actual file system, as file list(modis_fs_list) does not exist
                    #Get the absolute path to the directory the file is in
                    head_path = modis_head_dict[head_file]
                    tail_path = modis_head_dict[tail_file]
                    if(head_path == tail_path):
                        #Only search one path if head and tail paths are the same
                        arr = os.listdir(head_path)
                        for file in arr:
                            if(candidate_filename in file):
                                #Record the possible input files that this candidate should be in, using the filename as key
                                missing_dict[candidate_filename] = modis_dict[head_file] + " or " + modis_dict[tail_file]
                    else:
                        #Get content of the directories of head and tail
                        head_arr = os.listdir(head_path)
                        tail_arr = os.listdir(tail_path)
                        found = False;
                        #Only search look at tail if it's not in head
                        for file in head_arr:
                            if(candidate_filename in file):
                                #Record the possible input files that this candidate should be in, using the filename as key
                                missing_dict[candidate_filename] = modis_dict[head_file]
                                found = True
                        if(not found):
                            for file in tail_arr:
                                if(candidate_filename in file):
                                    #Record the possible input files that this candidate should be in, using the filename as key
                                    missing_dict[candidate_filename] = modis_dict[tail_file]
                else:
                    #the file list exists, search from the list
                    found = False
                    #Check if the file exists in file list
                    for f in modis_fs_list:
                        if(candidate_filename in f):
                            found = True
                            break
                    if(found):
                        head_in = modis_dict[head_file]
                        tail_in = modis_dict[tail_file]
                        if(head_in == tail_in):
                            #Record the possible input files that this candidate should be in, using the filename as key
                            missing_dict[candidate_filename] = head_in
                        else:
                            #Record the possible input files that this candidate should be in, using the filename as key
                            missing_dict[candidate_filename] = head_in + " or " + tail_in
                #Update moving head time for next possible candidate
                moving_head_time = moving_head_time + timedelta(minutes=5)
        #Move to the next window of 2 filenames
        head_index += 1
        tail_index += 1
    #Print out files that are missing 
    for k, v in missing_dict.items():
        print("Missing: " + k)
        print("Suspected input location: " + v)
    print("Number of MODIS missing files: " + str(len(missing_dict)))


#Check continuity of CERES files
def ceres_cont_check(ipaths_file, ceres1_fs_list = None, ceres2_fs_list = None):
    """
                ceres_cont_check
    DESCRIPTION:
        This function takes in the BasicFusion input files list and file system list for each camera in CERES(FM1 and FM2)
        . It looks for discontinuity in the input files list. If there's discontinuity, it will look for the file in the 
        CERES file system list. If that file exists in the file system, the distontinuity is valid and would be reported. 
        If the file system list is provided, it would use the list. Else, it would look at the actual file system. The first 
        argument is a file pointer to the BasicFusion input files which SHOULD not be closed.
        
    ARGUMENTS:
        0. ipaths_file -- This argument is the file pointer to the BasicFusion input files list
        1. ceres1_fs_list -- This argument is a list of CERES FM1 files that exist in the file system
        2. ceres2_fs_list -- This argument is a list of CERES FM2 files that exist in the file system
        
    EFFECTS:
        The ipath_file variable would point to the end of the file after this function. Any potential CERES distontinuity would
        be printed out.
        
    RETURN:
        Nothing will be returned
        
    """
    #List of CERES file paths
    ceres_fm1_filenames = []
    ceres_fm2_filenames = []
    #Head lists store the paths to each CERES files found
    ceres_1_heads = []
    ceres_2_heads = []
    ceres_uniq1_heads = []
    ceres_uniq2_heads = []
    ceres_dict = {}
    #Get ceres file names
    for ipath in ipaths_file:
        input_file = open(ipath.replace("\n", ""), 'r')
        for file_path in input_file:
            tokens = file_path.split('/')
            #String constants are used to identify CERES files of different cameras
            if(CERES_FM1_NAME in file_path):
                ceres_fm1_filenames.append(tokens[len(tokens)-1].replace('\n', ''))
                ceres_dict[tokens[len(tokens)-1].replace('\n', '')] = ipath.replace('\n','')
                del tokens[-1]
                ceres_1_heads.append("/".join(tokens))
            elif(CERES_FM2_NAME in file_path):
                ceres_fm2_filenames.append(tokens[len(tokens)-1].replace('\n', ''))
                ceres_dict[tokens[len(tokens)-1].replace('\n', '')] = ipath.replace('\n', '')
                del tokens[-1]
                ceres_2_heads.append("/".join(tokens))
        input_file.close()
    ceres_uniq1_heads = list(set(ceres_1_heads))
    ceres_uniq2_heads = list(set(ceres_2_heads))
    ceres_fm1_filenames = sorted(list(set(ceres_fm1_filenames)))
    ceres_fm2_filenames = sorted(list(set(ceres_fm2_filenames)))
    
    print("Number of CERES FM1 files found: " + str(len(ceres_fm1_filenames)))
    print("Number of CERES FM2 files found: " + str(len(ceres_fm2_filenames)))    
    
    #Check filenames 2 by 2 to make sure they are 1 hour apart. Head is the first file, tail is the second file
    head_index_1 = 0
    tail_index_1 = 1
    confirmed_dict1 = {}
    while(tail_index_1 < len(ceres_fm1_filenames)):
        #Get file names from index
        head_file = ceres_fm1_filenames[head_index_1]
        tail_file = ceres_fm1_filenames[tail_index_1]
        #Get the absolute path to the directory the head and tail files are in
        head_head = head_file.split('.')[0]
        tail_head = head_file.split('.')[0]
        #Extract their timestamps
        head_timestring = head_file.split('.')[-1].replace('\n','')
        tail_timestring = tail_file.split('.')[-1].replace('\n','')
        head_time = datetime.strptime(head_timestring,"%Y%m%d%H")
        tail_time = datetime.strptime(tail_timestring,"%Y%m%d%H")
        if(head_time + timedelta(hours=1) != tail_time):
            #If they are more than 1 hour apart, dicontinuity is found
            moving_head_time = head_time + timedelta(hours=1)
            #keep adding to moving head time until it reaches tail time, which indicates the end of discontinuity
            while(moving_head_time != tail_time):
                candidate_time_string = moving_head_time.strftime("%Y%m%d%H")
                #Form missing candidate filename
                candidate_filename = head_head + '.'  + candidate_time_string
                if(ceres1_fs_list is None):
                    #CERES FM1 files list is missing, search the file system
                    #search the possible directories one by one
                    for head in ceres_uniq1_heads:
                        head_arr = os.listdir(head)
                        #for storing input file names before and after 
                        prev_in = None
                        nxt_in = None
                        if(candidate_filename in head_arr):
                            date_token = candidate_filename.split('.')[-1]
                            date_string = (date_token[0:4] + "-" + date_token[4:6] + "-" + date_token[6:8] + "-" + date_token[8:]).replace('\n', '')
                            #Retrieve timestamps before and after
                            prev = datetime.strptime(date_string, '%Y-%m-%d-%H') - timedelta(hours=1)
                            nxt = datetime.strptime(date_string, '%Y-%m-%d-%H') + timedelta(hours=1)
                            prev_string = prev.strftime('%Y%m%d%H')
                            nxt_string = nxt.strftime('%Y%m%d%H')
                            prev_in = ceres_dict[candidate_filename.split('.')[0] + '.' + prev_string]
                            nxt_in = ceres_dict[candidate_filename.split('.')[0] + '.' + nxt_string]
                            #Add potential input file names if the file is found in the file system
                            if(prev_in is None):
                                confirmed_dict1[head + "/" + candidate_filename] = prev_in
                            elif(nxt_in is None):
                                confirmed_dict1[head + "/" + candidate_filename] = nxt_in
                            else:
                                confirmed_dict1[head + "/" + candidate_filename] = prev_in + " or " + nxt_in
                            break
                else:
                    #file list exists
                    found = False
                    #Check if the candidate is found in the file list
                    for f in ceres1_fs_list:
                        if(candidate_filename in f):
                            found = True
                            break
                    if(found):
                        #If file is found in the list, retrieve the possible input file location and record it 
                        head_in = ceres_dict[head_file]
                        tail_in = ceres_dict[tail_file]
                        if(head_in == tail_in):
                            confirmed_dict1[candidate_filename] = head_in
                        else:
                            confirmed_dict1[candidate_filename] = head_in + " or " + tail_in
                #Update the moving head time for next possible missing candidate
                moving_head_time = moving_head_time + timedelta(hours=1)
        #Move to the next window of 2 filenames
        head_index_1 += 1
        tail_index_1 += 1
                    
    #Check filenames 2 by 2 to make sure they are 1 hour apart. Head is the first file, tail is the second file
    head_index_2 = 0
    tail_index_2 = 1
    confirmed_dict2 = {}
    while(tail_index_2 < len(ceres_fm2_filenames)):
        #Get file names from index
        head_file = ceres_fm2_filenames[head_index_2]
        tail_file = ceres_fm2_filenames[tail_index_2]
        #Get the absolute path to the directory the head and tail files are in
        head_head = head_file.split('.')[0]
        tail_head = head_file.split('.')[0]
        #Extract their timestamps
        head_timestring = head_file.split('.')[-1].replace('\n','')
        tail_timestring = tail_file.split('.')[-1].replace('\n','')
        head_time = datetime.strptime(head_timestring,"%Y%m%d%H")
        tail_time = datetime.strptime(tail_timestring,"%Y%m%d%H")
        if(head_time + timedelta(hours=1) != tail_time):
            #If they are more than 1 hour apart, dicontinuity is found
            moving_head_time = head_time + timedelta(hours=1)
            #keep adding to moving head time until it reaches tail time, which indicates the end of discontinuity
            while(moving_head_time != tail_time):
                candidate_time_string = moving_head_time.strftime("%Y%m%d%H")
                #Form missing candidate filename
                candidate_filename = head_head + '.'  + candidate_time_string
                if(ceres2_fs_list is None):
                    #CERES FM2 files list is missing, search the file system
                    #search the possible directories one by one
                    for head in ceres_uniq2_heads:
                        head_arr = os.listdir(head)
                        #for storing input file names before and after 
                        prev_in = None
                        nxt_in = None
                        if(candidate_filename in head_arr):
                            date_token = candidate_filename.split('.')[-1]
                            date_string = (date_token[0:4] + "-" + date_token[4:6] + "-" + date_token[6:8] + "-" + date_token[8:]).replace('\n', '')
                            #Retrieve timestamps before and after
                            prev = datetime.strptime(date_string, '%Y-%m-%d-%H') - timedelta(hours=1)
                            nxt = datetime.strptime(date_string, '%Y-%m-%d-%H') + timedelta(hours=1)
                            prev_string = prev.strftime('%Y%m%d%H')
                            nxt_string = nxt.strftime('%Y%m%d%H')
                            prev_in = ceres_dict[candidate_filename.split('.')[0] + '.' + prev_string]
                            nxt_in = ceres_dict[candidate_filename.split('.')[0] + '.' + nxt_string]
                            #Add potential input file names if the file is found in the file system
                            if(prev_in is None):
                                confirmed_dict2[head + "/" + candidate_filename] = prev_in
                            elif(nxt_in is None):
                                confirmed_dict2[head + "/" + candidate_filename] = nxt_in
                            else:
                                confirmed_dict2[head + "/" + candidate_filename] = prev_in + " or " + nxt_in
                            break
                else:
                    #file list exists
                    found = False
                    #Check if the candidate is found in the file list
                    for f in ceres2_fs_list:
                        if(candidate_filename in f):
                            found = True
                            break
                    if(found):
                        #If file is found in the list, retrieve the possible input file location and record it 
                        head_in = ceres_dict[head_file]
                        tail_in = ceres_dict[tail_file]
                        if(head_in == tail_in):
                            confirmed_dict2[candidate_filename] = head_in
                        else:
                            confirmed_dict2[candidate_filename] = head_in + " or " + tail_in
                #Update the moving head time for next possible missing candidate
                moving_head_time = moving_head_time + timedelta(hours=1)
        #Move to the next window of 2 filenames
        head_index_2 += 1
        tail_index_2 += 1
    
    #Print out the confirmed discontinuity information
    for k, v in confirmed_dict1.items():
        print("Missing: " + k)
        print("Suspected input location: " + v)
        
    for k, v in confirmed_dict2.items():
        print("Missing: " + k)
        print("Suspected input location: " + v)
    
    print("Number of missing CERES FM1 files: " + str(len(confirmed_dict1)))
    print("Number of missing CERES FM2 files: " + str(len(confirmed_dict2)))

#Check whether CERES files are in the correct orbit        
def ceres_orbit_check(ipaths_file, otime_file):
    """
                ceres_orbit_check
    DESCRIPTION:
        This function takes in the BasicFusion input files list and the orbit time file (which shows the start and end time
        of an orbit). It checks whether each CERES file are in the correct orbit by comparing its start and end time to the 
        orbit's start and end time. It is considered correct as long as there's overlapping between the times. The first and 
        second arguments are file pointers to the BasicFusion input files and orbit time file which SHOULD not be closed.
        
    ARGUMENTS:
        0. ipaths_file -- This argument is the file pointer to the BasicFusion input files list
        1. otime_file -- This argument is the file pointer to the orbit time file
        
    EFFECTS:
        The ipath_file variable would point to the end of the file after this function. Any potential misplacement of CERES files
        in the wrong orbit will be outputted 
        
    RETURN:
        Nothing will be returned
        
    """
    wrong_files = []
    for ipath in ipaths_file:
        #Extract orbit number from the input file paths
        orbit = ipath.split('/')[-1].replace('input','').replace('.txt', '').replace('\n', '')
        o_start_time = None
        o_end_time = None
        ceres_files = []
        #Extract orbit's start and end time from the orbit information text file
        for oinfo in otime_file:
            if(orbit in oinfo):
                o_split_infos = oinfo.split(' ') 
                o_start_time = datetime.strptime(o_split_infos[2],'%Y-%m-%dT%H:%M:%SZ')
                o_end_time = datetime.strptime(o_split_infos[3].replace('\r\n',''),'%Y-%m-%dT%H:%M:%SZ')
        input_file = open(ipath.replace("\n", ""), 'r')
        #Get CERES files
        for file_path in input_file:
            #A string is used as an indicator of CERES file
            if("CERES" in file_path):
                ceres_files.append(file_path)
        #Check whether they are in the correct place
        for ceres_f in ceres_files:
            file_name = ceres_f.split('/')[-1].replace('\n','')
            #Get the file start time from the filename 
            start_time = datetime.strptime(file_name.split('.')[-1], '%Y%m%d%H')
            end_time = start_time + timedelta(hours=1)
            #If there is overlapping in time, the file is considered as in the correct place
            if((o_start_time <= start_time and start_time <= o_end_time) or (o_start_time <= end_time and end_time <= o_end_time)):
                continue
            else:
                wrong_files.append((ipath, ceres_f))
        otime_file.seek(0)
        input_file.close()
    
    #Print out files that are in the wrong orbit
    for tup in wrong_files:
        print('Wrongly placed location: ' + tup[0])
        print('filename: ' + tup[1])
    print("Number of CERES files in the wrong orbit: " + str(len(wrong_files)))


#Check whether MOPITT files are in the correct orbit
def mop_orbit_check(ipaths_file, otime_file):
    
    """
                mop_orbit_check
    DESCRIPTION:
        This function takes in the BasicFusion input files list and the orbit time file (which shows the start and end time
        of an orbit). It checks whether each MOPITT file are in the correct orbit by comparing its start and end time to the 
        orbit's start and end time. It is considered correct as long as there's overlapping between the times. The first and 
        second arguments are file pointers to the BasicFusion input files and orbit time file which SHOULD not be closed.
        
    ARGUMENTS:
        0. ipaths_file -- This argument is the file pointer to the BasicFusion input files list
        1. otime_file -- This argument is the file pointer to the orbit time file
        
    EFFECTS:
        The ipath_file variable would point to the end of the file after this function. Any potential misplacement of MOPITT files
        in the wrong orbit will be outputted 
        
    RETURN:
        Nothing will be returned
        
    """

    
    wrong_files = []
    for ipath in ipaths_file:
        #Extract orbit number from the input file paths
        orbit = ipath.split('/')[-1].replace('input','').replace('.txt', '').replace('\n', '')
        o_start_time = None
        o_end_time = None
        mop_files = []
        #Extract orbit's start and end time from the orbit information text file
        for oinfo in otime_file:
            if(orbit in oinfo):
                o_split_infos = oinfo.split(' ') 
                o_start_time = datetime.strptime(o_split_infos[2],'%Y-%m-%dT%H:%M:%SZ')
                o_end_time = datetime.strptime(o_split_infos[3].replace('\r\n',''),'%Y-%m-%dT%H:%M:%SZ')
        input_file = open(ipath.replace("\n", ""), 'r')
        #Get MOPITT files
        for file_path in input_file:
             #A string is used as an indicator of MOPITT file
            if("MOPITT" in file_path):
                mop_files.append(file_path)
        #Check whether they are in the correct place
        for mop_f in mop_files:
            file_name = mop_f.split('/')[-1].replace('\n','')
            #Get the file start time from the filename 
            start_time = datetime.strptime(file_name.split('-')[1], '%Y%m%d')
            end_time = start_time + timedelta(days=1)
            #If there is overlapping in time, the file is considered as in the correct place
            if((o_start_time <= start_time and start_time <= o_end_time) or (o_start_time <= end_time and end_time <= o_end_time) or (start_time < o_start_time and end_time >= o_end_time)):
                continue
            else:
                wrong_files.append((ipath, mop_f))
        otime_file.seek(0)
        input_file.close()
    
    #Print out files that are in the wrong orbit
    for tup in wrong_files:
        print('Wrongly placed location: ' + tup[0])
        print('filename: ' + tup[1])
    print("Number of MOPITT files in the wrong orbit: " + str(len(wrong_files)))
        
#####Assumption: input_file_paths are ordered temporally 
def main():
    
    """
                    main
    DESCRIPTION:
        This function calls the respective validation check functions. It first parses the file system list int to different lists
        based on instruments and sorts each list alphabetically. They are then passed toeach functions accordingly.
    
    ARGUMENT:
        0. validation_mod.py -- program name
        1. input_file_paths.txt -- list of paths to the input files
        2. orbit_time.txt -- text file of the times of all orbits
        3. file_sys_list.txt -- text file of all existing files in the file system
    
    EFFECT: 
        Validation work would be performed. If file_sys_list is specified, it will perform validation on the list instead of the 
        actual file system
        
    RETURN:
        Nothing will be returned
    """
    
    nearline_mode = False
    fs_list = None
    modis_fs_list = []
    mop_fs_list = []
    ceres1_fs_list = []
    ceres2_fs_list = []
    if(len(sys.argv) < 3):
        print("Usage: ./validation_mod input_file_paths.txt orbit_time.txt [optional-only when nearline check]file_sys_list.txt")
        return
    if(len(sys.argv) == 4):
        #Open file system list
        fs_list = open(sys.argv[3], 'r') 
        for f in fs_list:
            #Store the file path in corresponding file list based on instruments
            if(MODIS_1KM_NAME in f):
                modis_fs_list.append(f)
            elif("MOPITT" in f):
                mop_fs_list.append(f)
            elif("/CERES/FM1" in f and ".met" not in f):
                ceres1_fs_list.append(f)
            elif("/CERES/FM2" in f and ".met" not in f):
                ceres2_fs_list.append(f)
        #Sort the lists alphabetically 
        modis_fs_list = sorted(modis_fs_list)
        mop_fs_list = sorted(mop_fs_list)
        ceres1_fs_list = sorted(ceres1_fs_list)
        ceres2_fs_list = sorted(ceres2_fs_list)
        fs_list.seek(0)
        #This means the search would be on the list instead of going over the file system
        nearline_mode = True
    
    #Open the file that contains paths to input files
    ipaths_file = open(sys.argv[1], 'r')
    #Open the file that contains orbit time
    otime_file = open(sys.argv[2], 'r')
    
    if(nearline_mode == False):
        #Perform CERES continuity check
        ceres_cont_check(ipaths_file)
        ipaths_file.seek(0)
        #Perform MODIS continuity check
        modis_cont_check(ipaths_file)
        ipaths_file.seek(0)
    else:
        #Perform CERES continuity check
        ceres_cont_check(ipaths_file, ceres1_fs_list, ceres2_fs_list)
        ipaths_file.seek(0)
        #Perform MODIS continuity check
        modis_cont_check(ipaths_file, modis_fs_list)
        ipaths_file.seek(0)
        fs_list.close()

    #Perform CERES orbital check
    ceres_orbit_check(ipaths_file, otime_file)
    ipaths_file.seek(0)
    #Perform MOPITT orbital check
    mop_orbit_check(ipaths_file, otime_file)
    ipaths_file.seek(0)
    
    
    #Close opened resources
    ipaths_file.close()
    otime_file.close()
    
main()