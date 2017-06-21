import sys, os

dir_dict = {'1': 'MOD021KM', 'H' : 'MOD02HKM', 'Q' : 'MOD02QKM', '3' : 'MOD03'}
error_list = {}
warning_list = []
op_dir_names = []
NUM_ORBITAL_PATHS = 345
mod_paths = {}
#err1file = open('err1', 'w')
#errhfile = open('errh', 'w')
#errqfile = open('errq', 'w')
#err3file = open('err3', 'w')
#errpathfile = open('errpath', 'w')
total_err = open('modis_missing_list', 'w')
warnings = open('modis_warning_list', 'w')

def init_error_list():
    for key in dir_dict.keys():
        error_list[key] = []
    error_list['P'] = []

def init_orbital_dir_names():
    for i in range(NUM_ORBITAL_PATHS):
        op_dir_names.append("{0:0>3}".format(i+1))
 
def init_file_paths():
    for key in dir_dict.keys():
        mod_paths[key] = str(dir_dict[key]) + "/" + str(year)
    print(str(mod_paths))

def get_file_identifier(filename):
    #print(filename)
    dot_count = 0
    idx = 0
    sdx = 0
    edx = 0
    for c in filename:
        if(c == "."):
            if(sdx == 0):
                sdx = idx
            dot_count += 1
        if(dot_count == 3):
            edx = idx
            break
        idx += 1
    identifier = filename[sdx:edx]
    #print(identifier)
    return identifier

def check_directory(key, path_num, identifier):
    msg = None
    check_o_path = mod_paths[key] + '/' + str(path_num)
    check_o_file = dir_dict[key] + identifier
    if(os.path.isdir(check_o_path)):
        files = os.listdir(check_o_path)
        isExist = False
        for f in files:
            if check_o_file in f:
                #print(os.stat(check_o_path + '/'  + f))
                if(os.stat(check_o_path + '/'  + f).st_size != 0):
                    #print(check_o_path + '/'  + f)
                    isExist = True
                    break
            else:
                continue
        if(isExist):
            return True, msg
        else:
            msg = check_o_path + '/' + check_o_file
            print('missing: ' + msg)
            return False, msg
    else:
        msg = check_o_path 
        return False, msg

def file_search(curr_key, path_num, identifier):
    #Decide which way to search
    if(curr_key == '1'):
        hIsExist, h_msg = check_directory('H', path_num, identifier)
        if(hIsExist):
            IsQExist, q_msg = check_directory('Q', path_num, identifier)
            Is3Exist, msg3 = check_directory('3', path_num, identifier)
            if(IsQExist == False):
                error_list[curr_key].append(q_msg)
                #print(curr_key + 'H: ' + q_msg)
            if(Is3Exist == False):
                error_list[curr_key].append(msg3)
                #print(curr_key + 'H: ' + msg3)
        else:
            Is3Exist, msg3 = check_directory('3', path_num, identifier)
            if(Is3Exist == False):
                IsQExist, q_msg = check_directory('Q', path_num, identifier)
                if(IsQExist):
                    error_list[curr_key].append(h_msg)
                    error_list[curr_key].append(msg3)
                else:
                    error_list[curr_key].append(msg3)
                    warning_list.append(h_msg)
                    warning_list.append(q_msg)
                #print(curr_key +'3: ' + msg3)
    elif(curr_key == 'H'):
        IsQExist, q_msg = check_directory('Q', path_num, identifier)
        Is3Exist, msg3 = check_directory('3', path_num, identifier)
        Is1Exist, msg1 = check_directory('1', path_num, identifier)
        if(IsQExist == False):
            error_list[curr_key].append(q_msg)
            #print(curr_key + ': ' + q_msg)
        if(Is3Exist == False):
            error_list[curr_key].append(msg3)
            #print(curr_key + ': ' + msg3)
        if(Is1Exist == False):
            error_list[curr_key].append(msg1)
            #print(curr_key + ': ' + msg1)
    elif(curr_key == 'Q'):
        Is3Exist, msg3 = check_directory('3', path_num, identifier)
        Is1Exist, msg1 = check_directory('1', path_num, identifier)
        hIsExist, h_msg = check_directory('H', path_num, identifier)
        if(Is3Exist == False):
            error_list[curr_key].append(msg3)
            #print(curr_key + ': ' + msg3)
        if(Is1Exist == False):
            error_list[curr_key].append( msg1)
            #print(curr_key + ': ' + msg1)
        if(hIsExist == False):
            error_list[curr_key].append(h_msg)
            #print(curr_key + ': ' + h_msg)
    else:
        hIsExist, h_msg = check_directory('H', path_num, identifier)
        if(hIsExist):
            IsQExist, q_msg = check_directory('Q', path_num, identifier)
            Is1Exist, msg1 = check_directory('1', path_num, identifier)
            if(IsQExist == False):
                error_list[curr_key].append(q_msg)
                #print(curr_key + 'H: ' + q_msg)
            if(Is1Exist == False):
                error_list[curr_key].append(msg1)
                #print(curr_key + 'H: ' + msg1)
            pass    
        else:
            Is1Exist, msg1 = check_directory('1', path_num, identifier)
            if(Is1Exist == False):
                IsQExist, q_msg = check_directory('Q', path_num, identifier)
                if(IsQExist):    
                    error_list[curr_key].append(msg1)
                    error_list[curr_key].append(h_msg)
                else:
                    error_list[curr_key].append(msg1)
                    warning_list.append(h_msg)
                    warning_list.append(q_msg)
                #print(curr_key + '1: ' + msg1)

def four_way_validate():
    #Loop through 4 ways
    for key in dir_dict.keys():
        #Loop through all possible paths
        for path in op_dir_names:
            o_path = mod_paths[key] + '/' + path
            #Check if orbital path's file exists
            if(os.path.isdir(o_path)):
                files = os.listdir(o_path)
                for file in files:
                    #Get file name, proccess it and search other folders, need dir_dict key and path number
                    identifier = get_file_identifier(str(file))
                    file_search(key, path, identifier)
            else:
                error_list['P'].append("Missing: " + dir_dict[key] + " " + path)
                print("Missing: " + dir_dict[key] + " " + path)

def write_err_file(err_list, filename):
    for err in err_list:
        filename.write(err + '\n')
    filename.close()

#Assumption: pwd = projects/TDataFus/gyzhao/TF/data/MODIS
def main() :
    if len(sys.argv) < 2 :
        print("usage: python v_test_modis.py year")
        sys.exit(1)
    global year 
    year = int(sys.argv[1])
    if(year < 2000 or year > 2015):
        print("Year argument out of range (2000-2015)")
        sys.exit(1)
    init_orbital_dir_names()
    init_file_paths()
    init_error_list()
    four_way_validate()
    total_err_list = []
    for key, e_list in error_list.items():
        for e in e_list:
            total_err_list.append(e + '\n')
    total_err_list = list(set(total_err_list))
    for e in total_err_list:
        total_err.write(e)
    for w in warning_list:
        warnings.write(w)
    #write_err_file(error_list['1'], err1file)
    #write_err_file(error_list['H'], errhfile)
    #write_err_file(error_list['Q'], errqfile)
    #write_err_file(error_list['3'], err3file)
    #write_err_file(error_list['P'], errpathfile)
    
#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
    os.chdir('/')
    os.chdir('/projects/TDataFus/BFData/MODIS')
    print(" Now at " + str(os.getcwd()))
    main()