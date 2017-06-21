import sys, os
dir_paths = {}
total_err = open('ceres_missing_list', 'w')

def init_dir_paths(year):
    dir_paths['fm1'] = os.getcwd() + '/FM1/SSF/' + str(year)
    dir_paths['fm2'] = os.getcwd() + '/FM2/SSF/' + str(year)

def validate():
    miss_list = []
    files_to_check = os.listdir(dir_paths['fm2'])
    files_to_check2 = os.listdir(dir_paths['fm1'])
    cross_files = []
    for f in files_to_check:
        cross_files.append(f.replace('FM2', 'FM1'))
    for f in cross_files:
        if('.met' in f):
            continue
        if(f not in files_to_check2):
            miss_list.append(f+'\n')
    return miss_list
        
        

def main():
    if len(sys.argv) < 2 :
        print("usage: python v_test_ceres.py year")
        sys.exit(1)
    global year 
    year = int(sys.argv[1])
    if(year < 2000 or year > 2016):
        print("Year argument out of range (2000-2016)")
        sys.exit(1)
    init_dir_paths(year)
    missing_list = validate()
    for m in missing_list:
        total_err.write(m)
    
#---------------------------------------------------------------------------------------------------
if __name__ == '__main__':
#---------------------------------------------------------------------------------------------------
    os.chdir('/')
    os.chdir('projects/TDataFus/gyzhao/TF/data/CERES')
    print(" Now at " + str(os.getcwd()))
    main()