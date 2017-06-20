#!/usr/bin/python

import sys, os

def main():

    MOP_NA = 0
    CER_NA = 0
    AST_NA = 0
    MIS_NA = 0
    MOD_NA = 0
    missingList = []
    
    if len(sys.argv) < 2 : 
        print ('{} parses all of the input.txt files to the Basic Fusion program and prints which instruments')
        print ('are missing from each of the input file listings, as well as their corresponding orbit number.')

        print ('Usage: {} [input Fusion file directory]'.format(sys.argv[0]) )
        sys.exit(1)

    # Find all of the input files in this directory
    for filename in os.listdir( argv[1] ):
        if not filename.endswith(".txt") :
            continue

        # For every file, first save the orbit number. Then, find all of the "... N/A" lines and log this
        with open(filename, 'r') as f:
            orbitStr = f.readline().strip()
            try:
                orbitInt = int(orbitStr)
                break
            except NAN:
                print("\nError: The first line for file ", filename, "\ndid not contain the orbit number.\n")
                
            for line in f:
                if " N/A" not in line:
                    continue
                else:
                    missingList.append(line)
                    
            if len(missingList) > 0 :
                print("Orbit ", orbitInt, " is missing:")
                
                for missing in missingList :
                    print("\t", missing)

                print('')

            # done printing missing list
        # done with filename
    # done with all files in directory

    sys.exit(0)


if __name__ == '__main__' :
    main()
