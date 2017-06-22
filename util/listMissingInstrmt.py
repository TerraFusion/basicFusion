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
        print
        print ('{} parses all of the input.txt files to the Basic Fusion program and prints which instruments'.format(sys.argv[0]))
        print ('are missing from each of the input file listings, as well as their corresponding orbit number.')
        print 'Missing instruments are denoted inside the text file by the "[instrument] N/A" marker.'
        print
        print ('Usage: {} [input.txt directory]'.format(sys.argv[0]) )
        print
        sys.exit(1)

    # Find all of the input files in this directory
    for filename in os.listdir( sys.argv[1] ):
        if not filename.endswith(".txt") :
            continue

        # For every file, first save the orbit number. Then, find all of the "... N/A" lines and log this
        with open(filename, "r") as f:
            orbitStr = f.readline().strip()
            #print orbitStr
            #orbitStr = f.readline().strip()
            #print orbitStr
            try:
                orbitInt = int(orbitStr)
            except:
                print "\nError: The first line for file {} did not contain the orbit number.\n".format(filename)
                
            for line in f:
                line = line.strip()
                if " N/A" not in line:
                    continue
                else:
                    missingList.append(line)
                    
            if len(missingList) > 0 :
                print "Orbit {} from file {} is missing:".format(orbitInt, filename)
                
                for missing in missingList :
                    print "\t{}".format(missing)

                print('')

            # done printing missing list
        # done with filename
    # done with all files in directory

    sys.exit(0)


if __name__ == '__main__' :
    main()
