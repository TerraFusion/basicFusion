#!/bin/bash

#       generateInput.sh
#
#   AUTHOR: Landon Clipp
#   EMAIL:  clipp2@illinois.edu
#
#   This script reads the hdf files contained in the directory given to it and generates an inputFiles.txt
#   file for the TERRA fusion program to read. It performs comprehensive error checking for the generated
#   output file to make sure that all files are in chronological order, that all files are in the order
#   expected and that there are no unknown errors in the output file.

if [ "$#" -ne 1 ]; then
	printf "Usage:\n\t$0 [relative/absolute path to all 5 instruments]\n\n"
	printf "Example: $0 /path/to/instrument/directories\nDo not provide path to individual instruments.\n"
	exit 1
fi

INPATH=$1
OUTFILE=/u/sciteam/clipp/basicFusion/exe/inputFiles.txt
CURDIR=$(pwd)
FAIL=0
MOPITTNUM=0
CERESNUM=0
MODISNUM=0
ASTERNUM=0
MISRNUM=0
# Set DEBUGFILE to $OUTFILE to disable file debugging.
# DEBUGFILE is the file where all of the error checking for date, order and general file consistency
# is done. Setting this to anything other than $OUTFILE means that this program will NOT debug
# the file it generated (OUTFILE) but rather the file you point it to.
DEBUGFILE=$OUTFILE

# Check if the provided INPATH actually contains directories for all 5 instruments.
# Note that the following if statements do not take into account if the INPATH
# is a symbolic link.


if [ ! -d "$INPATH/MOPITT" ]; then
	printf "\e[4m\e[91mFatal Error\e[0m: Could not find MOPITT directory.\n" >&2
	FAIL=1
fi

if [ ! -d "$INPATH/CERES" ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: Could not find CERES directory.\n" >&2
        FAIL=1
fi

if [ ! -d "$INPATH/MISR" ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: Could not find MISR directory.\n" >&2
        FAIL=1
fi

if [ ! -d "$INPATH/ASTER" ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: Could not find ASTER directory.\n" >&2
        FAIL=1
fi

if [ ! -d "$INPATH/MODIS" ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: Could not find MODIS directory.\n" >&2
        FAIL=1
fi

if [ $FAIL -eq 1 ]; then
	printf "Exiting script.\n" >&2
	exit 1
fi

# Check that each directory has valid files
temp=$(ls "$INPATH/MOPITT" | grep "MOP" | grep "he5" )
if [ ${#temp} -lt 2 ]; then
	printf "\e[4m\e[91mFatal Error\e[0m: No valid MOPITT HDF files found.\n" >&2
	FAIL=1
fi

temp=$(ls "$INPATH/CERES" | grep "CER_BDS_Terra" )
if [ ${#temp} -lt 2 ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: No valid CERES HDF files found.\n" >&2
	FAIL=1
fi

temp=$(ls "$INPATH/MISR" | grep "MISR" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: No valid MISR HDF files found.\n" >&2
	FAIL=1
fi

temp=$(ls "$INPATH/ASTER" | grep "AST" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: No valid ASTER HDF files found.\n" >&2
	FAIL=1
fi

temp=$(ls "$INPATH/MODIS" | grep "MOD" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "\e[4m\e[91mFatal Error\e[0m: No valid MODIS HDF files found.\n" >&2
	FAIL=1
fi

if [ $FAIL -eq 1 ]; then
	printf "Exiting script.\n" >&2
	exit 1
fi


rm -f "$OUTFILE"
rm -r -f "$CURDIR"/__tempFiles

mkdir __tempFiles

##########
# MOPITT #
##########
printf "# MOPITT\n" >> "$OUTFILE"
cd "$INPATH"/MOPITT
# grep -v removes any "xml" files
# put results of ls into a temporary text file
ls | grep "MOP" | grep -v "xml" >> "$CURDIR"/__tempFiles/MOPITT.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE
while read -r line; do
	echo "$(pwd)/$line" >> "$OUTFILE"
	let "MOPITTNUM++"
done <"$CURDIR"/__tempFiles/MOPITT.txt

#########
# CERES #
#########
printf "# CERES\n" >> "$OUTFILE"
cd "../CERES"
# grep -v removes any files with "met" in them, they're unwanted
ls | grep "CER_BDS_Terra" | grep -v "met" >> "$CURDIR"/__tempFiles/CERES.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
        echo "$(pwd)/$line" >> "$OUTFILE"
	let "CERESNUM++"
done <"$CURDIR"/__tempFiles/CERES.txt


#########
# MODIS #
#########
# For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
# into separate text files to make it easy to handle

cd "../MODIS"
ls | grep "MOD021KM" | grep "hdf" >> "$CURDIR"/__tempFiles/1KMfile.txt
ls | grep "MOD02HKM" | grep "hdf" >> "$CURDIR"/__tempFiles/HKMfile.txt
ls | grep "MOD02QKM" | grep "hdf" >> "$CURDIR"/__tempFiles/QKMfile.txt
ls | grep "MOD03" | grep "hdf" >> "$CURDIR"/__tempFiles/MOD03file.txt

printf "# MODIS\n" >> "$OUTFILE"
while read -r line; do
	# first, echo this 1KM filename into the outfile
	echo "$(pwd)/$line" >> "$OUTFILE"
	let "MODISNUM++"
	# then extract the substring in this line containing the date info
	# (using shell parameter expansion)
	substr=${line:10:16}

	# write the corresponding HKM filename into our outfile.
	# note that it may not exist.
	temp="$(cat $CURDIR/__tempFiles/HKMfile.txt | grep $substr)"
	if [ ${#temp} -ge 2 ]; then	# if temp is a non-zero length string
		echo "$(pwd)/$temp" >> "$OUTFILE"
        let "MODISNUM++"
	fi

	# write in the corrseponding QKM file
	temp="$(cat $CURDIR/__tempFiles/QKMfile.txt | grep $substr)"
	if [ ${#temp} -ge 2 ]; then
        	echo "$(pwd)/$temp" >> "$OUTFILE"
            let "MODISNUM++"
	fi

	# write in the corresponding MOD03 file
	temp="$(cat $CURDIR/__tempFiles/MOD03file.txt | grep $substr)"
    if [ ${#temp} -ge 2 ]; then
		echo "$(pwd)/$temp" >> "$OUTFILE"
        let "MODISNUM++"
	fi

done <"$CURDIR"/__tempFiles/1KMfile.txt

#########
# ASTER #
#########

printf "# ASTER\n" >> "$OUTFILE"
cd "../ASTER"
# grep -v removes any files with "tif" in them, they're unwanted
ls | grep "AST" | grep "hdf" >> "$CURDIR"/__tempFiles/ASTER.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
        echo "$(pwd)/$line" >> "$OUTFILE"
	let "ASTERNUM++"
done <"$CURDIR"/__tempFiles/ASTER.txt

########
# MISR #
########

printf "# MISR\n" >> "$OUTFILE"
cd "../MISR"
# put the MISR files into temp text files
ls | grep "MISR_AM1_GRP" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_GRP.txt
ls | grep "MISR_AM1_AGP" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_AGP.txt
ls | grep "MISR_AM1_GP" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_GP.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
    echo "$(pwd)/$line" >> "$OUTFILE"
    let "MISRNUM++"
done <"$CURDIR"/__tempFiles/MISR_GRP.txt
while read -r line; do
    echo "$(pwd)/$line" >> "$OUTFILE"
    let "MISRNUM++"
done <"$CURDIR"/__tempFiles/MISR_AGP.txt
while read -r line; do
    echo "$(pwd)/$line" >> "$OUTFILE"
    let "MISRNUM++"
done <"$CURDIR"/__tempFiles/MISR_GP.txt

##################################################
#           PREPROCESSING ERROR CHECKS           #
##################################################

# We need to check for the existence of the necessary files for each instrument
# in our generated file list.
# Also in addition to the above, we need to check that the following is true:
#   MODIS: The first file is MOD021KM
#   MISR: The first file is AA
# For MODIS and MISR, other necessary files include MOD03, AF, AN, AGP etc,
# but if we can first establish that the first file for MISR, for instance,
# is an AA file, checking for the existence of everything else will be 
# handled downstream.

cd "$CURDIR"

FAIL=0

# Note: the '.*' is simply a wildcard operator for grep

tempLine=$(cat "$DEBUGFILE" | grep 'MOP01.*.he5')
if [ -z "$tempLine" ]; then
    printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
    printf "\tNo MOPITT \"MOP01\" files were found in generated file list.\n" >&2
    FAIL=1
fi
tempLine=$(cat "$DEBUGFILE" | grep "CER_BDS_Terra")
if [ -z "$tempLine" ]; then
    printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
    printf "\tNo CERES files were found in generated file list.\n" >&2
    FAIL=1
fi

# Note: the sed command is capturing the first line of the output of grep.
# We are checking that the first MODIS line is a 1KM file
tempLine=$(cat "$DEBUGFILE" | grep "MOD.*.hdf" | sed -n 1p )
# Now that tempLine has the first MODIS line, grep it to see if it's a
# 1KM file
tempLine=$(echo "$tempLine" | grep "MOD021KM")
if [ -z "$tempLine" ]; then
    printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
    printf "\tNo MODIS \"MOD021KM\" files found or \"MOD021KM\" was not the first MODIS file.\n" >&2
    FAIL=1
fi
tempLine=$(cat "$DEBUGFILE" | grep "AST_L1T_.*.hdf")
if [ -z "$tempLine" ]; then
    printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
    printf "\tNo ASTER files were found in generated file list.\n" >&2
    FAIL=1
fi

# Repeat same process here as for MODIS
tempLine=$(cat "$DEBUGFILE" | grep "MISR.*.hdf" | sed -n 1p )
tempLine=$(echo "$tempLine" | grep "MISR.*_AA_.*.hdf")
if [ -z "$tempLine" ]; then
    printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
    printf "\tNo MISR \"AA\" files found or \"AA\" was not the first MISR file.\n" >&2
    FAIL=1
fi

if [ $FAIL -ne 0 ]; then
    printf "Exiting script.\n" >&2
    rm -r "$CURDIR"/__tempFiles
    exit $FAIL
fi







#__________________________________________________________________
#                                                                  |
# CHECK THAT ALL FILES PRESENT IN OUR INPUT DIRECTORY MADE IT INTO |
# THE INPUT FILES LIST (ALL DESIRED HDF FILES)                     |
#__________________________________________________________________|

FAIL=0
cd "$CURDIR"

$(echo ${line##*/})

while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/MOPITT.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/CERES.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/1KMfile.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/HKMfile.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/QKMfile.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/MOD03file.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/ASTER.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/MISR_GRP.txt"
while read -r line; do
    searchFile="$(cat $DEBUGFILE | grep $line)"
    searchFile=$(echo ${searchFile##*/})
    if [[ "$line" != "$searchFile" ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\t$line was missing in the generated file list.\n" >&2
        FAIL=1
    fi
done <"$CURDIR/__tempFiles/MISR_AGP.txt"

if [ $FAIL -ne 0 ]; then
    printf "Exiting script.\n" >&2
    rm -r "$CURDIR"/__tempFiles
    exit 1
fi

#______________________________________________________#
#                                                      #
# CHECK FOR DATE CONSISTENCY AND GENERAL FILE ORDERING #
#______________________________________________________#

prevDate=""
prevRes=""
prevGroupDate=""
curGroupDate=""
instrumentSection=""
MODISVersionMismatch=0
ASTERVersionMismatch=0
MISRVersionMismatch=0

# Read through the DEBUGFILE file.
# Remember, DEBUGFILE is the variable our script will do the debugging on. It may
# or may not be set to the OUTFILE variable.
linenum=1

while read -r line; do
    # Check to make sure that each line is either a comment, a newline or a path.
    # If it is none of them, throw an error. This line would cause the C program
    # downstream to crash because if the line isn't a comment or newline,
    # it will interpret it as a file path.
    
    if [[ ${line:0:1} != "." && ${line:0:1} != "/" && ${line:0:1} != ".." && ${line:0:1} != "#" && ${line:0:1} != "\n" && ${line:0:1} != "\0" && ${#line} != 0 ]]; then
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\tAt line number: $linenum\n"
        printf "\t\"$line\" is an invalid line.\nExiting script.\n" >&2
        rm -r "$CURDIR"/__tempFiles
        exit 1
    fi
	# continue if we have a non-valid line (comment or otherwise)
	if [[ ${#line} == 0 || ${line:0:1} == "#" ]]; then
        let "linenum++"
		continue
	fi
    

	# check to see if we have entered into a new instrument section.
	# Note that internally we represent the current instrument we're reading
	# by the first 3 characters of the respective filename. For instance, MOPITT
	# would be "MOP", MODIS would be "MOD", MISR would be "MIS" etc.
	# Remember, these are coming from the FILENAMES themselves.
	curfilename=$(echo ${line##*/})
	
	if [ "$instrumentSection" != "${curfilename:0:3}" ]; then
		instrumentSection=${curfilename:0:3}
		# unset prevDate because we are starting a new section.
		unset prevDate
        unset prevGroupDate
        unset prevRes
        unset curGroupDate
	fi

	# note about the "prev" variables:
	# 	If only one file is present for that instrment, no checking will need
	# 	to be done. This code accounts for that.

                       ##########
                       # MOPITT #
                       ##########

	if [ "$instrumentSection" == "MOP" ]; then
		
		# line now contains a valid file path. Check to see if prevDate has been set.
		# If it has not, set it to the value of line and continue to the next line
		if [ -z "$prevDate" ]; then
			# get the date information from line
			# first find the filename itself (truncate line by cutting off the path)
			prevfilename=$(echo ${line##*/})
			#set prevDate to the date contained in filename
			prevDate=${prevfilename:6:8}
			# continue while loop
			let "linenum++"
            continue
		fi

		# we now have the previous date and the current line. Now we need to find the
		# current date from the variable "curfilename" and compare that with the previous date
		curDate=${curfilename:6:8}
		if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
			printf "\e[4m\e[93mWarning\e[0m: " >&2
			printf "MOPITT files found to be out of order.\n" >&2
			printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
		fi

		prevDate="$curDate"
		prevfilename="$curfilename"

                      #########
                      # CERES #
                      #########

    elif [ "$instrumentSection" == "CER" ]; then
        # line now contains a valid file path. Check to see if prevDate has been set.
        # If it has not, set it to the value of line and continue to the next line
        if [ -z "$prevDate" ]; then
            # get the date information from line
            #set prevDate to the date contained in filename
		    prevfilename=$(echo ${line##*/})
            prevDate=$(echo ${prevfilename##*.})
            # continue while loop
            let "linenum++"
            continue
        fi

	    # we now have the previous date and the current line. Now we need to find the
        # current date from the variable "curfilename" and compare that with the previous date
        curDate=${curfilename##*.}
        if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
            printf "\e[4m\e[93mWarning\e[0m: " >&2
            printf "CERES files found to be out of order.\n" >&2
            printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
        fi

        prevDate="$curDate"
	    prevfilename="$curfilename"

                      #########
                      # MODIS #
                      #########
    # MODIS will have tgree checks. The first is to check for chronological consistency.
    # The second is to check that the files follow in either the order of:
    # MOD021KM
    # MOD03
    # or
    # MOD021KM
    # MOD02HKM
    # MOD02QKM
    # MOD03
    #
    # The third will throw a warning if the files have different version numbers

    elif [ "$instrumentSection" == "MOD" ]; then
        # note: "res" refers to "MOD021KM", "MOD02HKM", "MOD02QKM", or "MOD03"
        if [ -z "$prevDate" ]; then
            prevfilename=$(echo ${line##*/})
            # extract the date
            tmp=${prevfilename#*.A}
            prevDate=$(echo "$tmp" | cut -f1,2 -d'.')
            prevRes=$(echo ${prevfilename%%.*})
            curGroupDate="$prevDate"
            let "linenum++"
            continue
        fi

        # CHECK FOR RESOLUTION CONSISTENCY

        tmp=${curfilename#*.A}
        curDate=$(echo "$tmp" | cut -f1,2 -d'.')
        curRes=$(echo ${curfilename%%.*})

        if [[ "$prevRes" == "MOD021KM" ]]; then
            if [[ "$curRes" != "MOD03" && "$curRes" != "MOD02HKM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                printf "\tExpected to see MOD03 or MOD02HKM after MOD021KM.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevRes" == "MOD02HKM" ]]; then
            if [[ "$curRes" != "MOD02QKM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                printf "\tExpected to see MOD02QKM after MOD02HKM.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevRes" == "MOD02QKM" ]]; then
            if [[ "$curRes" != "MOD03" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                printf "\tExpected to see MOD03 after MOD02QKM.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevRes" == "MOD03" ]]; then
            if [[ "$curRes" != "MOD021KM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                printf "\tExpected to see MOD021KM after MOD03.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        else
            printf "\e[4m\e[91mFatal Error\e[0m: " >&2
            printf "Unknown error at line $LINENO. Exiting script.\n" >&2
            rm -r "$CURDIR"/__tempFiles
            exit 1
        fi

        # CHECK FOR DATE CONSISTENCY

        # Internally, we will consider the file "MOD021KM" to be the indication that a new
        # "group" has begun i.e.:
        # MOD021KM...
        # MOD03...
        # or
        # MOD021KM...
        # MOD02HKM...
        # MOD02QKM
        # MOD03
        
        # If the current line is a MOD021KM file, we have found the beginning of a new group
        
        if [[ "$curRes" == "MOD021KM" ]]; then
            prevGroupDate="$curGroupDate"
            curGroupDate="$curDate"
            # if prevGroupDate has not yet been set (we are on our first group)
            if [ -z "$prevGroupDate" ]; then
                continue
            # else check if the current group's date correctly follows the previous group's date
            else
                if [[ "$curDate" < "$prevGroupDate" || "$curDate" == "$prevGroupDate" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MODIS date inconsistency found.\n"
                    printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                    printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                    printf "\tDates should be strictly increasing between groups!\n" >&2
                fi
            fi
        fi

        # check that the current line's date is the same as the rest its group. Each file in a group should
        # have the same date.
        if [[ "$curDate" != "$curGroupDate" ]]; then
            printf "\e[4m\e[93mWarning\e[0m: " >&2
            printf "MODIS date inconsistency found.\n"
            printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
            printf "\tThese two dates should be equal.\n" >&2
        fi

        # CHECK FOR VERSION CONSISTENCY
        # If there hasn't been a previous MODIS file, then skip this step
        
        if [[ ${#prevfilename} != 0 && $MODISVersionMismatch == 0 ]]; then
            prevVersion=$(echo "$prevfilename" | cut -f4,4 -d'.')
            curVersion=$(echo "$curfilename" | cut -f4,4 -d'.')
            
            if [[ "$prevVersion" != "$curVersion" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MODIS file version mismatch.\n" >&2
                printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                printf "\tWill not print any more MODIS warnings of this type.\n" >&2
                MODISVersionMismatch=1
            fi
        fi

        prevDate="$curDate"
        prevRes="$curRes"
        prevfilename="$curfilename"

                #########
                # ASTER #
                #########

    elif [ "$instrumentSection" == "AST" ]; then
        # line now contains a valid file path. Check to see if prevDate has been set.
        # If it has not, set it to the value of line and continue to the next line
        if [ -z "$prevDate" ]; then
            # get the date information from line
            #set prevDate to the date contained in filename
            prevfilename=$(echo ${line##*/})
            prevDate=$(echo "$prevfilename" | cut -f3,3 -d'_')
            prevVersion=${prevDate:0:3}
            prevDate="${prevDate:3}"
            # continue while loop
            let "linenum++"
            continue
        fi

        # we now have the previous date and the current line. Now we need to find the
        # current date from the variable "curfilename" and compare that with the previous date
        curDate=$(echo "$curfilename" | cut -f3,3 -d'_')
        curVersion=${curDate:0:3}
        curDate="${curDate:3}"
        if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
            printf "\e[4m\e[93mWarning\e[0m: " >&2
            printf "ASTER files found to be out of order.\n" >&2
            printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
        fi

        # CHECK FOR VERSION CONSISTENCY
        # If there hasn't been a previous ASTER file, then skip this step

        if [[ ${#prevfilename} != 0 && $ASTERVersionMismatch == 0 ]]; then

            if [[ "$prevVersion" != "$curVersion" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "ASTER file version mismatch.\n" >&2
                printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                printf "\tWill not print any more ASTER warnings of this type.\n" >&2
                ASTERVersionMismatch=1
            fi
        fi


        prevDate="$curDate"
        prevfilename="$curfilename"
        prevVersion="$curVersion"

       
            ########
            # MISR #
            ######## 

    elif [ "$instrumentSection" == "MIS" ]; then
        # note: "cam" refers to which camera, ie "AA", "AF", "AN" etc.
        if [ -z "$prevDate" ]; then
            prevfilename=$(echo ${line##*/})
            # extract the date
            prevDate=$(echo "$prevfilename" | cut -f6,7 -d'_')
            prevCam=$(echo "$prevfilename" | cut -f8,8 -d'_' )
            curGroupDate="$prevDate"
            let "linenum++"
            continue
        fi

        # CHECK FOR CAMEREA CONSISTENCY

        curDate=$(echo "$curfilename" | cut -f6,7 -d'_')
        curCam=$(echo "$curfilename" | cut -f8,8 -d'_' )

        if [[ "$prevCam" == "AA" ]]; then
            if [[ "$curCam" != "AF" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see AF after AA.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "AF" ]]; then
            if [[ "$curCam" != "AN" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see AN after AF.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "AN" ]]; then
            if [[ "$curCam" != "BA" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see BA after AN.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi

        elif [[ "$prevCam" == "BA" ]]; then
            if [[ "$curCam" != "BF" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see BF after BA.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "BF" ]]; then
            if [[ "$curCam" != "CA" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see CA after BF.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "CA" ]]; then
            if [[ "$curCam" != "CF" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see CF after CA.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "CF" ]]; then
            if [[ "$curCam" != "DA" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see DA after CF.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "DA" ]]; then
            if [[ "$curCam" != "DF" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                printf "\tExpected to see DF after DA.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$prevCam" == "DF" ]]; then
            if [[ "$(echo "$curfilename" | cut -f7,3 -d'_')" != "AGP" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\"\n" >&2
                printf "\tExpected to see an AGP file after a DF camera.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
            if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" != "GP" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                printf "\tExpected to see GP after AGP file.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
            if [[ "$curCam" != "AA" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MISR files are out of order.\n" >&2
                printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                printf "\tExpected to see AA file after GP file.\n" >&2
                printf "Exiting script.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                exit 1
            fi
        else
            printf "\e[4m\e[91mFatal Error\e[0m: " >&2
            printf "Unknown error at line $LINENO. Exiting script.\n" >&2
            rm -r "$CURDIR"/__tempFiles
            exit 1
        fi

        # CHECK FOR DATE CONSISTENCY
        
       
        if [[ "$curCam" == "AA" ]]; then
            prevGroupDate="$curGroupDate"
            curGroupDate="$curDate"
            # if prevGroupDate has not yet been set (we are on our first group)
            if [ -z "$prevGroupDate" ]; then
                continue
            # else check if the current group's date correctly follows the previous group's date
            else
                if [[ "$curDate" < "$prevGroupDate" || "$curDate" == "$prevGroupDate" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MISR date inconsistency found.\n"
                    printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                    printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                    printf "\tDates should be strictly increasing between groups!\n" >&2
                fi
            fi
        fi

        # check that the current line's date is the same as the rest its group. Each file in a group should
        # have the same date.

        # if the current line is an AGP file, we just need to check the path number "Pxxx"
        if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
            # if the current line's path number is different than the previous line's
            curpath="$(echo "$curfilename" | cut -f4,4 -d'_')"
            prevpath="$(echo "$prevfilename" | cut -f6,6 -d'_')"
            if [[ "$curpath" != "$prevpath" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MISR file inconsistency found.\n"
                printf "\t$curfilename has a path number: $curpath != $prevpath\n" >&2
                printf "\tThese two path numbers should be equal.\n" >&2
            fi
        elif [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
            curDate=$(echo "$curfilename" | cut -f5,6 -d'_')
            if [[ "$curDate" != "$curGroupDate" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MISR date inconsistency found.\n"
                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                printf "\tThese two dates should be equal.\n" >&2
            fi
        # else we need to check the dates between cur and prev
        elif [[ "$curDate" != "$curGroupDate" ]]; then
            printf "\e[4m\e[93mWarning\e[0m: " >&2
            printf "MISR date inconsistency found.\n"
            printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
            printf "\tThese two dates should be equal.\n" >&2
        fi

        # CHECK FOR VERSION CONSISTENCY
        # If there hasn't been a previous MISR file, then skip this step

        # If the current file is an AGP file, skip it
        if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" != "AGP" ]]; then
            # if this isn't our first MISR file and there hasn't been a version mismatch
            # before, then
            if [[ ${#prevfilename} != 0 && $MISRVersionMismatch == 0 ]]; then
                # if the previous file was an AGP file, then don't update the prevVersion
                # variable
                if [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" != "AGP" ]]; then
                    prevVersion=$(echo ${prevfilename##*_})
                    prevVersion=${prevVersion:0:4}
                fi

                curVersion=$(echo ${curfilename##*_})
                curVersion=${curVersion:0:4}

                if [[ "$prevVersion" != "$curVersion" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MISR file version mismatch.\n" >&2
                    printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                    printf "\tWill not print any more MISR warnings of this type.\n" >&2
                    MISRVersionMismatch=1
                fi
            fi
        fi


        prevDate="$curDate"
        prevCam="$curCam"
        prevfilename="$curfilename"

    else
        printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
        printf "\tUnable to determine which was the current instrument being read.\n" >&2
        printf "\tDebugging information follows:\n" >&2
        printf "\t\tline=$line\n" >&2
        printf "\t\tprevfilename=$prevfilename\n" >&2
        printf "\t\tcurfilename=$curfilename\n" >&2
        printf "\t\tinstrumentSection=$instrumentSection\n" >&2
        printf "Exiting script.\n" >&2
        rm -r "$CURDIR"/__tempFiles
        exit 1
    fi

    let "linenum++"
done <"$DEBUGFILE"


printf "Files read:\n"
printf "\tMOPITT: $MOPITTNUM\n"
printf "\tCERES:  $CERESNUM\n"
printf "\tMODIS:  $MODISNUM\n"
printf "\tASTER:  $ASTERNUM\n"
printf "\tMISR:   $MISRNUM\n"

rm -r "$CURDIR"/__tempFiles
