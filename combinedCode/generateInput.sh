#!/bin/bash

# This script makes an assumption that the result of command ls
# gives an alphabetical listing

if [ "$#" -ne 1 ]; then
	printf "Usage:\n\t$0 [relative/absolute path to all 5 instruments]\n\n"
	printf "Example: $0 /path/to/instrument/directories\nDo not provide path to individual instruments.\n"
	exit 1
fi

INPATH=$1
OUTFILE=inputFiles.txt
CURDIR=$(pwd)
FAIL=0
MOPITTNUM=0
CERESNUM=0
MODISNUM=0
ASTERNUM=0
MISRNUM=0

# Check if the provided INPATH actually contains directories for all 5 instruments.
# Note that the following if statements do not take into account if the INPATH
# is a symbolic link.


if [ ! -d "$INPATH/MOPITT" ]; then
	printf "Error: Could not find MOPITT directory.\n"
	FAIL=1
fi

if [ ! -d "$INPATH/CERES" ]; then
        printf "Error: Could not find CERES directory.\n"
        FAIL=1
fi

if [ ! -d "$INPATH/MISR" ]; then
        printf "Error: Could not find MISR directory.\n"
        FAIL=1
fi

if [ ! -d "$INPATH/ASTER" ]; then
        printf "Error: Could not find ASTER directory.\n"
        FAIL=1
fi

if [ ! -d "$INPATH/MODIS" ]; then
        printf "Error: Could not find MODIS directory.\n"
        FAIL=1
fi

if [ $FAIL -eq 1 ]; then
	printf "Exiting script.\n"
	exit 1
fi

# Check that each directory has valid files
temp=$(ls "$INPATH/MOPITT" | grep "MOP" | grep "he5" )
if [ ${#temp} -lt 2 ]; then
	printf "Error: No valid MOPITT HDF files found.\n"
	FAIL=1
fi

temp=$(ls "$INPATH/CERES" | grep "CER_BDS_Terra" )
if [ ${#temp} -lt 2 ]; then
        printf "Error: No valid CERES HDF files found.\n"
	FAIL=1
fi

temp=$(ls "$INPATH/MISR" | grep "MISR" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "Error: No valid MISR HDF files found.\n"
	FAIL=1
fi

temp=$(ls "$INPATH/ASTER" | grep "AST" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "Error: No valid ASTER HDF files found.\n"
	FAIL=1
fi

temp=$(ls "$INPATH/MODIS" | grep "MOD" | grep "hdf")
if [ ${#temp} -lt 2 ]; then
        printf "Error: No valid MODIS HDF files found.\n"
	FAIL=1
fi

if [ $FAIL -eq 1 ]; then
	printf "Exiting script.\n"
	exit 1
fi


rm "$OUTFILE"

##########
# MOPITT #
##########
printf "# MOPITT\n" >> "$CURDIR/$OUTFILE"
cd "$INPATH"/MOPITT
# grep -v removes any "xml" files
# put results of ls into a temporary text file
ls | grep "MOP" | grep -v "xml" >> temp.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE
while read -r line; do
	echo "$(pwd)/$line" >> "$CURDIR"/"$OUTFILE"
	let "MOPITTNUM++"
done <temp.txt
rm temp.txt

#########
# CERES #
#########
printf "# CERES\n" >> "$CURDIR/$OUTFILE"
cd "../CERES"
# grep -v removes any files with "met" in them, they're unwanted
ls | grep "CER_BDS_Terra" | grep -v "met" >> temp.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
        echo "$(pwd)/$line" >> "$CURDIR"/"$OUTFILE"
	let "CERESNUM++"
done <temp.txt
rm temp.txt


#########
# MODIS #
#########
# For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
# into separate text files to make it easy to handle

cd "../MODIS"
ls | grep "MOD021KM" | grep "hdf" >> ./1KMfile.txt
ls | grep "MOD02HKM" | grep "hdf" >> ./HKMfile.txt
ls | grep "MOD02QKM" | grep "hdf" >> ./QKMfile.txt
ls | grep "MOD03" | grep "hdf" >> ./MOD03file.txt

printf "# MODIS\n" >> "$CURDIR/$OUTFILE"
while read -r line; do
	# first, echo this 1KM filename into the outfile
	echo "$(pwd)/$line" >> "$CURDIR"/"$OUTFILE"
	let "MODISNUM++"
	# then extract the substring in this line containing the date info
	# (using shell parameter expansion)
	substr=${line:10:16}

	# write the corresponding HKM filename into our outfile.
	# note that it may not exist.
	temp="$(cat ./HKMfile.txt | grep $substr)"
	if [ ${#temp} -ge 2 ]; then	# if temp is a non-zero length string
		echo "$(pwd)/$temp" >> "$CURDIR"/"$OUTFILE"
	fi

	# write in the corrseponding QKM file
	temp="$(cat ./QKMfile.txt | grep $substr)"
	if [ ${#temp} -ge 2 ]; then
        	echo "$(pwd)/MODIS/$temp" >> "$CURDIR"/"$OUTFILE"
	fi

	# write in the corresponding MOD03 file
	temp="$(cat ./MOD03file.txt | grep $substr)"
        if [ ${#temp} -ge 2 ]; then
		echo "$(pwd)/$temp" >> "$CURDIR"/"$OUTFILE"
	fi

done <./1KMfile.txt

rm ./1KMfile.txt ./HKMfile.txt ./QKMfile.txt ./MOD03file.txt

#########
# ASTER #
#########

printf "# ASTER\n" >> "$CURDIR/$OUTFILE"
cd "../ASTER"
# grep -v removes any files with "tif" in them, they're unwanted
ls | grep "AST" | grep "hdf" >> temp.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
        echo "$(pwd)/$line" >> "$CURDIR"/"$OUTFILE"
	let "ASTERNUM++"
done <temp.txt
rm temp.txt

########
# MISR #
########

printf "# MISR\n" >> "$CURDIR/$OUTFILE"
cd "../MISR"
# put the result of ls into a temp text file
ls | grep "MISR" | grep "hdf" >> temp.txt
# iterate over this file, prepending the path of the file into our
# OUTFILE

while read -r line; do
        echo "$(pwd)/$line" >> "$CURDIR"/"$OUTFILE"
	let "MISRNUM++"
done <temp.txt
rm temp.txt

##################################################
#           PREPROCESSING ERROR CHECKS           #
##################################################
		#############################
		# CHECK CHRONOLOGICAL ORDER #
		#############################
prevDate=""
prevRes=""
instrumentSection=""

while read -r line; do
	
	# continue if we have a non-valid line (comment or otherwise)
	if [[ ${line:0:1} != "." && ${line:0:1} != "/" && ${line:0:1} != ".." ]]; then
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
			continue
		fi

		# we now have the previous date and the current line. Now we need to find the
		# current date from the variable "curfilename" and compare that with the previous date
		curDate=${curfilename:6:8}
		if [[ "$curDate"<"$prevDate" || "$curDate"=="$prevDate" ]]; then
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
    # MODIS will have two checks. The first is to check for chronological consistency.
    # The second is to check that the files follow in either the order of:
    # MOD021KM
    # MOD03
    # or
    # MOD021KM
    # MOD02HKM
    # MOD02QKM
    # MOD03

    # TODO: Check for date consistency

    elif [ "$instrumentSection" == "MOD" ]; then
        # note: "res" refers to "MOD021KM", "MOD02HKM", "MOD02QKM", or "MOD03"
        if [ -z "$prevDate" ]; then
            prevfilename=$(echo ${line##*/})
            # extract the date
            tmp=${prevfilename#*.A}
            prevDate=$(echo ${tmp%%.*})
            prevRes=$(echo ${prevfilename%%.*})
            continue
        fi

        # CHECK FOR RESOLUTION CONSISTENCY

        tmp=${curfilename#*.A}
        curDate=$(echo ${tmp%%.*})
        curRes=$(echo ${curfilename%%.*})

        if [[ "$prevRes" == "MOD021KM" ]]; then
            if [[ "$curRes" != "MOD03" && "$curRes" != "MOD02HKM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n"
                printf "\tExpected to see MOD03 or MOD02HKM after MOD021KM.\n"
                printf "Exiting script.\n"
                exit 1
            fi
        elif [[ "$prevRes" == "MOD02HKM" ]]; then
            if [[ "$curRes" != "MOD02QKM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n"
                printf "\tExpected to see MOD02QKM after MOD02HKM.\n"
                printf "Exiting script.\n"
                exit 1
            fi
        elif [[ "$prevRes" == "MOD02QKM" ]]; then
            if [[ "$curRes" != "MOD03" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n"
                printf "\tExpected to see MOD03 after MOD02QKM.\n"
                printf "Exiting script.\n"
                exit 1
            fi
        elif [[ "$prevRes" == "MOD03" ]]; then
            if [[ "$curRes" != "MOD021KM" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "MODIS resolutions are out of order.\n" >&2
                printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n"
                printf "\tExpected to see MOD021KM after MOD03.\n"
                printf "Exiting script.\n"
                exit 1
            fi
        else
            printf "\e[4m\e[91mFatal Error\e[0m: " >&2
            printf "Unknown error at line $LINENO. Exiting script.\n"
            exit 1
        fi
        prevDate="$curDate"
        prevRes="$curRes"
        prevfilename="$curfilename"
    fi
        

done <"$CURDIR"/"test.txt"

printf "Granules read:\n"
printf "\tMOPITT: $MOPITTNUM ($(($MOPITTNUM * 1440)) minutes)\n"
printf "\tCERES:  $CERESNUM ($(($CERESNUM * 1440)) minutes)\n"
printf "\tMODIS:  $MODISNUM ($(($MODISNUM * 5)) minutes)\n"
printf "\tASTER:  $ASTERNUM ($(bc -l <<< "$ASTERNUM * 0.15") minutes)\n"
printf "\tMISR:   $MISRNUM (~$(($MISRNUM * 45))-$(($MISRNUM * 50)) minutes)\n"









