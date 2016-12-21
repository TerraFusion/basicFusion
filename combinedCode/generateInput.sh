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
ls | grep "MOP" | grep "he5" >> temp.txt
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

printf "Granules read:\n"
printf "\tMOPITT: $MOPITTNUM ($(($MOPITTNUM * 1440)) minutes)\n"
printf "\tCERES:  $CERESNUM ($(($CERESNUM * 1440)) minutes)\n"
printf "\tMODIS:  $MODISNUM ($(($MODISNUM * 5)) minutes)\n"
printf "\tASTER:  $ASTERNUM ($(bc -l <<< "$ASTERNUM * 0.15") minutes)\n"
printf "\tMISR:   $MISRNUM (~$(($MISRNUM * 45))-$(($MISRNUM * 50)) minutes)\n"









