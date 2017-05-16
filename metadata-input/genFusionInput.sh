#!/bin/bash

# Args:
# 1 = unordered input file
# 2 = output file (properly ordered)

orderFiles() {

    MOPITTNUM=0
    CERESNUM=0
    MODISNUM=0
    ASTERNUM=0
    MISRNUM=0

    UNORDEREDFILE=$1
    OUTFILE=$2
    CURDIR=$(pwd)

    rm -f "$OUTFILE" 
    rm -r -f __tempFiles
    mkdir __tempFiles

    ##########
    # MOPITT #
    ##########
    # grep -v removes any "xml" files
    # put results of ls into a temporary text file
    cat "$UNORDEREDFILE" | grep "MOP.*he5" | grep -v "xml"  >> "$CURDIR"/__tempFiles/MOPITT.txt
    # iterate over this file, prepending the path of the file into our
    # OUTFILE
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MOPITTNUM++"
    done <"$CURDIR"/__tempFiles/MOPITT.txt

    #########
    # CERES #
    #########
    # grep -v removes any files with "met" in them, they're unwanted
    cat "$UNORDEREDFILE" | grep "CER_SSF_Terra.*FM1.*" | grep -v "met" >> "$CURDIR"/__tempFiles/CERESFM1.txt
    cat "$UNORDEREDFILE" | grep "CER_SSF_Terra.*FM2.*" | grep -v "met" >> "$CURDIR"/__tempFiles/CERESFM2.txt

    while read -r line; do
        echo "$line" >> "$OUTFILE"
        # Find the corresponding FM2 file to add
        FM1date=${line##*.}
        FM2line=$(cat "$CURDIR"/__tempFiles/CERESFM2.txt | grep "$FM1date")
        if [[ ${#FM2line} == 0 ]]; then
            printf "\e[4m\e[91mFatal Error\e[0m: " >&2
            printf "\tCould not find a corresponding FM2 file for $line\nExiting script.\n" >&2
            rm -r "$CURDIR"/__tempFiles
            exit 1
        else
            let "CERESNUM++"
        fi

        echo "$FM2line" >> "$OUTFILE"
        let "CERESNUM++"
    done <"$CURDIR"/__tempFiles/CERESFM1.txt


    #########
    # MODIS #
    #########
    # For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
    # into separate text files to make it easy to handle

    cat "$UNORDEREDFILE" | grep "MOD021KM" | grep "hdf" >> "$CURDIR"/__tempFiles/1KMfile.txt
    cat "$UNORDEREDFILE" | grep "MOD02HKM" | grep "hdf" >> "$CURDIR"/__tempFiles/HKMfile.txt
    cat "$UNORDEREDFILE" | grep "MOD02QKM" | grep "hdf" >> "$CURDIR"/__tempFiles/QKMfile.txt
    cat "$UNORDEREDFILE" | grep "MOD03" | grep "hdf" >> "$CURDIR"/__tempFiles/MOD03file.txt

    while read -r line; do
        # first, echo this 1KM filename into the outfile
        echo "$line" >> "$OUTFILE"
        let "MODISNUM++"
        # then extract the substring in this line containing the date info
        # (using shell parameter expansion)
        substr="${line##*/}"

        substr="${substr:10:16}"

        # write the corresponding HKM filename into our outfile.
        # note that it may not exist.
        temp="$(cat $CURDIR/__tempFiles/HKMfile.txt | grep $substr)"
        if [ ${#temp} -ge 2 ]; then # if temp is a non-zero length string
            echo "$temp" >> "$OUTFILE"
            let "MODISNUM++"
        fi

        # write in the corrseponding QKM file
        temp="$(cat $CURDIR/__tempFiles/QKMfile.txt | grep $substr)"
        if [ ${#temp} -ge 2 ]; then
                echo "$temp" >> "$OUTFILE"
                let "MODISNUM++"
        fi

        # write in the corresponding MOD03 file
        temp="$(cat $CURDIR/__tempFiles/MOD03file.txt | grep $substr)"
        if [ ${#temp} -ge 2 ]; then
            echo "$temp" >> "$OUTFILE"
            let "MODISNUM++"
        fi

    done <"$CURDIR"/__tempFiles/1KMfile.txt

    #########
    # ASTER #
    #########

    cat "$UNORDEREDFILE" | grep "AST" | grep "hdf" >> "$CURDIR"/__tempFiles/ASTER.txt
    # iterate over this file, prepending the path of the file into our
    # OUTFILE

    while read -r line; do
            echo "$line" >> "$OUTFILE"
        let "ASTERNUM++"
    done <"$CURDIR"/__tempFiles/ASTER.txt

    ########
    # MISR #
    ########

    # put the MISR files into temp text files
    cat "$UNORDEREDFILE" | grep "MISR_AM1_GRP_ELLIPSOID" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_GRP.txt
    cat "$UNORDEREDFILE" | grep "MISR_AM1_AGP" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_AGP.txt
    cat "$UNORDEREDFILE" | grep "MISR_AM1_GP" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_GP.txt
    cat "$UNORDEREDFILE" | grep "MISR_HRLL_" | grep "hdf" >> "$CURDIR"/__tempFiles/MISR_HRLL.txt
    # iterate over this file, prepending the path of the file into our
    # OUTFILE

    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <"$CURDIR"/__tempFiles/MISR_GRP.txt
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <"$CURDIR"/__tempFiles/MISR_AGP.txt
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <"$CURDIR"/__tempFiles/MISR_GP.txt
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <"$CURDIR"/__tempFiles/MISR_HRLL.txt

    rm -r "$CURDIR"/__tempFiles
}

if [ "$#" -ne 1 ]; then
    echo "Usage: ./genFusionFiles [orbit number]"
    exit 0
fi

DB=accesslist.sqlite
UNORDERED=./unorderedFiles.txt
ORDERED=./inputFiles.txt

. ./queries.bash

rm -f "$ORDERED"
rm -f "$UNORDERED"

instrumentOverlappingOrbit "$DB" $1 MOP >> "$UNORDERED"
instrumentOverlappingOrbit "$DB" $1 CER >> "$UNORDERED"
instrumentOverlappingOrbit "$DB" $1 MOD >> "$UNORDERED"
instrumentOverlappingOrbit "$DB" $1 AST >> "$UNORDERED"
instrumentOverlappingOrbit "$DB" $1 MIS >> "$UNORDERED"

orderFiles $UNORDERED $ORDERED


