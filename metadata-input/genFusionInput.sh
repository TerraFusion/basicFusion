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

    local MOPGREP
    local CERFM1GREP
    local CERFM2GREP
    local MOD1KMGREP
    local MODHKMGREP
    local MODQKMGREP
    local MOD03GREP
    local ASTGREP
    local MISGRPGREP
    local MISAGPGREP
    local MISGPGREP
    local MISHRLLGREP

    rm -f "$OUTFILE" 

    ##########
    # MOPITT #
    ##########
    # grep -v removes any "xml" files
    # put results of ls into a temporary text file
    MOPGREP=$(cat "$UNORDEREDFILE" | grep -e "MOP.*he5" -e 'MOP N/A' | grep -v "xml"  | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE
    if [[ ! "$MOPGREP" == *"MOP N/A"* ]]; then
        while read -r line; do
            echo "$line" >> "$OUTFILE"
            let "MOPITTNUM++"
        done <<< "$MOPGREP"
    else
        echo "MOP N/A" >> "$OUTFILE"
    fi

    #########
    # CERES #
    #########
    # grep -v removes any files with "met" in them, they're unwanted
    CERFM1GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM1.*" -e 'CER N/A' | grep -v "met" | sort)
    CERFM2GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM2.*" | grep -v "met" | sort)

    if [[ ! "$CERFM1GREP" == *"CER N/A"* ]]; then
        while read -r line; do
            echo "$line" >> "$OUTFILE"
            # Find the corresponding FM2 file to add
            FM1date=${line##*.}
            FM2line=$(echo "$CERFM2GREP" | grep "$FM1date")
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
        done <<< "$CERFM1GREP"
    else
        echo "CER N/A" >> "$OUTFILE"
    fi

    #########
    # MODIS #
    #########
    # For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
    # into separate text files to make it easy to handle

    MOD1KMGREP=$(cat "$UNORDEREDFILE" | grep -e "MOD021KM.*hdf" -e 'MOD N/A' | sort)
    MODHKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02HKM.*hdf"  | sort)
    MODQKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02QKM.*hdf"  | sort)
    MOD03GREP=$(cat "$UNORDEREDFILE" | grep "MOD03.*hdf" | sort)


    if [[ ! "$MOD1KMGREP" == *"MOD N/A"* ]]; then
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
            temp="$(echo "$MODHKMGREP" | grep $substr)"

            # I'm having trouble with the same file being included twice during the grepping of the substring.
            # resolved by grabbing only the first line in $temp
            temp=$(echo "$temp" | sed -n '1 p')
            if [ ${#temp} -ge 2 ]; then # if temp is a non-zero length string
                echo "$temp" >> "$OUTFILE"
                let "MODISNUM++"
            fi

            # write in the corrseponding QKM file
            temp="$(echo "$MODQKMGREP" | grep $substr)"
            temp=$(echo "$temp" | sed -n '1 p')
            if [ ${#temp} -ge 2 ]; then
                    echo "$temp" >> "$OUTFILE"
                    let "MODISNUM++"
            fi

            # write in the corresponding MOD03 file
            temp="$(echo "$MOD03GREP" | grep $substr)"
            temp="$(echo "$temp" | sed -n '1 p')"
            if [ ${#temp} -ge 2 ]; then
                echo "$temp" >> "$OUTFILE"
                let "MODISNUM++"
            fi

        done <<< "$MOD1KMGREP"
    else
        echo "MOD N/A" >> "$OUTFILE"
    fi

    #########
    # ASTER #
    #########

    ASTGREP=$(cat "$UNORDEREDFILE" | grep -e "AST.*hdf" -e 'AST N/A' | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE
    if [[ ! "$ASTGREP" == *"AST N/A"* ]]; then
        while read -r line; do
                echo "$line" >> "$OUTFILE"
            let "ASTERNUM++"
        done <<< "$ASTGREP"
    else
        echo "AST N/A" >> "$OUTFILE"
    fi

    ########
    # MISR #
    ########

    # put the MISR files into temp text files
    MISGRPGREP=$(cat "$UNORDEREDFILE" | grep -e "MISR_AM1_GRP_ELLIPSOID.*hdf" -e 'MIS N/A' | sort)
    MISAGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_AGP.*hdf" | sort)
    MISGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_GP.*hdf"  | sort)
    MISHRLLGREP=$(cat "$UNORDEREDFILE" | grep "MISR_HRLL_.*hdf"   | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE

    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <<< "$MISGRPGREP"
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <<< "$MISAGPGREP"
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <<< "$MISGPGREP"
    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
    done <<< "$MISHRLLGREP"

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

MOPLINES=$(instrumentOverlappingOrbit "$DB" $1 MOP)
CERLINES=$(instrumentOverlappingOrbit "$DB" $1 CER)
MODLINES=$(instrumentOverlappingOrbit "$DB" $1 MOD)
ASTLINES=$(instrumentOverlappingOrbit "$DB" $1 AST)
MISLINES=$(instrumentOverlappingOrbit "$DB" $1 MIS)

if [ ${#MOPLINES} -eq 0 ]; then
    echo "MOP N/A" >> "$UNORDERED"
else
    echo "$MOPLINES" >> "$UNORDERED"
fi

if [ ${#CERLINES} -eq 0 ]; then
    echo "CER N/A" >> "$UNORDERED"
else
    echo "$CERLINES" >> "$UNORDERED"
fi

if [ ${#MODLINES} -eq 0 ]; then
    echo "MOD N/A" >> "$UNORDERED"
else
    echo "$MODLINES" >> "$UNORDERED"
fi

if [ ${#ASTLINES} -eq 0 ]; then
    echo "AST N/A" >> "$UNORDERED"
else
    echo "$ASTLINES" >> "$UNORDERED"
fi

if [ ${#MISLINES} -eq 0 ]; then
    echo "MIS N/A" >> "$UNORDERED"
else
    echo "$MISLINES" >> "$UNORDERED"
fi


orderFiles $UNORDERED $ORDERED

# Add the orbit number to the top of the file
echo -e "$1\n$(cat $ORDERED)" > "$ORDERED"
