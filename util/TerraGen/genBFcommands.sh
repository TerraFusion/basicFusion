#!/bin/bash

if [ $# -ne 6 ]; then
    echo
    echo "Usage: $0 [orbit start] [orbit end] [input file list path] [output commands path] [basicFusion project path]"
    echo "          [output basicFusion HDF5 path]"
    echo
    printf "This script generates the proper command file for GNU parallel to execute basicFusion program. "
    printf "Each line in the output file corresponds to a single call to the basic fusion program. "
    printf "Each input file is assumed to be present in the input file list path.\n"
    echo
    printf "Each input file must be named \"input[orbitNum].txt\".\n"
    echo
    printf "The input text file, output, and basicFusion project paths should be directory paths,\nnot a path to any file.\n"
    echo
    printf "ARGUMENTS:\n"
    printf "[orbit start]   -- Starting orbit\n"
    printf "[orbit end]     -- Ending orbit\n"
    printf "[input file list path]  -- Directory that contains ONLY the BF input file list files, formatted as input[orbitNum].txt\n"
    printf "[output commands path]  -- Directory where the output commands file will be stored\n"
    printf "[basicFusion project path] -- Absolute directory to your local basicFusion repository. /path/to/basicFusion\n"
    echo
    exit 1
fi


START=$1
END=$2
INFILES=$3
CMD_PATH=$4
PROJPATH=$5
HDF5_PATH=$6
CURDIR=$(pwd)

#__________________VARIOUS VARIABLES____________________#

BINFILES="$PROJPATH/bin"
BF_PROG="$BINFILES/basicFusion"
ORBIT_INFO="$BINFILES/orbit_info.bin"
DB="$PROJPATH/metadata-input/accesslist.sqlite"
FILENAME='TERRA_BF_L1B_O${ORBIT}_F000_V000.h5'
CMD_FILE="$CMD_PATH/parallel_BF_CMD_${START}_${END}.txt"

#_______________________________________________________#

mkdir -p "$CMD_PATH"
mkdir -p "$HDF5_PATH/errors"
rm -f "$CMD_FILE"


for ORBIT in $(seq $START $END ); do
    export ORBIT=$ORBIT
    evalFileName=$(eval echo "$FILENAME")
    echo "$BF_PROG $HDF5_PATH/$evalFileName $INFILES/input$ORBIT.txt $ORBIT_INFO &> $HDF5_PATH/errors/errors$ORBIT.txt" >> "$CMD_FILE"
done

unset ORBIT

exit 0

