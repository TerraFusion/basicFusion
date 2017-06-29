#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 [input HDF file directory] [output DB directory]"
    exit 1
fi

# Get the absolute path of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

DBDIR=$2                                        # Where the database will be stored
findFiles="$SCRIPT_DIR/genInputPosix"           # Where the findFiles script is located
INDIR=$1                                        # Where the input HDF files are located
DATADIR="$SCRIPT_DIR/../data"                   # Where to store intermediate results of the findFiles query

#_____PYTHON DEPENDENCY RESOLUTIONS__________#
# The fusionBuilDB python script requires:
# 1. Docopt
# 2. pytz
# 3. ???
#
# A virtual environment has been provided in externLibs to resolve these dependencies.
# Another option would have been to have each user install the required packages, but a virtual env
# is the most portable solution.

source "$SCRIPT_DIR"/../../externLib/pyVirtualEnv/BFBWpy/bin/activate

#____________________________________________#


genInputFiles=$(eval $findFiles "$DATADIR" "$INDIR/ASTER" "$INDIR/CERES" "$INDIR/MOPITT" "$INDIR/MISR" "$INDIR/MODIS")
echo "$genInputFiles"

retVal=$?
if [ $retVal -ne 0 ]; then
    echo "$findFiles returned with exit status of $retVal."
    echo "Exiting script."
    exit 1
fi

i=0
while IFS= read -r line; do
    fileList[$i]=$(cut -f2 -d' ' <<< $line)
    let "i++"
done <<< "$genInputFiles"

python ./fusionBuildDB -o --anomalies=anom.txt --discards=discards.txt --trace=trace.txt "$DBDIR" "$DATADIR"/Orbit_Path_Time.txt.gz ${fileList[0]} ${fileList[1]} ${fileList[2]} ${fileList[3]} ${fileList[4]}

retVal=$?
if [ $retVal -ne 0 ]; then
    echo "fusionBuildDB returned with exit status of $retVal."
    echo "Exiting script."
    exit 1
fi

deactivate
exit 0
