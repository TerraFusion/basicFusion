#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [output DB directory]"
    exit 1
fi

DBDIR=$1
findFiles="./ROGER-findFiles"
INDIR="/projects/TDataFus/BFData"
DATADIR="../data"

export PYTHONUSERBASE=/home/clipp/libs/python
export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:$PYTHONPATH
module load python/2.7.10


eval $findFiles "$INDIR"

if [ $? -ne 0 ]; then
    echo "$findFiles returned with exit status of $?."
    echo "Exiting script."
    exit 1
fi

for i in ASTER MODIS MISR MOPITT CERES; do
   gzip -f "$DATADIR"/$i.list 
    if [ $? -ne 0 ]; then
        echo "gzip returned with exit status of $?."
        echo "Exiting script."
        exit 1
    fi

done

python ./fusionBuildDB -o -q --discards=discards.txt --anomalies=errors.txt "$DBDIR" "$DATADIR"/Orbit_Path_Time.txt.gz "$DATADIR"/MODIS.list.gz "$DATADIR"/ASTER.list.gz "$DATADIR"/MOPITT.list.gz "$DATADIR"/MISR.list.gz "$DATADIR"/CERES.list.gz

if [ $? -ne 0 ]; then
    echo "fusionBuildDB returned with exit status of $?."
    echo "Exiting script."
    exit 1
fi

