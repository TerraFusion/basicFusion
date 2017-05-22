#!/bin/bash

if [ "$#" -ne 1 ]; then
        printf "Usage:\n\t$0 [Update file listing? 0 or 1]\n"
        exit 1
fi

if [ "$1" -eq 1 ]; then
    ./ROGER-findFiles
    for i in ASTER MODIS MISR MOPITT CERES; do
       gzip -f data/$i.list 
    done
fi

export PYTHONUSERBASE=/home/clipp/libs/python
export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:$PYTHONPATH
module load python/2.7.10
python fusionBuildDB -o --trace=trace.txt --discards=discards.txt --anomalies=errors.txt accesslist.sqlite data/Orbit_Path_Time.txt.gz data/MODIS.list.gz data/ASTER.list.gz data/MOPITT.list.gz data/MISR.list.gz data/CERES.list.gz > out.txt

