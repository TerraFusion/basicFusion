#!/bin/bash
export PYTHONUSERBASE=/home/clipp/libs/python
export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:$PYTHONPATH
module load python/2.7.10

./ROGER-findFiles
for i in ASTER MODIS MISR MOPITT CERES; do
   gzip -f data/$i.list 
done

python fusionBuildDB -o -q --discards=discards.txt --anomalies=errors.txt accesslist.sqlite data/Orbit_Path_Time.txt.gz data/MODIS.list.gz data/ASTER.list.gz data/MOPITT.list.gz data/MISR.list.gz data/CERES.list.gz
