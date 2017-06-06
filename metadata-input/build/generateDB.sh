#!/bin/bash
export PYTHONUSERBASE=/home/clipp/libs/python
export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:$PYTHONPATH
module load python/2.7.10

DATADIR="../data"
./ROGER-findFiles
for i in ASTER MODIS MISR MOPITT CERES; do
   gzip -f "$DATADIR"/$i.list 
done

python ./fusionBuildDB -o -q --discards=discards.txt --anomalies=errors.txt ../accesslist.sqlite "$DATADIR"/Orbit_Path_Time.txt.gz "$DATADIR"/MODIS.list.gz "$DATADIR"/ASTER.list.gz "$DATADIR"/MOPITT.list.gz "$DATADIR"/MISR.list.gz "$DATADIR"/CERES.list.gz
