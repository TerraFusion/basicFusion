#! /bin/bash
if [ -e orbittotimelist.txt ]
    then
        rm orbittotimelist.txt
fi

libExist=$(echo $LD_LIBRARY_PATH | grep '/home/gyzhao/Mtk-src-1.4.3/lib')

if [ ${#libExist} -lt 2 ]; then
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH":/home/gyzhao/Mtk-src-1.4.3/lib
fi

d1='0'
d2='00'
for i in `seq 995 85302`;
do              
        pathnum=`/home/gyzhao/Mtk-src-1.4.3/bin/MtkOrbitToPath $i | cut -d: -f2 | cut -c2-4`
        if [ $pathnum -lt 10 ]
        then
                pn=$d2$pathnum
        elif [ $pathnum -lt  100 ]
        then
                #echo $pathnum
                pn="$d1""$pathnum"
        else
                pn=$pathnum
        fi
        echo $i $pn `/home/gyzhao/Mtk-src-1.4.3/bin/MtkOrbitToTimeRange $i` >> orbittotimelist.txt
done
