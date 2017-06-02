#!/bin/bash

START=69365
END=$(($START+40))

rm -f errors.txt
touch errors.txt

for i in $(seq $START $END); do
    echo "Input $i" >> errors.txt
    rm -f "../fusionTestInput/input$i.txt"
    ./genFusionInput.sh $i "../fusionTestInput/input$i.txt" 2>> errors.txt
    echo >> errors.txt
done
