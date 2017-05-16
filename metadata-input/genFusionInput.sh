#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./genFusionFiles [orbit number]"
    exit 0
fi

DB=/u/sciteam/draila/accesslist.sqlite
OUTPUT=./inputFiles.txt

. ./queries.bash

rm -f "$OUTPUT"

instrumentOverlappingOrbit "$DB" $1 MOP >> "$OUTPUT"
instrumentOverlappingOrbit "$DB" $1 CER >> "$OUTPUT"
instrumentOverlappingOrbit "$DB" $1 MOD >> "$OUTPUT"
instrumentOverlappingOrbit "$DB" $1 AST >> "$OUTPUT"
instrumentOverlappingOrbit "$DB" $1 MIS >> "$OUTPUT"

