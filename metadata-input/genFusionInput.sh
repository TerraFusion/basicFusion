#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./genFusionFiles [orbit number]"
    exit 0
fi

OUTPUT=./inputFiles.txt

. ./queries.bash

rm -f "$OUTPUT"

instrumentOverlappingOrbit accesslist.sqlite $1 MOP >> "$OUTPUT"
instrumentOverlappingOrbit accesslist.sqlite $1 CER >> "$OUTPUT"
instrumentOverlappingOrbit accesslist.sqlite $1 MOD >> "$OUTPUT"
instrumentOverlappingOrbit accesslist.sqlite $1 AST >> "$OUTPUT"
instrumentOverlappingOrbit accesslist.sqlite $1 MIS >> "$OUTPUT"

