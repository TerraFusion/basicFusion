#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 [directory path]"
    exit 1
fi

SUFFIX=".h5"

printf "file size > 100GB: "
find "$1" -iname "*$SUFFIX" -type f -size +100G | wc -l

printf "80GB < file size < 100GB: "
find "$1" -iname "*$SUFFIX" -type f -size +80G -size -100G | wc -l

printf "60GB < file size < 80GB: "
find "$1" -iname "*$SUFFIX" -type f -size +60G -size -80G | wc -l

printf "40GB < file size < 60GB: "
find "$1" -iname "*$SUFFIX" -type f -size +40G -size -60G | wc -l

printf "20GB < file size < 40GB: "
find "$1" -iname "*$SUFFIX" -type f -size +20G -size -40G | wc -l

printf "10GB < file size < 20GB: "
find "$1" -iname "*$SUFFIX" -type f -size +10G -size -20G | wc -l

printf "1GB < file size < 10GB: "
find "$1" -iname "*$SUFFIX" -type f -size +1G -size -10G | wc -l

printf "100MB < file size < 1GB: "
find "$1" -iname "*$SUFFIX" -type f -size +100M -size -1G | wc -l

printf "10MB < file size < 100MB: "
find "$1" -iname "*$SUFFIX" -type f -size +10M -size -100M | wc -l

printf "1M < file size < 10MB: "
find "$1" -iname "*$SUFFIX" -type f -size +1M -size -10M | wc -l

printf "file size < 1MB: "
find "$1" -iname "*$SUFFIX" -type f -size -1M | wc -l

