#!/bin/bash
# Prints the number of lines of code for the entire project (for C and BASH programs)

cd .. && git ls-files | sed -r -n -e '/.c|.h|.sh|.pbs|jobSubmit|loadModules/p' | sed '/inputFileDebug/d' | sed '/.txt/d' | xargs wc -l
