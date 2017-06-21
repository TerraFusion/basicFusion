#!/bin/bash

# Args:
# 1 = unordered input file
# 2 = output file (properly ordered)

orderFiles() {

    local MOPITTNUM=0
    local CERESNUM=0
    local MODISNUM=0
    local ASTERNUM=0
    local MISRNUM=0

    UNORDEREDFILE=$1
    OUTFILE=$2
    CURDIR=$(pwd)

    local MOPGREP=''
    local CERFM1GREP=''
    local CERFM2GREP=''
    local MOD1KMGREP=''
    local MODHKMGREP=''
    local MODQKMGREP=''
    local MOD03GREP=''
    local ASTGREP=''
    local MISGRPGREP=''
    local MISAGPGREP=''
    local MISGPGREP=''
    local MISHRLLGREP=''

    rm -f "$OUTFILE" 

    ##########
    # MOPITT #
    ##########
    # grep -v removes any "xml" files
    # put results of ls into a variable
    MOPGREP=$(cat "$UNORDEREDFILE" | grep -e "MOP.*he5" -e 'MOP N/A' | grep -v "xml"  | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE
    if [[ ! "$MOPGREP" == *"MOP N/A"* ]]; then
        while read -r line; do
            echo "$line" >> "$OUTFILE"
            let "MOPITTNUM++"
        done <<< "$MOPGREP"
    else
        echo "MOP N/A" >> "$OUTFILE"
    fi

    #########
    # CERES #
    #########
    # grep -v removes any files with "met" in them, they're unwanted
    CERFM1GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM1.*" -e 'CER N/A' | grep -v "met" | sort)
    CERFM2GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM2.*" | grep -v "met" | sort)

    if [[ ! "$CERFM1GREP" == *"CER N/A"* ]]; then
        while read -r line; do
            echo "$line" >> "$OUTFILE"
            # Find the corresponding FM2 file to add
            FM1date=${line##*.}
            FM2line=$(echo "$CERFM2GREP" | grep "$FM1date")
            if [[ ${#FM2line} == 0 ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "\tCould not find a corresponding FM2 file for $line.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                return 1
            else
                let "CERESNUM++"
            fi

            echo "$FM2line" >> "$OUTFILE"
            let "CERESNUM++"
        done <<< "$CERFM1GREP"
    else
        echo "CER N/A" >> "$OUTFILE"
    fi

    #########
    # MODIS #
    #########
    # For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
    # into separate text files to make it easy to handle

    MOD1KMGREP=$(cat "$UNORDEREDFILE" | grep -e "MOD021KM.*hdf" -e 'MOD N/A' | sort)
    MODHKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02HKM.*hdf"  | sort)
    MODQKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02QKM.*hdf"  | sort)
    MOD03GREP=$(cat "$UNORDEREDFILE" | grep "MOD03.*hdf" | sort)


    if [[ ! "$MOD1KMGREP" == *"MOD N/A"* ]]; then
        while read -r line; do
            # first, echo this 1KM filename into the outfile
            echo "$line" >> "$OUTFILE"
            let "MODISNUM++"
            # then extract the substring in this line containing the date info
            # (using shell parameter expansion)
            substr="${line##*/}"

            substr="${substr:10:16}"

            # write the corresponding HKM filename into our outfile.
            # note that it may not exist.
            temp="$(echo "$MODHKMGREP" | grep $substr)"

            # I'm having trouble with the same file being included twice during the grepping of the substring.
            # resolved by grabbing only the first line in $temp
            temp=$(echo "$temp" | sed -n '1 p')
            if [ ${#temp} -ge 2 ]; then # if temp is a non-zero length string
                echo "$temp" >> "$OUTFILE"
                let "MODISNUM++"
            fi

            # write in the corrseponding QKM file
            temp="$(echo "$MODQKMGREP" | grep $substr)"
            temp=$(echo "$temp" | sed -n '1 p')
            if [ ${#temp} -ge 2 ]; then
                    echo "$temp" >> "$OUTFILE"
                    let "MODISNUM++"
            fi

            # write in the corresponding MOD03 file
            temp="$(echo "$MOD03GREP" | grep $substr)"
            temp="$(echo "$temp" | sed -n '1 p')"
            if [ ${#temp} -ge 2 ]; then
                echo "$temp" >> "$OUTFILE"
                let "MODISNUM++"
            fi

        done <<< "$MOD1KMGREP"
    else
        echo "MOD N/A" >> "$OUTFILE"
    fi

    #########
    # ASTER #
    #########

    ASTGREP=$(cat "$UNORDEREDFILE" | grep -e "AST.*hdf" -e 'AST N/A' | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE
    if [[ ! "$ASTGREP" == *"AST N/A"* ]]; then
        while read -r line; do
                echo "$line" >> "$OUTFILE"
            let "ASTERNUM++"
        done <<< "$ASTGREP"
    else
        echo "AST N/A" >> "$OUTFILE"
    fi

    ########
    # MISR #
    ########

    # put the MISR files into temp text files
    MISGRPGREP=$(cat "$UNORDEREDFILE" | grep -e "MISR_AM1_GRP_ELLIPSOID.*hdf" -e 'MIS N/A' | sort)
    MISAGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_AGP.*hdf" | sort)
    MISGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_GP.*hdf"  | sort)
    MISHRLLGREP=$(cat "$UNORDEREDFILE" | grep "MISR_HRLL_.*hdf"   | sort)
    # iterate over this file, prepending the path of the file into our
    # OUTFILE

    while read -r line; do
        echo "$line" >> "$OUTFILE"
        let "MISRNUM++"
        if [[ $line == *"_DF_"* ]]; then
            pathnum=$(cut -d'_' -f 6 <<< "$line")
            orbitnum=$(cut -d'_' -f 7 <<< "$line")
            echo $(grep "$pathnum" <<< "$MISAGPGREP") >> "$OUTFILE"
            echo $(grep "$orbitnum" <<< "$MISGPGREP") >> "$OUTFILE"
            echo $(grep "$pathnum" <<< "$MISHRLLGREP") >> "$OUTFILE"
        fi
    done <<< "$MISGRPGREP"

    return 0
}

# Args:
# 1 = ordered input file
#
verifyFiles()
{
    local DEBUGFILE="$1"
    local prevDate=""
    local prevRes=""
    local prevGroupDate=""
    local curGroupDate=""
    local instrumentSection=""
    local MODISVersionMismatch=0
    local ASTERVersionMismatch=0
    local MISRVersionMismatch=0
    local MISRNUM=0
    
    # Read through the DEBUGFILE file.
    # Remember, DEBUGFILE is the variable our script will do the debugging on. It may
    # or may not be set to the OUTFILE variable.
    linenum=1

    while read -r line; do
        # Check to make sure that each line is either a comment, a newline or a path.
        # If it is none of them, throw an error. This line would cause the C program
        # downstream to crash because if the line isn't a comment or newline,
        # it will interpret it as a file path.
       
         
        # continue if we have a non-valid line (comment or otherwise)
        if [[ ${#line} == 0 || ${line:0:1} == "#" || "$line" == "MOP N/A" || "$line" == "CER N/A" || "$line" == "MIS N/A" || "$line" == "MOD N/A" || "$line" == "AST N/A" ]]; then
            let "linenum++"
            continue
        fi

        if [[ ${line:0:1} != "." && ${line:0:1} != "/" && ${line:0:1} != ".." && ${line:0:1} != "#" && ${line:0:1} != "\n" && ${line:0:1} != "\0" && ${#line} != 0 ]]; then
            printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
            printf "\tAt line number: $linenum\n"
            printf "\t\"$line\" is an invalid line.\n" >&2
            return 1
        fi
        

        # check to see if we have entered into a new instrument section.
        # Note that internally we represent the current instrument we're reading
        # by the first 3 characters of the respective filename. For instance, MOPITT
        # would be "MOP", MODIS would be "MOD", MISR would be "MIS" etc.
        # Remember, these are coming from the FILENAMES themselves.
        curfilename=$(echo ${line##*/})
        
        if [ "$instrumentSection" != "${curfilename:0:3}" ]; then
            instrumentSection=${curfilename:0:3}
            # unset prevDate because we are starting a new section.
            unset prevDate
            unset prevGroupDate
            unset prevRes
            unset curGroupDate
        fi

        # note about the "prev" variables:
        #   If only one file is present for that instrment, no checking will need
        #   to be done. This code accounts for that.

                           ##########
                           # MOPITT #
                           ##########

        if [ "$instrumentSection" == "MOP" ]; then
            
            # line now contains a valid file path. Check to see if prevDate has been set.
            # If it has not, set it to the value of line and continue to the next line
            if [ -z "$prevDate" ]; then
                # get the date information from line
                # first find the filename itself (truncate line by cutting off the path)
                prevfilename=$(echo ${line##*/})
                #set prevDate to the date contained in filename
                prevDate=${prevfilename:6:8}
                # continue while loop
                let "linenum++"
                continue
            fi

            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=${curfilename:6:8}
            if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MOPITT files found to be out of order.\n" >&2
                printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
            fi

            prevDate="$curDate"
            prevfilename="$curfilename"

                          #########
                          # CERES #
                          #########

        elif [ "$instrumentSection" == "CER" ]; then
            # line now contains a valid file path. Check to see if prevDate has been set.
            # If it has not, set it to the value of line and continue to the next line
            if [ -z "$prevDate" ]; then
                # get the date information from line
                # set prevDate to the date contained in filename
                prevfilename=$(echo ${line##*/})
                prevDate=$(echo ${prevfilename##*.})
                # continue while loop
                let "linenum++"
                continue
            fi

            
            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=${curfilename##*.}

            prevCam=${prevfilename:14:3}
            curCam=${curfilename:14:3}

            if [[ "$curCam" == "FM1" ]]; then
                if [[ ${#prevGroupDate} != 0 ]]; then
                    if [[ $curDate < $prevGroupDate || $curDate == $prevGroupDate ]]; then
                        printf "\e[4m\e[93mWarning\e[0m: " >&2
                        printf "CERES files found to be out of order.\n" >&2
                        printf "\tDate of previous FM1/FM2 pair was $prevGroupDate.\n" >&2
                        printf "\tDate of current FM1/FM2 pair is $curDate.\n" >&2
                        printf "\tThe dates between pairs should be strictly increasing down the file list.\n" >&2
                    fi
                fi
                prevGroupDate="$curDate"
            fi
            if [[ "$prevCam" == "$curCam" ]]; then
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "CERES FM1 followed FM1 or FM2 followed FM2.\n" >&2
                printf "\tAn FM1 file should always be paired with an FM2 file!\n" >&2
                printf "\tFound at line $linenum\n" >&2
                return 1
            elif [[ "$prevCam" != "$curCam" && "$curCam" == "FM2" ]]; then
                if [[ "$curDate" != "$prevDate" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "FM1/FM2 pair don't have the same date.\n" >&2
                    printf "\tIf an FM1 file is followed by FM2, they must have the same date.\n"
                    printf "\tprevDate=$prevDate curDate=$curDate"
                    printf "\tLine $linenum\n"
                fi
            fi

            prevDate="$curDate"
            prevfilename="$curfilename"

                          #########
                          # MODIS #
                          #########
        # MODIS will have three checks. The first is to check for chronological consistency.
        # The second is to check that the files follow in either the order of:
        # MOD021KM
        # MOD03
        # or
        # MOD021KM
        # MOD02HKM
        # MOD02QKM
        # MOD03
        #
        # The third will throw a warning if the files have different version numbers

        elif [ "$instrumentSection" == "MOD" ]; then
            # note: "res" refers to "MOD021KM", "MOD02HKM", "MOD02QKM", or "MOD03"
            if [ -z "$prevDate" ]; then
                prevfilename=$(echo ${line##*/})
                # extract the date
                tmp=${prevfilename#*.A}
                prevDate=$(echo "$tmp" | cut -f1,2 -d'.')
                prevRes=$(echo ${prevfilename%%.*})
                curGroupDate="$prevDate"
                let "linenum++"
                continue
            fi

            # CHECK FOR RESOLUTION CONSISTENCY

            tmp=${curfilename#*.A}
            curDate=$(echo "$tmp" | cut -f1,2 -d'.')
            curRes=$(echo ${curfilename%%.*})

            if [[ "$prevRes" == "MOD021KM" ]]; then
                if [[ "$curRes" != "MOD03" && "$curRes" != "MOD02HKM" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD03 or MOD02HKM after MOD021KM.\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD02HKM" ]]; then
                if [[ "$curRes" != "MOD02QKM" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD02QKM after MOD02HKM.\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD02QKM" ]]; then
                if [[ "$curRes" != "MOD03" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD03 after MOD02QKM.\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD03" ]]; then
                if [[ "$curRes" != "MOD021KM" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD021KM after MOD03.\n" >&2
                    return 1
                fi
            else
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "Unknown error at line $LINENO.\n" >&2
                return 1
            fi

            # CHECK FOR DATE CONSISTENCY

            # Internally, we will consider the file "MOD021KM" to be the indication that a new
            # "group" has begun i.e.:
            # MOD021KM...
            # MOD03...
            # or
            # MOD021KM...
            # MOD02HKM...
            # MOD02QKM
            # MOD03
            
            # If the current line is a MOD021KM file, we have found the beginning of a new group
            
            if [[ "$curRes" == "MOD021KM" ]]; then
                prevGroupDate="$curGroupDate"
                curGroupDate="$curDate"
                # if prevGroupDate has not yet been set (we are on our first group)
                if [ -z "$prevGroupDate" ]; then
                    continue
                # else check if the current group's date correctly follows the previous group's date
                else
                    if [[ "$curDate" < "$prevGroupDate" || "$curDate" == "$prevGroupDate" ]]; then
                        printf "\e[4m\e[93mWarning\e[0m: " >&2
                        printf "MODIS date inconsistency found.\n"
                        printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                        printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                        printf "\tDates should be strictly increasing between groups!\n" >&2
                    fi
                fi
            fi

            # check that the current line's date is the same as the rest its group. Each file in a group should
            # have the same date.
            if [[ "$curDate" != "$curGroupDate" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MODIS date inconsistency found.\n"
                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                printf "\tThese two dates should be equal.\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous MODIS file, then skip this step
            
            if [[ ${#prevfilename} != 0 && $MODISVersionMismatch == 0 ]]; then
                prevVersion=$(echo "$prevfilename" | cut -f4,4 -d'.')
                curVersion=$(echo "$curfilename" | cut -f4,4 -d'.')
                
                if [[ "$prevVersion" != "$curVersion" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MODIS file version mismatch.\n" >&2
                    printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                    printf "\tWill not print any more MODIS warnings of this type.\n" >&2
                    MODISVersionMismatch=1
                fi
            fi

            prevDate="$curDate"
            prevRes="$curRes"
            prevfilename="$curfilename"

                    #########
                    # ASTER #
                    #########

        elif [ "$instrumentSection" == "AST" ]; then
            # line now contains a valid file path. Check to see if prevDate has been set.
            # If it has not, set it to the value of line and continue to the next line
            if [ -z "$prevDate" ]; then
                # get the date information from line
                #set prevDate to the date contained in filename
                prevfilename=$(echo ${line##*/})
                prevDate=$(echo "$prevfilename" | cut -f3,3 -d'_')
                prevVersion=${prevDate:0:3}
                prevDate="${prevDate:3}"
                # continue while loop
                let "linenum++"
                continue
            fi

            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=$(echo "$curfilename" | cut -f3,3 -d'_')
            curVersion=${curDate:0:3}
            curDate="${curDate:3}"
            if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "ASTER files found to be out of order.\n" >&2
                printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous ASTER file, then skip this step

            if [[ ${#prevfilename} != 0 && $ASTERVersionMismatch == 0 ]]; then

                if [[ "$prevVersion" != "$curVersion" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "ASTER file version mismatch.\n" >&2
                    printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                    printf "\tWill not print any more ASTER warnings of this type.\n" >&2
                    ASTERVersionMismatch=1
                fi
            fi


            prevDate="$curDate"
            prevfilename="$curfilename"
            prevVersion="$curVersion"

           
                ########
                # MISR #
                ######## 

        elif [ "$instrumentSection" == "MIS" ]; then
            
            let "MISRNUM++"

            # note: "cam" refers to which camera, ie "AA", "AF", "AN" etc.
            if [ -z "$prevDate" ]; then
                prevfilename=$(echo ${line##*/})
                # extract the date
                prevDate=$(echo "$prevfilename" | cut -f6,7 -d'_')
                prevCam=$(echo "$prevfilename" | cut -f8,8 -d'_' )
                curGroupDate="$prevDate"
                let "linenum++"
                continue
            fi

            # CHECK FOR CAMEREA CONSISTENCY

            curDate=$(echo "$curfilename" | cut -f6,7 -d'_')
            curCam=$(echo "$curfilename" | cut -f8,8 -d'_' )

            if [[ "$prevCam" == "AA" ]]; then
                if [[ "$curCam" != "AF" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AF after AA.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "AF" ]]; then
                if [[ "$curCam" != "AN" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AN after AF.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "AN" ]]; then
                if [[ "$curCam" != "BA" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BA after AN.\n" >&2
                    return 1
                fi

            elif [[ "$prevCam" == "BA" ]]; then
                if [[ "$curCam" != "BF" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BF after BA.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "BF" ]]; then
                if [[ "$curCam" != "CA" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CA after BF.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "CA" ]]; then
                if [[ "$curCam" != "CF" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CF after CA.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "CF" ]]; then
                if [[ "$curCam" != "DA" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DA after CF.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "DA" ]]; then
                if [[ "$curCam" != "DF" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DF after DA.\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "DF" ]]; then
                if [[ "$(echo "$curfilename" | cut -f7,3 -d'_')" != "AGP" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\"\n" >&2
                    printf "\tExpected to see an AGP file after a DF camera.\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" != "GP" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see GP after AGP file.\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f2,2 -d'_')" != "HRLL" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see HRLL file after GP file.\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f2,2 -d'_')" == "HRLL" ]]; then
                if [[ "$curCam" != "AA" ]]; then
                    printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see AA file after HRLL file.\n" >&2
                    return 1
                fi
            else
                printf "\e[4m\e[91mFatal Error\e[0m: " >&2
                printf "Unknown error at line $LINENO.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                return 1
            fi

            # CHECK FOR DATE CONSISTENCY
            
           
            if [[ "$curCam" == "AA" ]]; then
                prevGroupDate="$curGroupDate"
                curGroupDate="$curDate"
                # if prevGroupDate has not yet been set (we are on our first group)
                if [ -z "$prevGroupDate" ]; then
                    continue
                # else check if the current group's date correctly follows the previous group's date
                else
                    if [[ "$curDate" < "$prevGroupDate" || "$curDate" == "$prevGroupDate" ]]; then
                        printf "\e[4m\e[93mWarning\e[0m: " >&2
                        printf "MISR date inconsistency found.\n"
                        printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                        printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                        printf "\tDates should be strictly increasing between groups!\n" >&2
                    fi
                fi
            fi

            # check that the current line's date is the same as the rest its group. Each file in a group should
            # have the same date.

            # if the current line is an AGP file, we just need to check the path number "Pxxx"
            if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
                # if the current line's path number is different than the previous line's
                curpath="$(echo "$curfilename" | cut -f4,4 -d'_')"
                prevpath="$(echo "$prevfilename" | cut -f6,6 -d'_')"
                if [[ "$curpath" != "$prevpath" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MISR file inconsistency found.\n"
                    printf "\t$curfilename has a path number: $curpath != $prevpath\n" >&2
                    printf "\tThese two path numbers should be equal.\n" >&2
                fi
            elif [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
                curDate=$(echo "$curfilename" | cut -f5,6 -d'_')
                if [[ "$curDate" != "$curGroupDate" ]]; then
                    printf "\e[4m\e[93mWarning\e[0m: " >&2
                    printf "MISR date inconsistency found.\n"
                    printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                    printf "\tThese two dates should be equal.\n" >&2
                fi
            # else we need to check the dates between cur and prev
            elif [[ "$curDate" != "$curGroupDate" && "$line" != *"AGP"* && "$line" != *"HRLL"* ]]; then
                printf "\e[4m\e[93mWarning\e[0m: " >&2
                printf "MISR date inconsistency found.\n"
                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                printf "\tThese two dates should be equal.\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous MISR file, then skip this step

            # Version mismatches should only be checked for the GRP files
            if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "GRP" ]]; then
                # if this isn't our first MISR file and there hasn't been a version mismatch
                # before, then
                if [[ ${#prevfilename} != 0 && $MISRVersionMismatch == 0 ]]; then
                    # if the previous file was an AGP file, then don't update the prevVersion
                    # variable
                    if [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" != "AGP" ]]; then
                        prevVersion=$(echo ${prevfilename##*_})
                        prevVersion=${prevVersion:0:4}
                    fi

                    curVersion=$(echo ${curfilename##*_})
                    curVersion=${curVersion:0:4}

                    if [[ "$prevVersion" != "$curVersion" ]]; then
                        printf "\e[4m\e[93mWarning\e[0m: " >&2
                        printf "MISR file version mismatch.\n" >&2
                        printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                        printf "\tWill not print any more MISR warnings of this type.\n" >&2
                        MISRVersionMismatch=1
                    fi
                fi
            fi


            prevDate="$curDate"
            prevCam="$curCam"
            prevfilename="$curfilename"

        else
            printf "\e[4m\e[91mFatal Error\e[0m:\n" >&2
            printf "\tUnable to determine which was the current instrument being read.\n" >&2
            printf "\tDebugging information follows:\n" >&2
            printf "\t\tline=$line\n" >&2
            printf "\t\tprevfilename=$prevfilename\n" >&2
            printf "\t\tcurfilename=$curfilename\n" >&2
            printf "\t\tinstrumentSection=$instrumentSection\n" >&2
            return 1
        fi

        let "linenum++"
    done <"$DEBUGFILE"

    # Check the number of MISR files. We should only have 12 files (1 granule)
    if [ $MISRNUM -ne 12 ]; then
        printf "Fatal Error: received an unexpected number of MISR files. Please check that exactly 12 files are present.\n" >&2
        return 1
    fi

    return 0
}

if [ "$#" -ne 3 ]; then
    echo "Usage: ./genFusionFiles [database file] [orbit number] [.txt output filepath]"
    exit 0
fi

DB=$1
UNORDERED="$3"unordered.txt
ORDERED="$3"

. ../queries.bash

rm -f "$ORDERED"
rm -f "$UNORDERED"


MOPLINES=$(instrumentOverlappingOrbit "$DB" $2 MOP)
CERLINES=$(instrumentOverlappingOrbit "$DB" $2 CER)
MODLINES=$(instrumentStartingInOrbit "$DB" $2 MOD)
ASTLINES=$(instrumentStartingInOrbit "$DB" $2 AST)
MISLINES=$(instrumentInOrbit "$DB" $2 MIS)

if [ ${#MOPLINES} -lt 2 ]; then
    echo "MOP N/A" >> "$UNORDERED"
else
    echo "$MOPLINES" >> "$UNORDERED"
fi

if [ ${#CERLINES} -eq 0 ]; then
    echo "CER N/A" >> "$UNORDERED"
else
    echo "$CERLINES" >> "$UNORDERED"
fi

if [ ${#MODLINES} -eq 0 ]; then
    echo "MOD N/A" >> "$UNORDERED"
else
    echo "$MODLINES" >> "$UNORDERED"
fi

if [ ${#ASTLINES} -eq 0 ]; then
    echo "AST N/A" >> "$UNORDERED"
else
    echo "$ASTLINES" >> "$UNORDERED"
fi

if [ ${#MISLINES} -eq 0 ]; then
    echo "MIS N/A" >> "$UNORDERED"
else
    echo "$MISLINES" >> "$UNORDERED"
fi


# Order and format the files appropriately (Fusion program requires specific input format)
orderFiles $UNORDERED $ORDERED
if [ "$?" -ne 0 ]; then
    printf "Failed to order files.\n" >&2
    printf "Exiting script.\n" >&2
    rm -f "$UNORDERED"
    exit 1
fi

# Perform verification
verifyFiles "$ORDERED"
if [ "$?" -ne 0 ]; then
    printf "Failed to verify files.\n" >&2
    printf "Exiting script.\n" >&2
    rm -f "$UNORDERED"
    exit 1
fi


# Add the orbit number to the top of the file
echo -e "$2\n$(cat $ORDERED)" > "$ORDERED"

rm -f "$UNORDERED"
