#!/bin/bash

# Args:
# 1 = unordered input file
# 2 = output file (properly ordered)
# 
# Description:
#   This function takes a single text file containing the paths of all the input HDF files for the basic fusion
#   program for one granule. It then orders the files properly so that the BF program does not crash due to
#   and unexpected ordering.
#

orderFiles() {

    MOPITTNUM=0
    CERESNUM=0
    MODISNUM=0
    ASTERNUM=0
    MISRNUM=0

    UNORDEREDFILE=$1
    OUTFILE=$2
    CURDIR=$(pwd)

    local MOPGREP
    local CERFM1GREP
    local CERFM2GREP
    local MOD1KMGREP
    local MODHKMGREP
    local MODQKMGREP
    local MOD03GREP
    local ASTGREP
    local MISGRPGREP
    local MISAGPGREP
    local MISGPGREP
    local MISHRLLGREP

    rm -f "$OUTFILE" 
 
    ##########
    # MOPITT #
    ##########
    # grep -v removes any "xml" files
    # put results of ls into a variable

    # The first "grep" variable for each instrument goes through some nasty piping. These are the steps that the grep line is
    # doing for MOPITT, and the same general process applies for all the instruments:
    # 1. Get the contents of the UNORDEREDFILE
    # 2. For MOPITT, grep the lines that have MOP.*he5 string or MOP N/A string.
    # 3. For MOPITT, remove all files that have "xml" string
    # 4. We want to sort only based on filename, not on the path. So awk finds the last field delimited by the forward slash
    #    and puts it to the front of the line. This copies the file name to the front, separated from the original file path
    #    by a single space (space being the default delimiter). For instance:
    #    "/path/to/filename.h5" becomes
    #    "filename.h5 /path/to/filename.h5"
    # 5. Sort those resulting lines, removing any files that are duplicates of each other
    # 6. After being sorted, remove the appended filename from the front of the line, leaving only the file path.
    #
    # This nasty trickery here is done so that the file path is not considered in the sorting, only the file name. 
    # We cannot guarantee that all relevant files are in the same directory!
    #
    # This solution was found here:
    # https://stackoverflow.com/questions/3222810/sorting-on-the-last-field-of-a-line

    MOPGREP=$(cat "$UNORDEREDFILE" | grep -e "MOP.*he5" -e 'MOP N/A' | grep -v "xml"  | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2- )
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
    CERFM1GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM1.*" -e 'CER N/A' | grep -v "met" | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2- )
    CERFM2GREP=$(cat "$UNORDEREDFILE" | grep -e "CER_SSF_Terra.*FM2.*" | grep -v "met" | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 |  cut -d " " -f 2-)

    if [[ ! "$CERFM1GREP" == *"CER N/A"* ]]; then
        while read -r line; do
            echo "$line" >> "$OUTFILE"
            # Find the corresponding FM2 file to add
            FM1date=${line##*.}
            FM2line=$(echo "$CERFM2GREP" | grep "$FM1date")
            if [[ ${#FM2line} != 0 ]]; then
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

    MOD1KMGREP=$(cat "$UNORDEREDFILE" | grep -e "MOD021KM.*hdf" -e 'MOD N/A' | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MODHKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02HKM.*hdf"  | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MODQKMGREP=$(cat "$UNORDEREDFILE" | grep "MOD02QKM.*hdf"  | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MOD03GREP=$(cat "$UNORDEREDFILE" | grep "MOD03.*hdf" | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)


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

    ASTGREP=$(cat "$UNORDEREDFILE" | grep -e "AST.*hdf" -e 'AST N/A' | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
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
    MISGRPGREP=$(cat $UNORDEREDFILE | grep -e "MISR_AM1_GRP_ELLIPSOID.*hdf" -e 'MIS N/A' | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MISAGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_AGP.*hdf" | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MISGPGREP=$(cat "$UNORDEREDFILE" | grep "MISR_AM1_GP.*hdf"  | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
    MISHRLLGREP=$(cat "$UNORDEREDFILE" | grep "MISR_HRLL_.*hdf"  | awk -F "/" '{ print $NF, $0}' | sort -u -t' ' -k1,1 | cut -d " " -f 2-)
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

fatalError(){
    printf "Fatal Error:\n" >&2
    printf "\tAt line number: $1\n"
    return 0
}

# Args:
# 1 = ordered input file
# 2 = orbit_info.bin file
#
# Description:
#   This function verifies the files after being passed through the orderFiles function for time continuity, that
#   the files are actually ordered properly, that all of the input granules have the necessary files, and that
#   there are no other errors that would cause the BF program to crash.
#
#   In addition to checking for fatal errors, it performs a series of checks to confirm that the input file will 
#   conform to the input granularity specifications as outlined on the GitHub page.
#

verifyFiles()
{
    local DEBUGFILE="$1"
    local orbitInfo="$2"
    local prevDate=""
    local prevRes=""
    local prevGroupDate=""
    local curGroupDate=""
    local instrumentSection=""
    local MODISVersionMismatch=0
    local ASTERVersionMismatch=0
    local MISRVersionMismatch=0
    local orbitNum=0
    local numRegExp='^[0-9]+$'

    # TODO
    # Finish the code below for adding orbit starting time to the file name

    # Get the orbit starting and ending info
    # First find the orbit number from the top of the file
    #orbitNum=$(head -n 1 $DEBUGFILE)
    #if [[ $line =~ $numRegExp ]]; then
    #    orbitNum=$line
    #    continue
    #else
    #    fatalError $linenum
    #    printf "\tThe first line in input file is not a valid orbit number.\n" >&2
    #    return 1
    #fi

    # Get the corresponding line in the orbit_info file
    #oInfoLine=$(grep "$orbitNum " $DEBUGFILE)
    #if [[ ${#oInfoLine} == 0 ]]; then
    #    printf "Fatal Error:\n" >&2
    #    printf "\tFailed to find the proper line in the orbit info file.\n" >&2
    #fi

    # Split the oInfoLine with a delimiter of the space character. This will split each section into an element of an array.
    #oInfoSplit=(${oInfoLine// / })
    # Now, oInfoSplit[0] has orbit num, [1] has beginning time, [2] has end time.
    # Make two more arrays that split the beginning and end times into their respective year/month/day/hour/minue/seconds.
    #startTime[0]=${oInfoSplit[1]

    # Read through the DEBUGFILE file.
    # Remember, DEBUGFILE is the variable our script will do the debugging on. It may
    # or may not be set to the OUTFILE variable.
    linenum=0
    
    while read -r line; do
        
        let "linenum++"
        
        # Grab the orbit number. Use a regular expression to determine if the first line is a number
        if [[ $linenum == 1 ]]; then
            continue
        fi
        
        # Check to make sure that each line is either a comment, a newline or a path.
        # If it is none of them, throw an error. This line would cause the C program
        # downstream to crash because if the line isn't a comment or newline,
        # it will interpret it as a file path.

        # continue if we have a non-valid line (comment or otherwise)
        if [[ ${#line} == 0 || ${line:0:1} == "#" || "$line" == "MOP N/A" || "$line" == "CER N/A" || "$line" == "MIS N/A" || "$line" == "MOD N/A" || "$line" == "AST N/A" ]]; then
            continue
        fi

        if [[ ${line:0:1} != "." && ${line:0:1} != "/" && ${line:0:1} != ".." && ${line:0:1} != "#" && ${line:0:1} != "\n" && ${line:0:1} != "\0" && ${#line} != 0 ]]; then
            printf "Fatal Error:\n" >&2
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
                continue
            fi

            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=${curfilename:6:8}
            if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
                printf "Warning: " >&2
                printf "MOPITT files found to be out of order.\n" >&2
                printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
                printf "\tLine: $linenum\n" >&2
                printf "\tFile: $line\n" >&2
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
                        printf "Warning: " >&2
                        printf "CERES files found to be out of order.\n" >&2
                        printf "\tDate of previous FM1/FM2 pair was $prevGroupDate.\n" >&2
                        printf "\tDate of current FM1/FM2 pair is $curDate.\n" >&2
                        printf "\tThe dates between pairs should be strictly increasing down the file list.\n" >&2
                        printf "\tFile: $line\n" >&2
                    fi
                fi
                prevGroupDate="$curDate"
            fi
            if [[ "$prevCam" == "$curCam" ]]; then
                printf "Warning: " >&2
                printf "CERES FM1 followed FM1 or FM2 followed FM2.\n" >&2
                printf "\tAn FM1 file should always be paired with an FM2 file!\n" >&2
                printf "\tFound at line $linenum\n" >&2
                printf "\tFile: $line\n" >&2
            elif [[ "$prevCam" != "$curCam" && "$curCam" == "FM2" ]]; then
                if [[ "$curDate" != "$prevDate" ]]; then
                    printf "Warning: " >&2
                    printf "FM1/FM2 pair don't have the same date.\n" >&2
                    printf "\tIf an FM1 file is followed by FM2, they must have the same date.\n" >&2
                    printf "\tprevDate=$prevDate curDate=$curDate" >&2
                    printf "\tLine $linenum\n" >&2
                    printf "\tFile: $line" >&2
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
                continue
            fi

            # CHECK FOR RESOLUTION CONSISTENCY

            tmp=${curfilename#*.A}
            curDate=$(echo "$tmp" | cut -f1,2 -d'.')
            curRes=$(echo ${curfilename%%.*})

            if [[ "$prevRes" == "MOD021KM" ]]; then
                if [[ "$curRes" != "MOD03" && "$curRes" != "MOD02HKM" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD03 or MOD02HKM after MOD021KM.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD02HKM" ]]; then
                if [[ "$curRes" != "MOD02QKM" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD02QKM after MOD02HKM.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD02QKM" ]]; then
                if [[ "$curRes" != "MOD03" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD03 after MOD02QKM.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevRes" == "MOD03" ]]; then
                if [[ "$curRes" != "MOD021KM" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MODIS resolutions are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Resolution: $prevRes)\n\tcame before\n\t\"$curfilename\" (Resolution: $curRes)\n" >&2
                    printf "\tExpected to see MOD021KM after MOD03.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            else
                printf "Fatal Error: " >&2
                printf "Unknown error at line $linenum.\n" >&2
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
                        printf "Warning: " >&2
                        printf "MODIS date inconsistency found.\n"
                        printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                        printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                        printf "\tDates should be strictly increasing between groups!\n" >&2
                        printf "\tLine: $linenum\n" >&2
                        printf "\tFile: $line\n" >&2
                    fi
                fi
            fi

            # check that the current line's date is the same as the rest its group. Each file in a group should
            # have the same date.
            if [[ "$curDate" != "$curGroupDate" ]]; then
                printf "Warning: " >&2
                printf "MODIS date inconsistency found.\n"
                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                printf "\tThese two dates should be equal.\n" >&2
                printf "\tLine: $linenum\n" >&2
                printf "\tFile: $line\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous MODIS file, then skip this step
            
            if [[ ${#prevfilename} != 0 && $MODISVersionMismatch == 0 ]]; then
                prevVersion=$(echo "$prevfilename" | cut -f4,4 -d'.')
                curVersion=$(echo "$curfilename" | cut -f4,4 -d'.')
                
                if [[ "$prevVersion" != "$curVersion" ]]; then
                    printf "Warning: " >&2
                    printf "MODIS file version mismatch.\n" >&2
                    printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                    printf "\tWill not print any more MODIS warnings of this type.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
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
                continue
            fi

            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=$(echo "$curfilename" | cut -f3,3 -d'_')
            curVersion=${curDate:0:3}
            curDate="${curDate:3}"
            if [[ "$curDate" < "$prevDate" || "$curDate" == "$prevDate" ]]; then
                printf "Warning: " >&2
                printf "ASTER files found to be out of order.\n" >&2
                printf "\t\"$prevfilename\" (Date: $prevDate)\n\tcame before\n\t\"$curfilename\" (Date: $curDate)\n\tDates should be strictly increasing downward.\n" >&2
                printf "\tLine: $linenum\n" >&2
                printf "\tFile: $line\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous ASTER file, then skip this step

            if [[ ${#prevfilename} != 0 && $ASTERVersionMismatch == 0 ]]; then

                if [[ "$prevVersion" != "$curVersion" ]]; then
                    printf "Warning: " >&2
                    printf "ASTER file version mismatch.\n" >&2
                    printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                    printf "\tWill not print any more ASTER warnings of this type.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
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
            # note: "cam" refers to which camera, ie "AA", "AF", "AN" etc.
            if [ -z "$prevDate" ]; then
                prevfilename=$(echo ${line##*/})
                # extract the date
                prevDate=$(echo "$prevfilename" | cut -f6,7 -d'_')
                prevCam=$(echo "$prevfilename" | cut -f8,8 -d'_' )
                curGroupDate="$prevDate"
                continue
            fi

            # CHECK FOR CAMEREA CONSISTENCY

            curDate=$(echo "$curfilename" | cut -f6,7 -d'_')
            curCam=$(echo "$curfilename" | cut -f8,8 -d'_' )

            if [[ "$prevCam" == "AA" ]]; then
                if [[ "$curCam" != "AF" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AF after AA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "AF" ]]; then
                if [[ "$curCam" != "AN" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AN after AF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "AN" ]]; then
                if [[ "$curCam" != "BA" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BA after AN.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi

            elif [[ "$prevCam" == "BA" ]]; then
                if [[ "$curCam" != "BF" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BF after BA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "BF" ]]; then
                if [[ "$curCam" != "CA" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CA after BF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "CA" ]]; then
                if [[ "$curCam" != "CF" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CF after CA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "CF" ]]; then
                if [[ "$curCam" != "DA" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DA after CF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "DA" ]]; then
                if [[ "$curCam" != "DF" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DF after DA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$prevCam" == "DF" ]]; then
                if [[ "$(echo "$curfilename" | cut -f7,3 -d'_')" != "AGP" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\"\n" >&2
                    printf "\tExpected to see an AGP file after a DF camera.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" != "GP" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see GP after AGP file.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f2,2 -d'_')" != "HRLL" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see HRLL file after GP file.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            elif [[ "$(echo "$prevfilename" | cut -f2,2 -d'_')" == "HRLL" ]]; then
                if [[ "$curCam" != "AA" ]]; then
                    printf "Fatal Error: " >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see AA file after HRLL file.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                fi
            else
                printf "Fatal Error: " >&2
                printf "Unknown error at line $linenum.\n" >&2
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
                        printf "Warning: " >&2
                        printf "MISR date inconsistency found.\n"
                        printf "\tThe group proceeding \"$curfilename\" has a date of: $curGroupDate.\n" >&2
                        printf "\tThe previous group had a date of: $prevGroupDate.\n" >&2
                        printf "\tDates should be strictly increasing between groups!\n" >&2
                        printf "\tLine: $linenum\n" >&2
                        printf "\tFile: $line\n" >&2
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
                    printf "Warning: " >&2
                    printf "MISR file inconsistency found.\n"
                    printf "\t$curfilename has a path number: $curpath != $prevpath\n" >&2
                    printf "\tThese two path numbers should be equal.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                fi
            elif [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
                curDate=$(echo "$curfilename" | cut -f5,6 -d'_')
                if [[ "$curDate" != "$curGroupDate" ]]; then
                    printf "Warning: " >&2
                    printf "MISR date inconsistency found.\n"
                    printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                    printf "\tThese two dates should be equal.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                fi
            # else we need to check the dates between cur and prev
            elif [[ "$curDate" != "$curGroupDate" && "$line" != *"AGP"* && "$line" != *"HRLL"* ]]; then
                printf "Warning: " >&2
                printf "MISR date inconsistency found.\n"
                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
                printf "\tThese two dates should be equal.\n" >&2
                printf "\tLine: $linenum\n" >&2
                printf "\tFile: $line\n" >&2
            fi

            # CHECK FOR VERSION CONSISTENCY
            # If there hasn't been a previous MISR file, then skip this step

            # If the current file is not a GRP file, skip it.
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

                    if [[ "$prevVersion" != "$curVersion" && $MISRVersionMismatch == 0 ]]; then
                        printf "Warning: " >&2
                        printf "MISR file version mismatch.\n" >&2
                        printf "\t$prevVersion and $curVersion versions were found at line $linenum\n" >&2
                        printf "\tWill not print any more MISR warnings of this type.\n" >&2
                        printf "\tLine: $linenum\n" >&2
                        printf "\tFile: $line\n" >&2
                        MISRVersionMismatch=1
                    fi
                fi
            fi


            prevDate="$curDate"
            prevCam="$curCam"
            prevfilename="$curfilename"

        else
            printf "Fatal Error:\n" >&2
            printf "\tUnable to determine which was the current instrument being read.\n" >&2
            printf "\tDebugging information follows:\n" >&2
            printf "\t\tline=$line\n" >&2
            printf "\t\tprevfilename=$prevfilename\n" >&2
            printf "\t\tcurfilename=$curfilename\n" >&2
            printf "\t\tinstrumentSection=$instrumentSection\n" >&2
            return 1
        fi

    done <"$DEBUGFILE"

    return 0
}

#
#
# Description:
#   This function takes an input file list after being passed through orderFiles, and then through verifyFiles. The main
#   difference between this function and verifyFiles is that this function ensures that the input file conforms to the
#   specifications of the BF granularity being one Terra orbit. This includes checks that ensure:
#       1. The file actually belongs to the orbit
#       2. That any files that should be spanning two separate orbits properly make it into both input file lists
#       3. That there are no duplicate files
#       4. If there are any time discontinuities, check to make sure that the file does not exist in the file system.
#
#   To reiterate, the verifyFiles ensures that no program crashes will happen due to a bad input file. verifyOneOrbit
#   verifies that the file conforms to specifications.
#

#verifyOneOrbit(){

#}

if [ "$#" -ne 3 ]; then
    echo "Usage: ./genFusionFiles [database file] [orbit number] [.txt output filepath]"
    exit 0
fi

DB=$1
UNORDERED="$3".unorderedDB
ORDERED="$3"
SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

mkdir -p $(dirname $ORDERED)/unorderedDatabaseReturn
UNORDERED=$(dirname $ORDERED)/unorderedDatabaseReturn/$(basename $ORDERED).unordered

source "$SCRIPT_PATH/../queries.bash"

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

if [ ${#CERLINES} -lt 2 ]; then
    echo "CER N/A" >> "$UNORDERED"
else
    echo "$CERLINES" >> "$UNORDERED"
fi

if [ ${#MODLINES} -lt 2 ]; then
    echo "MOD N/A" >> "$UNORDERED"
else
    echo "$MODLINES" >> "$UNORDERED"
fi

if [ ${#ASTLINES} -lt 2 ]; then
    echo "AST N/A" >> "$UNORDERED"
else
    echo "$ASTLINES" >> "$UNORDERED"
fi

# Get the number of GRP files
numGRP=$(echo "$MISLINES" | grep "GRP" | wc -l)
if [ $numGRP -eq 0 ]; then
    echo "MIS N/A" >> "$UNORDERED"

else [ $numGRP -eq 9 ]
    echo "$MISLINES" >> "$UNORDERED"
fi

# Order and format the files appropriately (Fusion program requires specific input format)
orderFiles $UNORDERED $ORDERED
if [ "$?" -ne 0 ]; then
    printf "Failed to order files.\n" >&2
    printf "Exiting script.\n" >&2
    exit 1
fi


# Add the orbit number to the top of the file
echo -e "$2\n$(cat $ORDERED)" > "$ORDERED"

# Perform verification (ensures BF program won't crash due to bad input)
verifyFiles "$ORDERED"
if [ "$?" -ne 0 ]; then
    printf "Failed to verify files.\n" >&2
    printf "Exiting script.\n" >&2
    exit 1
fi

exit 0
