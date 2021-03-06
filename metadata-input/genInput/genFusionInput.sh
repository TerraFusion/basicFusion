#!/bin/bash
# @author: Landon Clipp
# @email: clipp2@illinois.edu
#
# My feelings won't be hurt if you want to rewrite this script in Python. In fact, it would be preferable.
# The horrific nature of this monstrosity isn't lost on me.


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

    local MOPITTNUM=0
    local CERESNUM=0
    local MODISNUM=0
    local ASTERNUM=0
    local MISRNUM=0

    local UNORDEREDFILE=$1
    local OUTFILE=$2
    local CURDIR=$(pwd)

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
    
    # Explanation of the CERGREP line, where each number is one of the instructions done in the $(...):
    #   1. Print all lines in the the UNORDEREDFILE with the string "CER_SSF_Terra"
    #   2. Find the camera number, "FM1" or "FM2" and print it to the beginning of the line such that:
    #      "[directory]/CER_SSF_Terra-FM2-MODIS_Edition4A_400403.2001112804"
    #      becomes:
    #      "Terra-FM2- [directory]/CER_SSF_Terra-FM2-MODIS_Edition4A_400403.2001112804"
    #   3. Take the date given in the filename and print it at the beginning of each line, such that:
    #       "Terra-FM2- [directory]/CER_SSF_Terra-FM2-MODIS_Edition4A_400403.2001112804"
    #      Becomes::
    #      "2001112804 Terra-FM2- [directory]/CER_SSF_Terra-FM2-MODIS_Edition4A_400403.2001112804"
    #   4. Perform an alphabetic sort on all the lines, such that all dates are grouped together, and an FM1
    #      camera for a particular date comes before the FM2 camera for that particular date.
    #   5. Remove the extra "2001112804 Terra-FM2- " from each line. They are no longer needed for sorting purposes.

    CERGREP="$( grep "CER_SSF_Terra" "$UNORDEREDFILE" | awk '{match($0,/Terra-FM[1|2]-/)} {print substr($0, RSTART, RLENGTH),$0}' | awk -F. '{print $NF,$0}' | sort | cut -d' ' -f3)"
    CER_NA="$(grep 'CER N/A' "$UNORDEREDFILE")"
    
    # If there is any "CER N/A" line in the file, don't print any of the CERES lines.
    if [ "$CER_NA" != "CER N/A" ]; then
        echo "$CERGREP" >> "$OUTFILE"
    else
        echo "CER N/A" >> "$OUTFILE"
    fi

    #########
    # MODIS #
    #########
    # For MODIS, we will split the 1KM, HKM, QKM and MOD03 filenames
    # into separate text files to make it easy to handle


    # Explanation of the MODGREP line, where each number is one of the instructions done in the $(...):
    #   1. Print all lines in the the UNORDEREDFILE with the string "/MODIS/". Note that the directory path
    #      in this case should match "/MODIS/", not the file name. Strangely, CERES files have the string "MODIS"
    #      in them as well, so matching the MODIS file itself is dangerous.
    #   2. Do a REGEX search for "/MOD021KM", "/MOD02HKM", "/MOD02QKM", or "/MOD03" and print the matched string to
    #      the beginning of the line such that:
    #      [directpry]/MOD021KM.A2001332.0415.006.2014229230016.hdf
    #      becomes
    #      /MOD021KM [directory]/MOD021KM.A2001332.0415.006.2014229230016.hdf 
    #   3. Take the date given in the filename and print it at the beginning of each line, such that:
    #      /MOD021KM [directory]/MOD021KM.A2001332.0415.006.2014229230016.hdf
    #      Becomes:
    #      A2001332.0415 /MOD021KM [directory]/MOD021KM.A2001332.0415.006.2014229230016.hdf
    #   4. Perform an alphabetic sort on all the lines, such that all dates are grouped together, with sort priority
    #      for each file type given by: MOD021KM, MOD02HKM, MOD02QKM, MOD03
    #   5. Remove the extra "A2001332.0415 /MOD021KM " from each line. They are no longer needed for sorting purposes.

    MODGREP="$(grep "MOD0[2HKM|2QKM|21KM|3]" "$UNORDEREDFILE" | \
awk '{match($0,/\/MOD0(21KM|2HKM|2QKM|3)./)} {print substr($0, RSTART, RLENGTH),$0}' | \
awk --posix '{match($0,/A[0-9]{7}.[0-9]{4}/)} {print substr($0, RSTART, RLENGTH),$0}' | \
sort | \
cut -d' ' -f3)"

    MOD_NA="$(grep "MOD N/A" "$UNORDEREDFILE")"

    if [ "$MOD_NA" != "MOD N/A" ]; then
        echo "$MODGREP" >> "$OUTFILE"
    else
        echo "MOD N/A" >> "$OUTFILE"
    fi

    #########
    # ASTER #
    #########

    ASTGREP=$(cat "$UNORDEREDFILE" | \
grep -e "AST.*hdf" -e 'AST N/A' | \
awk -F "/" '{ print $NF, $0}' | \
sort -u -t' ' -k1,1 | \
cut -d " " -f 2-)
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
    MISGREP="$(grep "/MISR_" "$UNORDEREDFILE" | \
awk '{match($0,/(_(AA|AF|AN|BA|BF|CA|CF|DA|DF)_)/)} {if ( RLENGTH > 0) print substr($0, RSTART, RLENGTH),$0; else print $0}' | \
awk '{match( $0, /_AGP_/)} {if( RLENGTH > 0) print "xxa",$0 ; else print $0}' | \
awk '{match( $0, /_GMP_/)} {if( RLENGTH > 0) print "xxb",$0 ; else print $0}' | \
awk '{match( $0, /_HRLL_/)} {if( RLENGTH > 0) print "xxc",$0 ; else print $0}' | \
sort | \
cut -d' ' -f2)"
    
    MIS_NA="$(grep "MIS N/A" "$UNORDEREDFILE" )"

    if [ "$MIS_NA" != "MIS N/A" ]; then
        echo "$MISGREP" >> "$OUTFILE"
        # Add the missing MISR file labels in the outfile
        python "$SCRIPT_PATH"/genFusionInput.py "$OUTFILE"
    else
        echo "MIS N/A" >> "$OUTFILE"
    fi


    return 0
}

fatalError(){
    printf "Fatal Error:\n" >&2
    printf "\tAt line number: $1\n"
    return 0
}

# belongsToOrbit()
#
# DESCRIPTION:
#   This function determines whether or not the file time passed to it belongs to the current orbit. The logic for
#   determining whether it belongs to the orbit given the time in the filename is described below. This logic is
#   what is used throughout all of the basic fusion project and is documented on the GitHub wiki.
#
# ARGUMENTS:
#   1: Orbit start time             -- In format YYYYMMDDHHmmSS
#   2: Orbit end time               -- In format YYYYMMDDHHmmSS
#   3: The time given in file name  -- Each Terra file gives some sort of time indication in the filename itself.
#                                      MOP: YYYYMMDD
#                                      CER: YYYYMMDDHH
#                                      MOD: YYYYjjj.HHmm where jjj is the day of the year
#                                      AST: DDMMYYYYHHmmSS
#                                      MIS: Orbit number
#   4: Instrument                   -- Can be the string "MOP", "MIS", "MOD", "AST", or "CER"
#   5: Orbit number                 -- The orbit number that the files will be checked against
#
# EFFECTS:
#   None
#
# RETURN:
#   Returns 0 if file belongs to orbit $5
#   Returns 1 if it does not.

belongsToOrbit(){
    declare Orbit_sTime="$1"
    declare Orbit_eTime="$2"
    declare fileDate="$3"
    declare instr="$4"
    declare orbitNum="$5"

    declare -a MOPdate
    declare -a CERdate
    declare -a MISdate
    declare -a ASTdate
    declare -a MODdate
    declare fileEnd

    # Pad the orbitNum with leading 0's so that it is always the same width
    orbitNum=$(printf "%06d\n" $orbitNum)

    # I've been having problems with the Orbit_eTime containing a ^M character. Removing this now.
    Orbit_eTime=$(tr -dc '[[:print:]]' <<< "$Orbit_eTime")

    if [ "$instr" == "MOP" ]; then
        # The rule for MOPITT is that if any portion of the granule overlaps the orbit, then include it.
        # MOPITT files provide time information up to the day. We need to expand fileDate so that it includes
        # up to the second. We safely assume that MOPITT files start at time 00:00:00 where HH:MM:SS.
        fileDate="${fileDate}000000"

        # MOPITT end is 24 hours, or 1 day from fileDate.
        # The 10# is to force the arithmetic to interpret it as a base 10 number. Leading 0's in the number
        # will cause it to interpret the number as octal.
        fileEnd=$(date --utc -d "${fileDate:0:4}-${fileDate:4:2}-${fileDate:6:2} ${fileDate:8:2} hours ${fileDate:10:2} minutes ${fileDate:12:2} seconds + 1 day" +"%Y%m%d%0k%M%S")

        if [ $fileDate -le $Orbit_sTime  -a  $fileEnd -ge $Orbit_eTime ] || \
           [ $fileDate -ge $Orbit_sTime  -a  $fileDate -le $Orbit_eTime ] || \
           [ $fileEnd -ge $Orbit_sTime   -a $fileEnd -le $Orbit_eTime ]; then
            return 0
        else
            printf "Warning:\n" >&2
            printf "\tThis file does not belong to the orbit!\n" >&2
            return 1
        fi

    elif [ "$instr" == "CER" ]; then
        # The rule for CERES is the same for MOPITT.
        # CERES SSF filenames provide a file time up to the hour. So we need to expand fileDate so that it 
        # includes up to the second. We safely assume that CERES files start exactly on the hour.
        fileDate="${fileDate}0000"
        
        # CERES SSF granularity is one hour.
        fileEnd=$(date --utc -d "${fileDate:0:4}-${fileDate:4:2}-${fileDate:6:2} ${fileDate:8:2} hours ${fileDate:10:2} minutes ${fileDate:12:2} seconds + 1 hour" +"%Y%m%d%0k%M%S")

        if [ $fileDate -le $Orbit_sTime -a $fileEnd -ge $Orbit_eTime ] || \
           [ $fileDate -ge $Orbit_sTime -a $fileDate -le $Orbit_eTime ] || \
           [ $fileEnd -ge $Orbit_sTime -a $fileEnd -le $Orbit_eTime ]; then
            return 0
        else
            printf "Warning:\n" >&2
            printf "\tThis file does not belong to the orbit!\n" >&2
            return 1
        fi
        
    elif [ "$instr" == "MIS" ]; then 
        # MISR files have a granularity of 1 orbit. Thus, the check to see if it belongs in the orbit is trivial,
        # just see if the file's orbit number matches the current orbit.
        if [[ $fileDate == $orbitNum ]]; then
            return 0
        else
            printf "Warning:\n" >&2
            printf "\tThis file does not belong to the orbit!\n" >&2
            return 1
        fi
    elif [ "$instr" == "AST" ]; then
        # ASTER filename times provide a resolution up to the second.
        # Convert the ASTER date into our common format
        fileDate=$(date --utc -d "${fileDate:4:4}-${fileDate:0:2}-${fileDate:2:2} ${fileDate:8:2}:${fileDate:10:2}:${fileDate:12:2}" +"%Y%m%d%0k%M%S")
    
        if [[ $fileDate -ge $Orbit_sTime && $fileDate -le $Orbit_eTime  ]]; then
            return 0
        else
            printf "Warning:\n" >&2
            printf "\tThis file does not belong to the orbit!\n" >&2
            return 1
        fi

    elif [ "$instr" == "MOD" ]; then
        # Rule for MODIS is that if the file starts inside the orbit, then include it. File end time is not considered.
        # MODIS granularity is 5 minutes. Unfortunately, MODIS files provide the date in YYYYDDD.MMMM where DDD is the
        # running total day count (max value is 365 or 366 depending on if it is a leap year), and MMMM is the running
        # total minute counter of the day. So we have to do some really nasty shit to make this work...
        #
        # Remove the period
        fileDate="${fileDate//./}"
   
        # Need to subtract 1 from the day because the fileDate was always 1 day ahead of what it was supposed to be (it is 1-indexed)
        MODdays=$(( 10#${fileDate:4:3} - 1 ))
        # Get the date into a common format
        fileDate=$(date --utc -d "${fileDate:0:4}-01-01 + $MODdays days ${fileDate:7:2} hours ${fileDate:9:2} minutes" +"%Y%m%d%0k%M%S")
        if [[ ( $fileDate -ge $Orbit_sTime ) && ( $fileDate -le $Orbit_eTime ) ]]; then
            return 0
        else
            printf "Warning:\n" >&2
            printf "\tThis file does not belong to the orbit!\n" >&2
            return 1
        fi
    else
        printf "Fatal Error:\n" >&2
        printf "\tThe instrument argument to function belongsToOrbit() should be either:\n" >&2
        printf "\tMOP, CER, MIS, AST or MOD.\n" >&2
        return 1
    fi
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
    local grpMiss="MISR_AM1_GRP_MISS"
    local gpMiss="MISR_AM1_GP_MISS"
    local hrllMiss="# MISR_AM1_HRLL_MISS"
    local agpMiss="# MISR_AM1_AGP_MISS"

    ##################################################
    #           GATHER DATE INFO OF ORBIT            #
    ##################################################

    local linenum
    linenum=0

    # Get the orbit starting and ending info
    # First find the orbit number from the top of the file
    local orbitNum=$(head -n 1 $DEBUGFILE)
    if ! [[ $orbitNum =~ $numRegExp ]]; then
        fatalError 1
        printf "\tThe first line in input file is not a valid orbit number.\n" >&2
        echo "$orbitNum"
        return 1
    fi

    # Get the corresponding line in the orbit_info file
    local oInfoLine=$(grep "$orbitNum " $orbitInfo)
    if [[ ${#oInfoLine} == 0 ]]; then
        printf "Fatal Error:\n" >&2
        printf "\tFailed to find the proper line in the orbit info file.\n" >&2
        return 1
    fi

    # Split the oInfoLine with a delimiter of the space character. This will split each section into an element of an array.
    local oInfoSplit=(${oInfoLine// / })
    # Now, oInfoSplit[0] has orbit num, [2] has beginning time, [3] has end time.
    # Make two more arrays that split the beginning and end times into their respective year/month/day/hour/minue/seconds.
    local startTime
    # Remove all the unnecessary characters from oInfoSplit[2] so that only numbers remain
    startTime="${oInfoSplit[2]//-/}"
    startTime="${startTime//T/}"
    startTime="${startTime//:/}"
    startTime="${startTime//Z/}"

    local endTime
    # Remove all the unnecessary characters from oInfoSplit[3] so that only numbers remain
    endTime="${oInfoSplit[3]//-/}"
    endTime="${endTime//T/}"
    endTime="${endTime//:/}"
    endTime="${endTime//Z/}"

    #########################################
    #           BEGIN FILE CHECKING         #
    #########################################

    # Read through the DEBUGFILE file.
    # Remember, DEBUGFILE is the variable our script will do the debugging on. It may
    # or may not be set to the OUTFILE variable.
    
    while read -r line; do
        
        let "linenum++"
       
        # First line is always the orbit number 
        if [[ $linenum == 1 ]]; then
            continue
        fi
        
        # Check to make sure that each line is either a comment, a newline or a path.
        # If it is none of them, throw an error. This line would cause the C program
        # downstream to crash because if the line isn't a comment or newline,
        # it will interpret it as a file path.

        # continue if we have a non-valid line (comment or otherwise)
        #if [[ ${#line} == 0 || ${line:0:1} == "#" || "$line" == "MOP N/A" || "$line" == "CER N/A" || "$line" == "MIS N/A" || \
        #   "$line" == "MOD N/A" || "$line" == "AST N/A" || "$line" == "$grpMiss" || "$line" == "$gpMiss" ]]; then
        #    continue
        #fi

        if [[ ${#line} == 0 || ${line:0:1} == "#" || "$line" == "MOP N/A" || "$line" == "CER N/A" || "$line" == "MIS N/A" || \
           "$line" == "MOD N/A" || "$line" == "AST N/A"  ]]; then
            continue
        fi
        if [[ ${line:0:1} != "." && ${line:0:1} != "/" && ${line:0:1} != ".." && ${line:0:1} != "#" && ${line:0:1} != "\n" && \
            ${line:0:1} != "\0" && ${#line} != 0 && "$line" != "$grpMiss" && "$line" != "$gpMiss" ]]; then
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

            # VERIFY THAT MOPITT BELONGS TO ORBIT
            belongsToOrbit "$startTime" "$endTime" "$curDate" MOP $orbitNum
            retVal=$?
            if [ $retVal -ne 0 ]; then
                printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
            fi

            prevDate="$curDate"
            prevfilename="$curfilename"

            
                          #########
                          # CERES #
                          #########

        elif [ "$instrumentSection" == "CER" ]; then
            
            # we now have the previous date and the current line. Now we need to find the
            # current date from the variable "curfilename" and compare that with the previous date
            curDate=$(echo $curfilename | rev | cut -d'_' -f1 | rev)
            curDate=$(echo $curDate | cut -d'.' -f2)
            prevCam=${prevfilename:14:3}
            curCam=${curfilename:14:3}

            # Perform a check that there is the corrsponding camera in the list
            oppositeCamera=$(grep "$curDate" "$DEBUGFILE" | grep -v "$curCam")
            if [ ${#oppositeCamera} -le 2 ]; then
                if [ "$curCam" == "FM1" ]; then
                    oppositeCamera="FM2"        # Overload the oppositeCamera variable
                else
                    oppositeCamera="FM1"
                fi

                printf "Warning: " >&2
                printf "Failed to find a corresponding CERES file.\n" >&2
                printf "\tFile $curfilename was found but its corresponding $oppositeCamera was not found!\n" >&2

            fi

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

            prevDate="$curDate"
            prevfilename="$curfilename"

            # VERIFY THAT CERES BELONGS TO ORBIT
            belongsToOrbit "$startTime" "$endTime" "$curDate" CER $orbitNum
            retVal=$?
            if [ $retVal -ne 0 ]; then
                printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
            fi

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

            # VERIFY THAT MODIS BELONGS TO ORBIT
            belongsToOrbit "$startTime" "$endTime" "$curDate" MOD $orbitNum
            retVal=$?
            if [ $retVal -ne 0 ]; then
                printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
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

            # VERIFY THAT ASTER BELONGS TO ORBIT
            belongsToOrbit "$startTime" "$endTime" "$curDate" AST $orbitNum
            retVal=$?
            if [ $retVal -ne 0 ]; then
                printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
            fi


            prevDate="$curDate"
            prevfilename="$curfilename"
            prevVersion="$curVersion"

           
                ########
                # MISR #
                ######## 

        elif [ "$instrumentSection" == "MIS" ]; then
            # note: "cam" refers to which camera, ie "AA", "AF", "AN" etc.
            
            # CHECK FOR CAMEREA CONSISTENCY

            curDate=$(echo "$curfilename" | cut -f7 -d'_')
            # Get rid of the "O" at the front
            curDate=${curDate//O/}
            curCam=$(echo "$curfilename" | cut -f8 -d'_' )

            if [[ "$curCam" == "AA" ]]; then
                # VERIFY THAT MISR BELONGS TO ORBIT
                belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                retVal=$?
                if [ $retVal -ne 0 ]; then
                    printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                fi
                
                prevDate="$curDate"
                prevCam="$curCam"
                prevfilename="$curfilename"
                curGroupDate="$curDate"
                # There are no further checks that can be done on the AA file, so skip to the next line
                continue    
                
            elif [[ "$prevCam" == "AA" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="AF"
                    continue
                fi
                if [[ "$curCam" != "AF" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AF after AA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "AF" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="AN"
                    continue
                fi
                if [[ "$curCam" != "AN" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see AN after AF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then  
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "AN" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="BA"
                    continue
                fi
                if [[ "$curCam" != "BA" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BA after AN.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "BA" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="BF"
                    continue
                fi
                if [[ "$curCam" != "BF" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see BF after BA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                    return 1
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "BF" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="CA"
                    continue
                fi
                if [[ "$curCam" != "CA" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CA after BF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "CA" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="CF"
                    continue
                fi
                if [[ "$curCam" != "CF" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see CF after CA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "CF" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="DA"
                    continue
                fi
                if [[ "$curCam" != "DA" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DA after CF.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    # VERIFY THAT MISR BELONGS TO ORBIT
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "DA" ]]; then
                if [ "$line" == "$grpMiss" ]; then
                    prevCam="DF"
                    continue
                fi
                if [[ "$curCam" != "DF" && "$line" != "$grpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\" (Camera: $curCam)\n" >&2
                    printf "\tExpected to see DF after DA.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                elif [ "$line" != "$grpMiss" ]; then
                    belongsToOrbit "$startTime" "$endTime" "$curDate" MIS $orbitNum
                    retVal=$?
                    if [ $retVal -ne 0 ]; then
                        printf "\tThe file\n\t$line\n\tdoes not belong to orbit $orbitNum.\n" >&2
                    fi
                fi
            elif [[ "$prevCam" == "DF" ]]; then
                if [[ "$(echo "$curfilename" | cut -f7,3 -d'_')" != "AGP" && "$line" != "$agpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\" (Camera: $prevCam)\n\tcame before\n\t\"$curfilename\"\n" >&2
                    printf "\tExpected to see an AGP file after a DF camera.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "AGP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" != "GP" && "$line" != "$gpMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see GP after AGP file.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                fi
            elif [[ "$(echo "$prevfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
                if [[ "$(echo "$curfilename" | cut -f2,2 -d'_')" != "HRLL" && "$line" != "$hrllMiss" ]]; then
                    printf "Warning: \n" >&2
                    printf "MISR files are out of order.\n" >&2
                    printf "\t\"$prevfilename\"\n\tcame before\n\t\"$curfilename\".\n" >&2
                    printf "\tExpected to see HRLL file after GP file.\n" >&2
                    printf "\tLine: $linenum\n" >&2
                    printf "\tFile: $line\n" >&2
                fi
            elif [[ "$(echo "$prevfilename" | cut -f2,2 -d'_')" == "HRLL" ]]; then
                printf "Fatal Error: " >&2
                printf "Two MISR granules detected.\n" >&2
                printf "\tOnly one MISR granule should ever be present!\n" >&2
                return 1
            else
                printf "Fatal Error: " >&2
                printf "Unknown error at line $linenum.\n" >&2
                rm -r "$CURDIR"/__tempFiles
                return 1
            fi

            # CHECK FOR DATE CONSISTENCY
            
           

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
# LTC Jan 31, 2018: Comment out the following warnings for now. Throws warnings if GP_MISS was found,
# but this causes validation of log files to raise false alarms.
#            elif [[ "$(echo "$curfilename" | cut -f3,3 -d'_')" == "GP" ]]; then
#                curDate=$(echo "$curfilename" | cut -f6 -d'_' | cut -f2 -d'O')
#                if [[ "$curDate" != "$curGroupDate" ]]; then
#                    printf "Warning: " >&2
#                    printf "MISR date inconsistency found.\n"
#                    printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
#                    printf "\tThese two dates should be equal.\n" >&2
#                    printf "\tLine: $linenum\n" >&2
#                    printf "\tFile: $line\n" >&2
#                fi
#            # else we need to check the dates between cur and prev
#            elif [[ "$curDate" != "$curGroupDate" && "$line" != *"AGP"* && "$line" != *"HRLL"* ]]; then
#                printf "Warning: " >&2
#                printf "MISR date inconsistency found.\n"
#                printf "\t$curfilename has a date: $curDate != $curGroupDate\n" >&2
#                printf "\tThese two dates should be equal.\n" >&2
#                printf "\tLine: $linenum\n" >&2
#                printf "\tFile: $line\n" >&2
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

#!/bin/bash
usage(){

    description="Usage: $0 [arg1] [orbit number] [.txt output filepath] [--SQL | --dir]

DESCRIPTION:
\tThis script generates one single, canonical Basic Fusion input file list. It parses the database given to it through the basicFusion/metadata-input/queries.bash script and orders the files properly.

ARGUMENTS:
\t[arg1]                        -- The SQLite database made from the scripts in metadata-input/build
\t[orbit number]                -- The orbit to create the input file list for
\t[.txt output filepath]        -- The path to place the resulting output file.
\t[--SQL | --dir]               -- If --SQL, arg1 denotes path to the SQLite database.
\t                                 If --dir, arg1 denotes a directory where the files to be used for the BF input generation
\t                                 reside.
"
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
}

if [ "$#" -ne 4 ]; then
    usage
    exit 1
fi

DB=$1; shift
ORBIT=$1; shift
ORDERED="$1"; shift
INPUT_OPT="$1"; shift

UNORDERED="$ORDERED".unorderedDB

SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ORBITINFO="$SCRIPT_PATH/../data/Orbit_Path_Time.txt"
PROJ_PATH="$(cd "${SCRIPT_PATH}/../../" && pwd )"

# Set this to non-zero if you want the unordered database queries to be saved.
SAVE_UNORDERED=0

UNORDERED=$(dirname $ORDERED)/$(basename $ORDERED).unordered

QUERIES="$SCRIPT_PATH/../queries.bash"
#source "$SCRIPT_PATH/../queries.bash"

# =========
# = REGEX =
# =========
MOP_RE="MOP"
CER_RE="CER_SSF"
MOD_RE="MOD0"
AST_RE="AST_L1T"
MIS1_RE="MISR_AM1"
MIS2_RE="MISR_HRLL"

rm -f "$ORDERED"
rm -f "$UNORDERED"

if [ $INPUT_OPT == "--SQL" ]; then
    MOPLINES=$( $QUERIES instrumentOverlappingOrbit "$DB" $ORBIT MOP)
    CERLINES=$( $QUERIES instrumentOverlappingOrbit "$DB" $ORBIT CER)
    MODLINES=$( $QUERIES instrumentStartingInOrbit "$DB" $ORBIT MOD)
    ASTLINES=$( $QUERIES instrumentStartingInOrbit "$DB" $ORBIT AST)
    # The use of "instrumentInOrbit" instead of "instrumentStartingInOrbit" for MISR helps
    # prevent against edge cases
    MISLINES=$( $QUERIES instrumentInOrbit "$DB" $ORBIT MIS)

elif [ $INPUT_OPT == "--dir" ]; then
    # Make $DB an absolute directory
    DB="$(cd $DB && pwd)"

    fileListings=""    
    # Save all of the files in $DB into a variable, prepending their absolute path
    for file in $DB/*; do
        fileListings="${fileListings}${file}\n"
    done

    orbitPlusOne=$((ORBIT + 1))
    MOPLINES=$( printf "$fileListings" | grep "$MOP_RE")
    CERLINES=$( printf "$fileListings" | grep "$CER_RE")
    MODLINES=$( printf "$fileListings" | grep "$MOD_RE")
    ASTLINES=$( printf "$fileListings" | grep "$AST_RE")
    MISLINES=$( printf "$fileListings" | grep -e "$MIS1_RE" -e "$MIS2_RE" | grep -v "_O[0-9]*${orbitPlusOne}_" )
else
    echo "ERROR: Unrecognized argument: $INPUT_OPT"
    exit 1
fi
  
# LTC 1/27/2018: Decided to not use virtual environment, rather just
# put all python dependencies in python's USER_LOCAL  
# source "$PROJ_PATH"/externLib/BFpyEnv/bin/activate

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

else
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
echo -e "$ORBIT\n$(cat $ORDERED)" > "$ORDERED"

# Perform verification (ensures BF program won't crash due to bad input)
verifyFiles "$ORDERED" "$ORBITINFO"
if [ "$?" -ne 0 ]; then
    printf "Failed to verify files.\n" >&2
    printf "Exiting script.\n" >&2
    exit 1
fi

if [ "$SAVE_UNORDERED" -eq 0 ]; then
    rm -r "$UNORDERED"
fi
exit 0
