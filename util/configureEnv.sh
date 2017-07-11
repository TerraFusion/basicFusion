#!/bin/bash

downloadPY()
{
    local BF_PATH_l="$1"
    local retVal_l
    local virtEnvName="BFpyEnv"

    mkdir -p "$BF_PATH_l"/externLib
    cd "$BF_PATH_l"/externLib
    
    module load $PY_MOD     # Load the python module
    # Create the virtual environment
    rm -rf "$virtEnvName"
    echo "Generating the virtual environment..."
    virtualenv --no-site-packages ./"$virtEnvName"
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to establish the virtual environment." >&2
        return $retVal_l
    fi
    echo "Virtual environment created."
    echo
    
    # Source the virtualEnv
    cd BFpyEnv
    source ./bin/activate

    # Download the dependencies
    echo "Downloading dependencies"
    pip install docopt
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the docopt module." >&2
        return $retVal_l
    fi

    pip install pytz 
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the pytz module." >&2
        return $retVal_l
    fi

    deactivate

    echo "Python setup complete."

    return 0
}

if [ $# -ne 1 ] && [ $# -ne 2 ]; then
    printf "\nUSAGE: $0 [Scheduler directory path] [-s | -p | -a]\n" >&2
    description="
DESCRIPTION:

\tThis script generates the environment necessary to run all of the BasicFusion scripts. Namely, the two main dependencies are certain Python modules (for the database generation) and NCSA's Scheduler program. This handles downloading and configuring all of the dependencies. This script only works on Blue Waters.

ARGUMENTS:
\t[Scheduler directory path]    -- The path where this script will download NCSA Scheduler from GitHub. Can be left out if
\t                                 the -p argument is given.
\t[-s | -p | -a]                -- -s to download just Scheduler
\t                                 -p to download just the Python packages
\t                                 -a to download both Scheduler and Python
\t                                 If no argument is given here, -a is assumed.
"
 
    while read -r line; do
        printf "$line\n"
    done <<< "$description"
    exit 1
fi

#__________CONFIGURABLE VARIABLES___________#
SCHED_PATH=$1
DOWNLOAD_OPTS="$2"
SCHED_URL="https://github.com/ncsa/Scheduler"
PY_MOD="python"                                 # The site python module to load
#___________________________________________#

# Get the absolute path of this script
ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the basicFusion project directory
BF_PATH="$( cd "$ABS_PATH/../" && pwd )"

DOWNLOAD_SCHED=0
DOWNLOAD_PY=0

# Determine the command line options
if [ $# -eq 1 ]; then
    if [ "$1" == "-p" ]; then
        DOWNLOAD_PY=1
    elif [ "$1" == "-s" ] || [ "$1" == "-a" ]; then
        printf "Error: Scheduler was indicated for download, but no path was provided.\n" >&2
        exit 1
    else
        # Assume both SCHED and PY are being downloaded
        DOWNLOAD_SCHED=1
        DOWNLOAD_PY=1
    fi
elif [ $# -eq 2 ]; then
    if [ "$DOWNLOAD_OPTS" == "-s" ]; then
        DOWNLOAD_SCHED=1
    elif [ "$DOWNLOAD_OPTS" == "-p" ]; then
        DOWNLOAD_PY=1
    elif [ "$DOWNLOAD_OPTS" == "-a" ]; then
        DOWNLOAD_SCHED=1
        DOWNLOAD_PY=1
    else
        printf "Unrecognized command line argument: $DOWNLOAD_OPTS\n" >&2
        exit 1
    fi
fi

if [ $DOWNLOAD_PY -eq 1 ]; then
    downloadPY "$BF_PATH"
    retVal=$?

    if [ $retVal -ne 0 ]; then
        echo "Failed to setup Python." >&2
        exit $retVal
    fi
fi

