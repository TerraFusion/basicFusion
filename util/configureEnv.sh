#!/bin/bash
# Author: Landon Clipp
# Email: clipp2@illinois.edui

downloadPY()
{
    local BF_PATH_l="$1"
    local retVal_l
    local virtEnvName="BFpyEnv"
    mkdir -p "$BF_PATH_l"/externLib
    cd "$BF_PATH_l"/externLib
    
    pyVers=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    if [ $(cut -d'.' -f1,2 <<< $pyVers) != "2.7" ]; then 
        echo "ERROR: Need Python 2.7. Current version is: $pyVers" >&2
        echo "Configure your environment such that the \"python\" command calls Python 2.7." >&2
        echo "$pyVers"
        return 1
    fi

    # Create the virtual environment
    rm -rf "$virtEnvName"
    echo "Generating the virtual environment..."
    virtualenv --system-site-packages ./"$virtEnvName"
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
    echo "Upgrading pip..."
    pip install --upgrade pip
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to update pip." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    # Download the dependencies
    echo "Downloading dependencies"
    echo

    echo "Downloading docopt..."
    pip install docopt
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the docopt module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Downloading pytz..."
    pip install pytz 
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the pytz module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Downloading globus-sdk..."
    pip install globus-sdk
    retval_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the Globus SDK module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Downloading globus-cli..."
    pip install globus-cli
    retval_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the Globus CLI module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo


    echo "Downloading mpi4py..."
    pip install mpi4py
    retval_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the mpi4py module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Downloading numpy..."
    pip install numpy
    retval_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the numpy module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Downloading matplotlib..."
    pip install matplotlib
    retval_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to download the matplotlib module." >&2
        return $retVal_l
    fi
    echo "Done."
    echo

    echo "Copying BFfile.py to site-packages..."
    cp "$BF_PATH_l"/util/BFpy/basicFusion.py "$BF_PATH_l"/externLib/BFpyEnv/lib/python2.7/site-packages
    echo "Done."
    echo

    echo "Writing virtual environment activation helper script to ${HOME}/bin..."
    local script="#!/bin/bash
source \"$BF_PATH_l\"/externLib/BFpyEnv/bin/activate
"
    echo "$script" > ${HOME}/bin/activateBF
    chmod +x ${HOME}/bin/activateBF
    echo "Done."
    echo
    echo "==================================================================================="
    echo "= NOTE: Activate the virtual environment at any time by typing: source activateBF ="
    echo "==================================================================================="
    echo

    deactivate

    
    echo "Python setup complete."
    echo
    return 0
}

# Args:
# [Scheduler GitHub URL] [Scheduler download path]
downloadSched()
{
    local SCHED_URL_l
    local SCHED_PATH_l
    local retVal_l
    local SCHED_URL_l="$1"
    local SCHED_PATH_l="$2"
    local hn_l=$(hostname)
    # Change to the required PrgEnv module
    oldPRGENV=$(module list | grep PrgEnv | cut -f3 -d" ")
    if [ ${#oldPRGENV} -ne 0 ]; then
        retVal_l=0
        if [ $hn != "cg-gpu01" ]; then
            module swap $oldPRGENV PrgEnv-gnu
            retVal_l=$?
        fi
        if [ $retVal_l -ne 0 ]; then
            echo "Failed to swap modules." >&2
            return $retVal_l
        fi

    else
        retVal_l=0
        if [[ $hn_l == "h2ologin"* ]]; then
            module load PrgEnv-gnu
            retVal_l=$?
        fi
        if [ $retVal_l -ne 0 ]; then
            echo "Failed to load the proper modules." >&2
            return $retVal_l
        fi
    fi 

    cd $SCHED_PATH_l
    rm -rf "$SCHED_PATH_l"/Scheduler
    git clone "$SCHED_URL_l"
    retVal_l=$?
    if [ $retVal_l -ne 0 ]; then
        echo "Failed to clone the Scheduler repository." >&2
        return $retVal_l
    fi

    cd Scheduler

    # Now need to compile the Fortran executable
    echo "Compiling Scheduler Fortran code..."
    local hn=$(hostname)
    if [ $hn == "cg-gpu01" ]; then              # ROGER's login hostname is cg-gpu01
        module load mpich
        mpif90 -o scheduler.x scheduler.F90
        retVal_l=$?
    else                                        # Else assume we are on Blue Waters
        module load cray-mpich
        ftn -o scheduler.x scheduler.F90
        retVal_l=$?
    fi

    if [ $retVal_l -ne 0 ]; then
        echo "Failed to compile the Scheduler Fortran code." >&2
        return $retVal_l
    fi
    echo "Done."
    echo "Scheduler setup complete."
    return 0
}

if [ $# -ne 1 ]; then
    echo 
    printf "\nUSAGE: $0 [-s | -p | -a]\n" >&2
    pyVers=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    description="
DESCRIPTION:

\tThis script generates the environment necessary to run all of the BasicFusion scripts. Namely, the two main dependencies are certain Python modules (for the database generation) and NCSA's Scheduler program. This handles downloading and configuring all of the dependencies. This script was designed to run on either Blue Waters or ROGER. It attempts to detect which system it is on and then loads the appropriate Fortran and MPI modules. If this step fails, please review this script's code and modify it to use the appropriate compiler and/or module.

ARGUMENTS:
\t[-s | -p | -a]                -- -s to download just Scheduler
\t                                 -p to download just the Python packages
\t                                 -a to download both Scheduler and Python

REQUIREMENTS:
\tThe following requirements must be met:
\t\t1. A Python 2.7 interpreter is visible. Current detected version is: $pyVers

"
 
    while read -r line; do
        printf "$line\n" >&2
    done <<< "$description"
    exit 1
fi

# NOTE TO FUTURE USERS:
# If this script somehow fails to download the proper Python modules, here is a high level description of what it is trying
# to accomplish. First, it makes a python Virtual Environment in the externLib directory. Then, it sources the activate file
# to initiate the newly downloaded Virtual Environment. It then proceeds to download the docopt and the pytz Python modules
# using the:
#   pip install docopt
#   pip install pytz
# commands.
#
# This virtual environment is needed by the fusionBuildDB python script to build the BasicFusion SQLite database. Virtual
# environment was created to remove dependencies on the system's Python packages.
#
# If the script fails to download the Scheduler program, this is what it was trying to do:
#   git clone https://github.com/ncsa/Scheduler
#   cd Scheduler
#   module load PrgEnv-gnu
#   ftn -o scheduler.x Scheduler.F90
#
# Scheduler is used by the processBF_SX.sh and the genInputRange_SX.sh scripts to submit a large number of batch jobs to
# the queueing system.

#__________CONFIGURABLE VARIABLES___________#
DOWNLOAD_OPTS="$1"
SCHED_URL="https://github.com/ncsa/Scheduler"
#___________________________________________#

# Get the absolute path of this script
ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the basicFusion project directory
BF_PATH="$( cd "$ABS_PATH/../" && pwd )"

# Where Scheduler will be downloaded to
SCHED_PATH=$BF_PATH/externLib

DOWNLOAD_SCHED=0
DOWNLOAD_PY=0
OLD_CC=''
OLD_CXX=''

# Determine the command line options
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

# Detect whether we are on Blue Waters or ROGER, then load the modules that we need
hn=$(hostname)
ON_ROGER=0
ON_BW=0

# ON BLUE WATERS (we are assuming)
echo "Checking which system we're on..."
if [[ $hn == "h2ologin"* ]]; then
    PrgEnv=$(module list | grep PrgEnv)
    if [ ${#PrgEnv} -eq 0 ]; then
        module load PrgEnv-cray
    fi
    module load cray-mpich
    # Blue waters, we need to use cc and CC compilers for C and C++ respectively...
    OLD_CC=$CC
    OLD_CXX=$CXX
    export CC="cc"
    export CXX="CC"
    ON_BW=1
    echo "DETECTED BLUE WATERS."
    echo

    module load bwpy
    module load bwpy-mpi
# ON ROGER
elif [[ $hn == "cg-gpu01" ]]; then
    module load mpich
    ON_ROGER=1
    echo "DETECTED ROGER."
    echo
else
    echo "ERROR: Unable to detect which system we're on."
    exit 1
fi

if [ $DOWNLOAD_PY -eq 1 ]; then
    downloadPY "$BF_PATH"
    retVal=$?

    if [ $retVal -ne 0 ]; then
        echo "Failed to setup Python." >&2
        export CC=$OLD_CC
        export CXX=$OLD_CXX
        exit $retVal
    fi
fi

if [ $DOWNLOAD_SCHED -eq 1 ]; then
    downloadSched "$SCHED_URL" "$SCHED_PATH"
    retVal=$?

    if [ $retVal -ne 0 ]; then
        echo "Failed to setup NCSA Scheduler." >&2
        export CC=$OLD_CC
        export CXX=$OLD_CXX
        exit $retVal
    fi

fi

export CC=$OLD_CC
export CXX=$OLD_CXX
exit 0
