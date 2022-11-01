#!/usr/bin/env bash

if [ $# -eq 0 ]; then
    echo
    echo "Usage: launch_rwm.sh <TSL_input_file> <list_file> <step_size> <output_path>"  
    echo
elif [ ! -f $1 ]; then
    echo
    echo "Cannot open $1. Please provide a valid data file in HDF5 format."
    echo
else

    # PATH DEFINITIONS
    # Modify according to your username and configuration
    script=$( realpath $0 )
    SCRIPT_PATH=$( dirname $script )

    WORK_PATH="<PATH_TO_STORE_STUFF>"
    OUTPUT_PATH="<PATH_TO_STORE_LOG_FILES>" 
    ERROR_PATH="<PATH_TO_STORE_ERROR_FILES>"      
    

    if [ ! -d "$WORK_PATH" ]; then mkdir -p $WORK_PATH; fi
    if [ ! -d "$OUTPUT_PATH" ]; then mkdir -p $OUTPUT_PATH; fi
    if [ ! -d "$ERROR_PATH" ]; then mkdir -p $ERROR_PATH; fi

    # PYTHON file
    # Path and name of the python file to be executed
    python_file="<PATH_TO_REPO>/maxima-per-glacier-and-year.py"  

    ACCOUNT=morsanat

    input_file=$1

    list_file=$2
    n_lines=$(wc -l < "$list_file")


    if [ -z ${3+x} ]; then 
        step=100
    else 
        step=$3
    fi
    
    i=0
    
    while [ $i -le $n_lines ]; do
	# --exclusive \
        sbatch \
	    --ntasks=5 \
	    --output=$OUTPUT_PATH/%j.log \
	    --error=$ERROR_PATH/%j.log \
	    --workdir=$WORK_PATH \
	    --job-name="PPpy$i" \
	    --qos=short \
	    --account=$ACCOUNT \
	    --partition=computehm \
	    --mail-type=ALL \
	    ${SCRIPT_PATH}/PP_python.sh \
            $python_file \
            $input_file \
            $list_file \
            $i \
            $(( i + step )) \
            False

        ((i+=step))
    done
    
fi











