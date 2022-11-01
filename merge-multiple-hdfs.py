#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 18:22:16 2019

@author: David Loibl
"""

import pandas as pd
# import numpy as np
# import re
import os
import sys

verbosity = 2   # Reporting level

if verbosity >= 2:
    i = 0
    for i in range(len(sys.argv)):
        print('Arg '+ str(i) +' set to '+ str(sys.argv[i]))
        i += 1
    
if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("\nUsage: python merge-multiple-hdfs.py <input_dir> <output_file> <file_list>\n\n")
    print("   input_dir            -> Directory containing the HDF5 files to merge. ")
    print("   output_file          -> (Path and) file name of the file to write. Ending should be .h5")
    print("   file_list            -> Optional: An ASCII text file containing the names of the files to merge.")
    print("                           If not provided, all .h5 files in <input_dir> will be merged.\n\n")

    sys.exit(1)


# INPUT FILE
input_path = sys.argv[1]
# input_file = '../data/TSL/preprocessed/TSL-filtered.h5'
exists = os.path.isdir(input_path)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('\nCRITICAL ERROR')
    print(str(input_path) +' is no valid directory.')
    print('Exiting ...')
    sys.exit(1)


# OUTPUT FILE
output_file = sys.argv[2]


# GLACIER LIST

fallback_search = False

if len(sys.argv) >= 4:
    file_list = sys.argv[3]

    exists = os.path.isfile(file_list)
    if not exists:
        print('WARNING: no valid file list found at ' + str(file_list) +'. Falling back to search the input directory.')
        fallback_search = True
    else:    
        # Use existing glacier ID file to determine glaciers to process ...
        hdf_files = []
        
        with open(file_list, 'r') as filehandle:
            for line in filehandle:
                # remove linebreak which is the last character of the string
                current_hdf = line[:-1]
                
                # add item to the list
                hdf_files.append(current_hdf)


if len(sys.argv) == 3 or fallback_search == True:
    print('Searching for .h5 in '+ str(input_path) +' ...')
    hdf_files = []
    for found_file in os.listdir(input_path):
        if found_file.endswith(".h5"):
            hdf_files.append(found_file)
        
        # Use existing glacier ID file to determine glaciers to process ...
    if len(hdf_files) > 0:
        print('Found '+ str(len(hdf_files)) +' HDF5 files')
    else:
        print('Found no HDF files. Exititng')
        sys.exit(1)

# exists = os.path.isfile(list_file)

# if exists:
#     # Read list of HDF files
#     hdf_files = []
    
#     with open(list_file, 'r') as filehandle:
#         for line in filehandle:
#             # remove linebreak which is the last character of the string
#             current_hdf = line[:-1]
    
#             # add item to the list
#             hdf_files.append(current_hdf)

print('\nAppending HDFs to data frame...')
i = 0
for hdf_file in hdf_files:
    print('Processing '+ str(hdf_file))
    df_tmp = pd.read_hdf(input_path +'/'+ hdf_file) #, 'TSLs'
    if i == 0:
        df_TSL = df_tmp.copy()
    else:
        df_TSL = df_TSL.append(df_tmp, ignore_index=True)
    i += 1

if df_TSL.shape[0] > 0:
    print('Writing output file. This may take a while ...')
    df_TSL.to_hdf(output_file, key='TSLs', format='table', data_columns=['RGI_ID'])
else:
    print('No rows found in '+ str(input_path) +'. Skipping ...')

print('Processing finished')
