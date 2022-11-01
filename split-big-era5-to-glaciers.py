#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 17:04:42 2019

Helper tool to convert a full TSL file into individual files per glacier

@author: loibldav
"""

import pandas as pd
import sys
import os


# USER VARIABLES
verbosity = 2           # Reporting level


if verbosity >= 2:
    i = 0
    for i in range(len(sys.argv)):
        print('Arg '+ str(i) +' set to '+ str(sys.argv[i]))
        i += 1
    
if len(sys.argv) != 4:
    print("\nUsage: python era5-hfd5-to-glaciers-csvs.py <hdf5_file> <glacier_id_list> <output_dir>\n\n")
    print("   hdf5_file            -> A TSL result file in HDF format. ")
    print("   glacier_id_list      -> A list of glacier IDs, one ID per row, no header. ")
    print("   output_dir           -> A valid directory to which the output will be written.\n\n")
    #print("   output_type          -> Output file type (csv or hdf5).\n\n")
    sys.exit(1)


# INPUT FILE
input_file = sys.argv[1]
exists = os.path.isfile(input_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('\nCRITICAL ERROR')
    print('No input file found at '+ str(input_file))
    print('Exiting ...')
    sys.exit(1)

# INPUT FILE
glacier_id_list = sys.argv[2]
exists = os.path.isfile(input_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('\nCRITICAL ERROR')
    print('No id list file found at '+ str(glacier_id_list))
    print('Exiting ...')
    sys.exit(1)
    
# OUTPUT DIRECTORY
output_dir = sys.argv[3]

#output_type = sys.argv[3]


df_ids = pd.read_csv(glacier_id_list, sep=" ", header=None)
glacier_ids = df_ids[0].to_list()

for glacier_id in glacier_ids:
    # print('\n Working on glacier '+ str(glacier_id) +' ...')
    df_era5land = pd.read_hdf(input_file, key="ERA5/"+ str(glacier_id), low_memory=False)

    print('\n Writing '+ output_dir + glacier_id +'.h5'+'.')
    df_era5land.to_hdf(output_dir + glacier_id +'.h5', key='TSLs', mode='w', format='table', data_columns=['RGI_ID'])

print('\nProcessing finished.\n')
