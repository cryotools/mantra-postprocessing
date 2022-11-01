#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 17:04:42 2019

Helper tool to convert a full TSL file into individual files per glacier

@author: David Loibl
"""

import pandas as pd
import sys
import os
import re


# USER VARIABLES
verbosity = 2           # Reporting level


if verbosity >= 2:
    i = 0
    for i in range(len(sys.argv)):
        print('Arg '+ str(i) +' set to '+ str(sys.argv[i]))
        i += 1
    
if len(sys.argv) != 4:
    print("\nUsage: python plot-glacier-timeseries.py <tsl_file> <output_dir> <output_type>\n\n")
    print("   tsl_file             -> A TSL result file in HDF format. ")
    print("   output_dir           -> A valid directory to which the output will be written.")
    print("   output_type          -> Output file type (csv or hdf5).\n\n")
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


# OUTPUT DIRECTORY
output_dir = sys.argv[2]

output_type = sys.argv[3]


if re.search('.h5$', input_file):
    print('\nReading HDF5 input file. This may take a while ...')
    df_TSL = pd.read_hdf(input_file, parse_dates=True, index_col='LS_DATE', low_memory=False)
elif re.search('.csv$', input_file):
    print('\nReading CSV input file. This may take a while ...')
    df_TSL = pd.read_csv(input_file, parse_dates=True, index_col='LS_DATE', low_memory=False)
else:
    print('\nInput file must be .csv or .h5\n Exiting ...\n')
    sys.exit[1]
    
glacier_ids = df_TSL.RGI_ID.unique()

for glacier_id in glacier_ids:
    if output_type == 'hdf5':
        print('\n Writing '+ output_dir + glacier_id +'.h5'+'.\n')
        df_TSL.to_hdf(output_dir + glacier_id +'.h5', key='TSLs', mode='w', format='table', data_columns=['RGI_ID'])
    elif output_type == 'csv':
        print('\n Writing '+ output_dir + glacier_id +'.csv'+'.\n')
        df_TSL.to_csv(output_dir + glacier_id +'.csv', index=False)
    else:
        print('\nUnknown output format specified. Please use either "hdf5" or "csv".\n')


