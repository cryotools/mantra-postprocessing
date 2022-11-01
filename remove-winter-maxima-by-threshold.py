#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 18:22:16 2019

@author: loibldav
"""

import pandas as pd
import numpy as np
import re
import os
import sys

verbosity = 2           # Reporting level
thres_wintermax = 0.3   # Remove winter maxima > threshold                        

input_file = '../data/TSL/preprocessed/TSLA-HMA-2021-12-upd-filtered-nWmax_0_2SLAthres.h5'
exists = os.path.isfile(input_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('\nCRITICAL ERROR')
    print('No input file found at '+ str(input_file))
    print('Exiting ...')
    sys.exit(1)

output_file = re.sub('.h5$', '-WMthres'+ str(thres_wintermax) +'.h5', input_file)

# Read TSL data   
print('\nReading input full file. This may take a while ...')
df_TSL = pd.read_hdf(input_file, 'TSLs')

n_rows = df_TSL.shape[0]
n_cols = df_TSL.shape[1]
print('Success. Input dataset contains '+ str(n_rows) +' rows and '+ str(n_cols) +' cols')

df_TSL_cols = df_TSL.columns

if df_TSL.index.name == 'LS_DATE':
    if 'LS_DATE' in df_TSL.columns:
        df_TSL.reset_index(drop=True, inplace=True)
    else:
        df_TSL.reset_index(inplace=True)

if 'year' not in df_TSL_cols:
    df_TSL['year'] = pd.DatetimeIndex(df_TSL['LS_DATE']).year

if 'month' not in df_TSL_cols:
    df_TSL['month'] = pd.DatetimeIndex(df_TSL['LS_DATE']).month    


winter_maxima = np.where(((df_TSL['month'] < 7) | (df_TSL['month'] > 10)) & (df_TSL['TSL_normalized'] > thres_wintermax))[0]  

print('Removing '+ str(len(winter_maxima)) +' winter maxima > threshold '+ str(thres_wintermax))

df_TSL.drop(winter_maxima, axis=0, inplace=True)

print('Writing output file. This may take a while ...')
df_TSL.to_hdf(output_file, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])

print('\nProcessing finished\n')

