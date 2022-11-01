#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 09:06:15 2020

@author: David Loibl
"""

import pandas as pd
import datetime

# Start the clock
start_time = datetime.datetime.now().timestamp()


# General configuration
first_summer_month = 7
last_summer_month  = 10
# Path setup
input_path_tslprep = '../data/TSL/preprocessed/'

# File setup
input_file_tsl     = '<INPUT_FILE>.h5'
out_file_tsl_dd    = '<OUTPUT_FILE>.h5'
out_file_tsl_JASO  = '<OUTPUT_FILE_JULY_TO_OCT_ONLY>.h5'

### IMPORT DATA

# Import TSLA data
print('\nReading TSLA file. This may take a while ...')
df_tsl = pd.read_hdf(input_path_tslprep + input_file_tsl, parse_dates=True, index_col='LS_DATE', low_memory=False) 
n_orig = df_tsl.shape[0]
print('Success. Data frame contains '+ str(n_orig) +' rows and '+ str(df_tsl.shape[1]) +' columns. \n')   

print('\nDropping duplicates ...')
df_tsl.drop_duplicates(inplace=True)   
n_nodd = df_tsl.shape[0]
print('Success. Removed '+ str(n_orig - n_nodd) +' duplicate rows, '+ str(n_nodd) +' remaining.')

print('Writing cleaned dataframe to disk ...')
df_tsl.to_hdf(input_path_tslprep + out_file_tsl_dd, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])

print('\nExtracting summer subset ...')
df_tsl['LS_DATE'] = pd.to_datetime(df_tsl['LS_DATE'], infer_datetime_format=True)
df_tsl.set_index('LS_DATE', inplace=True)

df_tsl_JASO = df_tsl[(df_tsl.index.month >= first_summer_month) & (df_tsl.index.month <= last_summer_month) ]
n_JASO = df_tsl_JASO.shape[0]
print('Success. Removed '+ str(n_nodd - n_JASO) +' non-Summer rows, '+ str(n_JASO) +' remaining.')

print('Writing cleaned dataframe to disk ...')
df_tsl_JASO.to_hdf(input_path_tslprep + out_file_tsl_JASO, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])


end_time = datetime.datetime.now().timestamp()
proc_time = end_time - start_time
print('\nProcessing finished in '+ str(datetime.timedelta(seconds=proc_time)) +'\n')
