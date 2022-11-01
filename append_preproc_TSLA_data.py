#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 13:58:14 2019

Append preprocessed TSLA data (HDF file) to existing TSLA dataset.
The data to append should already be filtered.

@author: David Loibl
"""

import pandas as pd
import numpy as np
import os

append_file = "<FILE_TO_APPEND>.h5"
existing_file = "<FILE_TO_APPEND_NEW_DATA_TO>.h5"
output_file = "<OUTPUT_FILE>.h5"
remove_exisitng_output_file = 1
filter_again = False

print('Reading input file. This may take a while ...')
df_TSL_new = pd.read_hdf(append_file, 'TSLs', parse_dates=True, low_memory=False)

print('Success. Imported file contains '+ str(df_TSL_new.shape[0]) +' rows and ' + str(df_TSL_new.shape[1]) +' columns')

# df_TSL_new.LS_DATE = pd.to_datetime(df_TSL_new.LS_DATE, format='%Y-%m-%d')
# df_TSL_new.index = pd.to_datetime(df_TSL_new.LS_DATE, format='%Y-%m-%d')


# Normalize TSLs for individual glaciers to their respective elevation ranges
if 'TSL_stddev_norm' not in df_TSL_new.columns:
    df_TSL_new['TSL_stddev_norm'] = (df_TSL_new.SC_stdev - df_TSL_new.glacier_DEM_min) / (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min)
if 'TSL_max_norm' not in df_TSL_new.columns:    
    df_TSL_new['TSL_max_norm']    = (df_TSL_new.SC_max - df_TSL_new.glacier_DEM_min) / (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min)
if 'TSL_min_norm' not in df_TSL_new.columns:
    df_TSL_new['TSL_min_norm']    = (df_TSL_new.SC_min - df_TSL_new.glacier_DEM_min) / (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min)
if 'TSL_normalized' not in df_TSL_new.columns:    
    df_TSL_new['TSL_normalized']  = (df_TSL_new.SC_median - df_TSL_new.glacier_DEM_min) / (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min)

if filter_again:
    # Remove flawed measurements based on intrinsic cirteria
    print('Removing flawed measurements based on intrinsic cirteria ...')
    thresholds = {
        'CC_total': 70,
        'CC_TSLrg': 40,
        'class_cv': 40,
        'max_stdd': 0.2,
        'TSL_rnge': 0.005,
        'DICa_min': 0.01,
        'DICa_gla': 0.02,
        'SCmin_of': 50,
        'class_cc_cv': 60,
    }
    
    #first set class coverage > 100 to 100
    df_TSL_new.class_coverage.values[df_TSL_new.class_coverage.values > 100] = 100
    
    OK = np.where((df_TSL_new.CC_total_port < thresholds['CC_total']) &
                   (df_TSL_new.cc_TSLrange_percent < thresholds['CC_TSLrg']) &
                   (df_TSL_new.class_coverage > thresholds['class_cv']) &
                   (df_TSL_new.SC_stdev < (thresholds['max_stdd'] * (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min))) &
                   ((df_TSL_new.SC_max - df_TSL_new.SC_min) > thresholds['TSL_rnge'] * (df_TSL_new.glacier_DEM_max - df_TSL_new.glacier_DEM_min)) &
                   ((df_TSL_new.DIC_area > thresholds['DICa_min']) | (df_TSL_new.SC_min < (df_TSL_new.glacier_DEM_min + thresholds['SCmin_of']))) &
                   ((df_TSL_new.DIC_area > (thresholds['DICa_gla'] * df_TSL_new.glacier_area)) | (df_TSL_new.SC_min < (df_TSL_new.glacier_DEM_min + thresholds['SCmin_of']))) &
                   ((100-df_TSL_new.class_coverage+df_TSL_new.CC_total_port) < thresholds['class_cc_cv'])
                   )[0]

    failed_measurements = len(df_TSL_new) - len(OK)
    print('Total number of measurements: ' + str(len(df_TSL_new)))
    print('Number of measurements that failed checks: ' + str(failed_measurements))    
    df_TSL_new = df_TSL_new.iloc[OK,:]

unique_glaciers = df_TSL_new['RGI_ID'].unique()
n_glaciers = len(unique_glaciers)
print('Data set contains '+ str(n_glaciers) +' unique glaciers.')

if os.path.isfile(output_file):
    print('')
    if remove_exisitng_output_file == 1:
        print('Output file exists and will be deleted')
        os.remove(output_file)
    else:
        print('WARNING: Output file exists but should not be deleted.\nData will be appended which may lead to lots of dublettes!')

df_TSL_new.LS_DATE = pd.to_datetime(df_TSL_new.LS_DATE, format='%Y-%m-%d')

exists = os.path.isfile(existing_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('No input file found at '+ str(existing_file) +' - skipping')
else:    
    n_runs = 0
    for glacier_ID in unique_glaciers:        
        #print('Appending data for glacier with RGI-ID '+ str(glacier_ID) +' ...')
        #existing_file = glacier_TSL_path +'/'+ glacier_ID +'.h5'
        
        df_TSL_existing_add = df_TSL_new[df_TSL_new.RGI_ID == glacier_ID] 
        # print('Found '+ str(df_TSL_existing_add.shape[0]) +' new observations for '+ str(glacier_ID) +' ...')
        # print('Reading glacier file')
        df_TSL_existing = pd.read_hdf(existing_file,  where=['RGI_ID == glacier_ID'], parse_dates=True, low_memory=False)
        
        # Remove not matchting columns in first run
        if n_runs == 0:
            drop_cols = list(set(list(df_TSL_new.columns)).difference(list(df_TSL_existing.columns)))
            df_TSL_new.drop(drop_cols, axis=1, inplace=True)
            df_result = pd.DataFrame(columns=list(df_TSL_existing.columns))
            print('Setting up result DataFrame')

        df_TSL_existing.index = pd.to_datetime(df_TSL_existing.LS_DATE, format='%Y-%m-%d')
        df_TSL_existing.LS_DATE = pd.to_datetime(df_TSL_existing.LS_DATE, format='%Y-%m-%d')
        
        unique_LS_dates_base = df_TSL_existing.LS_DATE.unique()
        unique_LS_dates_add  = df_TSL_existing_add.LS_DATE.unique()
        
        non_duplicate_dates = np.invert(np.isin(unique_LS_dates_add, unique_LS_dates_base))
        
        df_TSL_existing_add_non_duplicate = df_TSL_existing_add.loc[non_duplicate_dates,:]
        
        df_TSL_existing.drop(['year', 'month'], axis=1, inplace=True)
        df_result = df_TSL_existing.append(df_TSL_existing_add_non_duplicate, sort=True)
        # df_update.reset_index(drop=True, inplace=True)
        # df_result = df_result.append(df_update, ignore_index=True)
        
        # Ensure Integer cols are detected corretly to avoid export error
        int_cols = ['glacier_DEM_min', 'glacier_DEM_max', 'scene']
        for int_col in int_cols:
            if df_result[int_col].dtype != np.int64:
                df_result = df_result.astype({int_col: int})

        drop_cols = ['year', 'month', 'TSL_max_norm', 'TSL_min_norm']
        for drop_col in drop_cols:
            if drop_col in df_result.columns:
                df_result.drop(columns=[drop_col], inplace=True)
                
        df_result.reset_index(drop=True, inplace=True)
        
        progress = (n_runs + 1) / n_glaciers * 100                        
        print('Processing '+ str(glacier_ID) +' - adding '+ str(df_TSL_existing_add_non_duplicate.shape[0]) +' new rows ['+ str(n_runs) +' of '+ str(n_glaciers) +'  - '+ str(round(progress, 4)) +' %]')
        df_result.to_hdf(output_file, key='TSLAs', mode='a', format='table', append=True, data_columns=['RGI_ID'])
        
        n_runs += 1
        # if n_runs == 10:
        #     break

# # Ensure Integer cols are detected corretly to avoid export error
# int_cols = ['glacier_DEM_min', 'glacier_DEM_max', 'scene']
# for int_col in int_cols:
#     if df_result[int_col].dtype != np.int64:
#         df_result = df_result.astype({int_col: int})

# drop_cols = ['year', 'month', 'TSL_max_norm', 'TSL_min_norm']
# for drop_col in drop_cols:
#     if drop_col in df_result.columns:
#         df_result.drop(columns=[drop_col], inplace=True);
        
#print('\nWriting result to file ...')    
#df_result.to_hdf(output_file, key='TSLAs', mode='a', format='table', data_columns=['RGI_ID'])        
print('\nProcessing finished.\n')
