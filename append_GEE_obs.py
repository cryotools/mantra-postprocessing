#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 13:58:14 2019

@author: loibldav
"""

import pandas as pd
import numpy as np
import os

input_file = "/home/loibo/Processing/TopoCliF/TSL/TSL-results-update-2021-12.csv"
glacier_TSL_file = "/data/Git/1_TopoCliF/fit-intern/data/TSL/preprocessed/TSL-update-2021-12.h5"
output_file = "/home/loibldav/Git/1_TopoCliF/fit-intern/data/TSL/preprocessed/test.h5"
remove_exisitng_output_file = 1

print('Reading input file. This may take a while ...')
df_TSL_add = pd.read_csv(input_file, 
                         parse_dates=True,                          
                         low_memory=False)

print('Success. Imported file contains '+ str(df_TSL_add.shape[0]) +' rows and ' + str(df_TSL_add.shape[1]) +' columns')

df_TSL_add.LS_DATE = pd.to_datetime(df_TSL_add.LS_DATE, format='%Y-%m-%d')
df_TSL_add.index = pd.to_datetime(df_TSL_add.LS_DATE, format='%Y-%m-%d')

# Fix LS_ID column label
if 'LS_ID: ' in df_TSL_add.columns:
    df_TSL_add.rename(columns={'LS_ID: ':'LS_ID'}, inplace=True)
    print('Fixed LS_ID column label')

# remove useless columns
if 'system:index' in df_TSL_add.columns:
    df_TSL_add.drop(['system:index'], axis=1, inplace=True)
    print('Removed column system:index')
if '.geo' in df_TSL_add.columns:
    df_TSL_add.drop(['.geo'], axis=1, inplace=True)
    print('Removed column .geo')

if 'LS_scene' not in df_TSL_add.columns:
    tmp = df_TSL_add.loc[:, 'LS_ID']   # 'LS_ID:'
    tmp2 = tmp.str.split(pat='_', expand=True)
    del tmp
    df_TSL_add['LS_scene'] = pd.to_numeric(tmp2.iloc[:, 2])
    del tmp2
    print('Added column LS_scene')
    
df_TSL_add.head()

# Normalize TSLs for individual glaciers to their respective elevation ranges
df_TSL_add['TSL_stddev_norm'] = (df_TSL_add.SC_stdev - df_TSL_add.glacier_DEM_min) / (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min)
df_TSL_add['TSL_max_norm']    = (df_TSL_add.SC_max - df_TSL_add.glacier_DEM_min) / (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min)
df_TSL_add['TSL_min_norm']    = (df_TSL_add.SC_min - df_TSL_add.glacier_DEM_min) / (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min)
df_TSL_add['TSL_normalized']  = (df_TSL_add.SC_median - df_TSL_add.glacier_DEM_min) / (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min)


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
'''
# thresh = [70,70,30,0.1,2,0.01,0.02,50,60]

# 1) CC_total_port < x%
#    total cloud cover is not too large: most relevant because of shadows which may be identified as ice
# 2) cc_TSLrange_percent < x%
# 3) class_coverage > x%
#    almost exclusively an issue in winter
# 4) SC_stdev < x * (glacier_DEM_max - glacier_DEM_min)
#    all pixels identified as TSL should not scatter too much
# 5) (SC_max-SC_min) > x * (glacier_DEM_max - glacier_DEM_min) m
#    all pixels identified as TSL should cover an elevation band of more than x times the glacier elevation range
#    (mostly an issue if there are too many clouds)
# 6) DIC_area > x km²
#    either ice/debris should be visible on x km² OR 8)
# 7) DIC_area > x * glacier_area
#    either ice/debris should be visible on x % of the glacier OR 8)
# 8) SC_min < glacier_DEM_min + x m
#    threshold for the criteria 6) and 7) where little ice/debris area is allowed
#    as the TSL is very close to the glacier minimum elevation
# 9) 100 - class_coverage + CC_total_port < 60
#    threshold for the criteria combination of 1) and 3) where cloud cover is not accounted as no class coverage
#    so that scenes with lots of missing pixels, as well as cloud cover are filtered out if not atleast 1-class_cc_cv %
#    of glacier has sufficient classification data
'''
#first set class coverage > 100 to 100
df_TSL_add.class_coverage.values[df_TSL_add.class_coverage.values > 100] = 100

OK = np.where((df_TSL_add.CC_total_port < thresholds['CC_total']) &
               (df_TSL_add.cc_TSLrange_percent < thresholds['CC_TSLrg']) &
               (df_TSL_add.class_coverage > thresholds['class_cv']) &
               (df_TSL_add.SC_stdev < (thresholds['max_stdd'] * (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min))) &
               ((df_TSL_add.SC_max - df_TSL_add.SC_min) > thresholds['TSL_rnge'] * (df_TSL_add.glacier_DEM_max - df_TSL_add.glacier_DEM_min)) &
               ((df_TSL_add.DIC_area > thresholds['DICa_min']) | (df_TSL_add.SC_min < (df_TSL_add.glacier_DEM_min + thresholds['SCmin_of']))) &
               ((df_TSL_add.DIC_area > (thresholds['DICa_gla'] * df_TSL_add.glacier_area)) | (df_TSL_add.SC_min < (df_TSL_add.glacier_DEM_min + thresholds['SCmin_of']))) &
               ((100-df_TSL_add.class_coverage+df_TSL_add.CC_total_port) < thresholds['class_cc_cv'])
               )[0]


failed_measurements = len(df_TSL_add) - len(OK)
print('Total number of measurements: ' + str(len(df_TSL_add)))
print('Number of measurements that failed checks: ' + str(failed_measurements))

df_TSL_add = df_TSL_add.iloc[OK,:]

unique_glaciers = df_TSL_add['RGI_ID'].unique()
n_glaciers = len(unique_glaciers)
print('Data set contains '+ str(n_glaciers) +' unique glaciers.')

if os.path.isfile(output_file):
    print('')
    if remove_exisitng_output_file == 1:
        print('Output file exists and will be deleted')
        os.remove(output_file)
    else:
        print('WARNING: Output file exists but should not be deleted.\nData will be appended which may lead to lots of dublettes!')

exists = os.path.isfile(glacier_TSL_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('No input file found at '+ str(glacier_TSL_file) +' - skipping')
else:
    n_runs = 0
    for glacier_ID in unique_glaciers:
        #print('Appending data for glacier with RGI-ID '+ str(glacier_ID) +' ...')
        #glacier_TSL_file = glacier_TSL_path +'/'+ glacier_ID +'.h5'
        
        df_TSL_glacier_add = df_TSL_add[df_TSL_add.RGI_ID == glacier_ID] 
        # print('Found '+ str(df_TSL_glacier_add.shape[0]) +' new observations for '+ str(glacier_ID) +' ...')
        # print('Reading glacier file')
        df_TSL_glacier = pd.read_hdf(glacier_TSL_file,  where=['RGI_ID == glacier_ID'], parse_dates=True, low_memory=False)
        

        df_TSL_glacier.index = pd.to_datetime(df_TSL_glacier.LS_DATE, format='%Y-%m-%d')
        df_TSL_glacier.LS_DATE = pd.to_datetime(df_TSL_glacier.LS_DATE, format='%Y-%m-%d')
        
        unique_LS_dates_base = df_TSL_glacier.LS_DATE.unique()
        unique_LS_dates_add  = df_TSL_glacier_add.LS_DATE.unique()
        
        non_duplicate_dates = np.invert(np.isin(unique_LS_dates_add, unique_LS_dates_base))
        
        df_TSL_glacier_add_non_duplicate = df_TSL_glacier_add.loc[non_duplicate_dates,:]
        
        df_TSL_glacier.drop(['year', 'month'], axis=1, inplace=True)
        
        df_result = df_TSL_glacier.append(df_TSL_glacier_add_non_duplicate, sort=True)
        
        progress = (n_runs + 1) / n_glaciers * 100
        print('Processing '+ str(glacier_ID) +' - adding '+ str(df_TSL_glacier_add_non_duplicate.shape[0]) +' new rows ['+ str(n_runs) +' of '+ str(n_glaciers) +'  - '+ str(round(progress, 4)) +' %]')
        df_result.to_hdf(output_file, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])
        
        n_runs += 1
        
print('\nProcessing finished.\n')