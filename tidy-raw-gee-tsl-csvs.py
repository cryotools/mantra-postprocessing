#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 08:43:44 2019

@author: David Loibl
"""
import numpy as np
import pandas as pd
#import os

# User configuration

input_file = '/home/loibo/Processing/TopoCliF/TSL/TSL-results-update-2021-12.csv'
output_file = '../data/TSL/preprocessed/TSLA-HMA-2021-12-upd-filtered.h5'

''' OLD
data_paths = {'TSL_orig': '/home/loibldav/Processing/topoCliF-GEE/raw/',
              'TSL_trtd': '/home/loibldav/Processing/topoCliF-GEE/processed/',
              'subdir': 'test_set/'}


# get all file names
files = os.listdir(data_paths['TSL_orig'] + data_paths['subdir'])
files = [x for x in sorted(files)]

if n_files == 0:
    n_files = len(files)

# read files
for j in range(0, n_files):
    tmp = pd.read_csv(data_paths['TSL_orig'] + data_paths['subdir'] + files[j], header=0)
    print('Reading file '+files[j])
    if j == 0:
        df_TSL = tmp
    else:
        df_TSL = pd.concat([df_TSL, tmp])
'''

print('\nReading input file. This may take a while ...')
df_TSL = pd.read_csv(input_file)
n_full = len(df_TSL)

# remove nans
df_TSL = df_TSL.loc[df_TSL.SC_median > 0, :]
n_noNaN = len(df_TSL)
n_diff = n_full - n_noNaN
print('Removed '+ str(n_diff) +' NaN rows')

# Fix LS_ID column label
if 'LS_ID: ' in df_TSL.columns:
    df_TSL.rename(columns={'LS_ID: ':'LS_ID'}, inplace=True)
    print('Fixed LS_ID column label')

# remove useless columns
if 'system:index' in df_TSL.columns:
    del df_TSL['system:index']
    print('Removed column system:index')
if '.geo' in df_TSL.columns:
    del df_TSL['.geo']
    print('Removed column .geo')
    
# add column with the scene identifier
tmp = df_TSL.loc[:, 'LS_ID']   # 'LS_ID:'
tmp2 = tmp.str.split(pat='_', expand=True)
df_TSL['scene'] = pd.to_numeric(tmp2.iloc[:, 2])
print('Added scene column')

del tmp
del tmp2

# Normalize TSLs for individual glaciers to their respective elevation ranges
df_TSL['TSL_stddev_norm'] = (df_TSL.SC_stdev - df_TSL.glacier_DEM_min) / (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min)
df_TSL['TSL_max_norm']    = (df_TSL.SC_max - df_TSL.glacier_DEM_min) / (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min)
df_TSL['TSL_min_norm']    = (df_TSL.SC_min - df_TSL.glacier_DEM_min) / (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min)
df_TSL['TSL_normalized']  = (df_TSL.SC_median - df_TSL.glacier_DEM_min) / (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min)



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
df_TSL.class_coverage.values[df_TSL.class_coverage.values > 100] = 100

OK = np.where((df_TSL.CC_total_port < thresholds['CC_total']) &
               (df_TSL.cc_TSLrange_percent < thresholds['CC_TSLrg']) &
               (df_TSL.class_coverage > thresholds['class_cv']) &
               (df_TSL.SC_stdev < (thresholds['max_stdd'] * (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min))) &
               ((df_TSL.SC_max - df_TSL.SC_min) > thresholds['TSL_rnge'] * (df_TSL.glacier_DEM_max - df_TSL.glacier_DEM_min)) &
               ((df_TSL.DIC_area > thresholds['DICa_min']) | (df_TSL.SC_min < (df_TSL.glacier_DEM_min + thresholds['SCmin_of']))) &
               ((df_TSL.DIC_area > (thresholds['DICa_gla'] * df_TSL.glacier_area)) | (df_TSL.SC_min < (df_TSL.glacier_DEM_min + thresholds['SCmin_of']))) &
               ((100-df_TSL.class_coverage+df_TSL.CC_total_port) < thresholds['class_cc_cv'])
               )[0]


failed_measurements = len(df_TSL) - len(OK)
print('Total number of measurements: ' + str(len(df_TSL)))
print('Number of measurements that failed checks: ' + str(failed_measurements))

df_TSL_ok = df_TSL.iloc[OK, :]
n_ok = len(df_TSL_ok)
n_diff = n_noNaN - n_ok
print('Removed '+ str(n_diff) +' rows not fullfilling quality criteria')
'''
df_TSL_ok = df_TSL_ok.copy()
del df_TSL


# Add normalized TSLs as columns
print('Normalizing TSL data to glacier elevation ranges ...')
df_TSL_ok['TSL_normalized'] = (df_TSL_ok.SC_median - df_TSL_ok.glacier_DEM_min) / (df_TSL_ok.glacier_DEM_max - df_TSL_ok.glacier_DEM_min)
df_TSL_ok['TSL_stddev_norm'] = (df_TSL_ok.SC_stdev - df_TSL_ok.glacier_DEM_min) / (df_TSL_ok.glacier_DEM_max - df_TSL_ok.glacier_DEM_min)
df_TSL_ok['TSL_max_norm'] = (df_TSL_ok.SC_max - df_TSL_ok.glacier_DEM_min) / (df_TSL_ok.glacier_DEM_max - df_TSL_ok.glacier_DEM_min)
df_TSL_ok['TSL_min_norm'] = (df_TSL_ok.SC_min - df_TSL_ok.glacier_DEM_min) / (df_TSL_ok.glacier_DEM_max - df_TSL_ok.glacier_DEM_min)
'''

# Write Output


print('Writing output file. This may take a while ...')
# df_subset = df_TSL_ok.loc[1:100,:]
# df_subset.to_hdf(output_file, key='TSLs', format='table', data_columns=df_subset.columns.tolist())

# df_TSL_ok.to_csv(output_file, sep=',',na_rep='NaN')
df_TSL_ok.to_hdf(output_file, key='TSLs', format='table', data_columns=['RGI_ID']) #dropna=True,
