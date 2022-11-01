#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 18:22:16 2019

@author: David Loibl
"""

import pandas as pd
import numpy as np
import re
import os
import sys

verbosity = 2               # Reporting level

thres_relative_sla = 0.2    # Remove winter maxima only when SLA > threshold
                            # Set to 0 to remove all winter maxima

thres_wintermax = 0.3       # Remove all winter values > threshold
                            # Value range 0 to 1
                            # Set to > 1 to deactivate
                            # Set to <= 0 to remove all winter values

abl_phase_begin = 7         # Month in which the ablation phase begins
abl_phase_end   = 10        # Month in which the abl. ph. ends (included)

if len(sys.argv) <= 1 or len(sys.argv) > 6:
    print("\nUsage: python remove-winter-maxima.py <input_file> <glacier_list_file> <lower_limit> <upper_limit> <random_choice>\n")
    print("All arguments but <input_file> are optional and may be deactivated by setting them to 0\n")
    sys.exit(1)


# import pickle
# from os import walk

i = 0
for i in range(len(sys.argv)):
    print('Arg '+ str(i) +' set to '+ str(sys.argv[i]))
    i += 1

# INPUT FILE

input_file = sys.argv[1]
# input_file = '../data/TSL/preprocessed/TSL-filtered.h5'
exists = os.path.isfile(input_file)
if not exists:
    # Use existing glacier ID file to determine glaciers to process ...
    print('\nCRITICAL ERROR')
    print('No input file found at '+ str(input_file))
    print('Exiting ...')
    sys.exit(1)



# GLACIER LIST

if len(sys.argv) >= 3:
    if sys.argv[2] != 0:
        glacier_list_file = sys.argv[2]
    else:    
        glacier_list_file = re.sub('.h5$', '-glaciers.list', input_file)
else:    
    glacier_list_file = re.sub('.h5$', '-glaciers.list', input_file)

# glacier_list_file = '/home/loibldav/Processing/topoCliF-GEE/raw/glaciers-HMA.list'

      

# ACTIVATE RANDOM SELECTION

if len(sys.argv) >= 6:
    if sys.argv[5] != 0:
        random_choice = sys.argv[5]
    else:    
        random_choice = False

    # > 0 -> Limit input df to n glaciers (for debugging)
else:    
    random_choice = False



exists = os.path.isfile(glacier_list_file)

if exists:
    # Use existing glacier ID file to determine glaciers to process ...
    glacier_ids = []
    
    with open(glacier_list_file, 'r') as filehandle:
        for line in filehandle:
            # remove linebreak which is the last character of the string
            current_glacier = line[:-1]
    
            # add item to the list
            glacier_ids.append(current_glacier)


    print('\nReading input file for limited range.')
    df_TSL = pd.read_hdf(input_file, 'TSLs', where=['RGI_ID in glacier_ids'])# where=['RGI_ID=="RGI60-15.09255"']    

    n_glaciers = len(glacier_ids)    
    
else:
    # Read full input dataset, write glacier ID file
    
    print('\nReading input full file. This may take a while ...')
    df_TSL = pd.read_hdf(input_file, 'TSLs')
        
    glacier_ids = df_TSL.RGI_ID.unique()
    n_glaciers = len(glacier_ids)
    
    with open(glacier_list_file, 'w') as filehandle:
        for glacier_id in glacier_ids:
            filehandle.write('%s\n' % glacier_id)

# Convert to time series by using the Landsat date as datetime index
df_TSL.set_index(pd.to_datetime(df_TSL['LS_DATE'].values), inplace=True)


# LOWER LIMIT

if len(sys.argv) >= 4:
    if sys.argv[3] != 0:
        lower_limit = int(sys.argv[3])
    else:
        lower_limit = 0

    # > 0 -> Limit input df to n glaciers (for debugging)
else:    
    lower_limit = 0

# UPPER LIMIT

if len(sys.argv) >= 5:
    if sys.argv[4] != 0:
        upper_limit = int(sys.argv[4])
    else:    
        upper_limit = n_glaciers

    # > 0 -> Limit input df to n glaciers (for debugging)
else:    
    upper_limit = n_glaciers

glacier_ids = glacier_ids[lower_limit:upper_limit]

# OUTPUT FILE 

output_file = re.sub('.h5$', str(lower_limit) +'-'+ str(upper_limit) +'-noWinterMax.h5', input_file)
# output_file = '/home/loibldav/Processing/topoCliF-GEE/raw/TSL-preproc-noWinterMax.h5'    

n_rows = df_TSL.shape[0]
n_cols = df_TSL.shape[1]

print('Success. Input dataset contains '+ str(n_rows) +' rows and '+ str(n_cols) +' cols')
print('Found data for '+ str(n_glaciers) +' glaciers')

if lower_limit > 0:
    if random_choice is True:
        n_choices = upper_limit - lower_limit
        glacier_ids = np.random.choice(glacier_ids, lower_limit)
        print('Selecting '+ str(lower_limit) +' random glaciers.')
    # else:
    #     print('Glacier ids befor '+ str(glacier_ids))
    #     glacier_ids = glacier_ids[lower_limit:upper_limit]        
    #     limit_range = upper_limit - lower_limit
    #     print('Limiting processing to '+ str(limit_range) +' glaciers.')

n_glaciers_limited = len(glacier_ids)


acc_max_ids = []
acc_max_dates = []
acc_maxima = pd.DataFrame({'RGI_ID' : [], 'winter_max_date': []})
n_runs = 0
n_total = len(glacier_ids)

for glacier_id in glacier_ids:
    print('Loop round '+ str(n_runs))
    years = []    
    acc_maxima = pd.DataFrame({'RGI_ID' : [], 'winter_max_date': []})
    
    drop_counter = 0
    data = df_TSL[df_TSL.RGI_ID == glacier_id].copy()
    data['data_index'] = data.LS_DATE
    data = data.set_index('data_index')
    
    data['year'] = pd.DatetimeIndex(data['LS_DATE']).year
    data['month'] = pd.DatetimeIndex(data['LS_DATE']).month
    years = data.year.unique()
    
    winter_max_abs = np.where((
        (data.month < abl_phase_begin) | 
        (data.month >abl_phase_end)) &
        (data.TSL_normalized > thres_wintermax))[0]
    if verbosity >= 1:
        progress = (n_runs + 1) / n_glaciers_limited * 100
        print('\n\nWorking on ' + str(glacier_id) + ' ['+ str(n_runs) +' of '+ str(n_glaciers_limited) +' (total '+ str(n_glaciers) +' in raw file) - '+ str(round(progress, 4)) +' %] ...')
    if verbosity >= 2:
        print(data.shape)
        print(data.size)            
    
    data.index = pd.to_datetime(data.LS_DATE, format='%Y-%m-%d')
    
    for year in years:
        
        year_subset = np.where(data.year == year)[0]          
        year_data = data.iloc[year_subset,:]

        ablphase_subset = np.where((year_data.month >= abl_phase_begin) & (year_data.month <= abl_phase_end))[0]
        accphase_subset = np.where((year_data.month < abl_phase_begin) | (year_data.month > abl_phase_end))[0]
                
        #max_idx = RG_series.groupby(RG_series.index.year)['SC_median'].transform(max) == RG_series['SC_median']
        if len(ablphase_subset) > 0:
            # 1. Find ablation period maximum
            ablphase_year_data = year_data.iloc[ablphase_subset,:]
            #print(str(ablphase_year_data.head()))
            yearly_abl_maximum = ablphase_year_data['SC_median'].max()
            yearly_abl_max_rel = ablphase_year_data['TSL_normalized'].max()

            if yearly_abl_max_rel < thres_relative_sla:
                if verbosity >= 2:
                    print('Relative ablation phase max for '+ str(year) +'('+ str(yearly_abl_max_rel) +') is < threshold ('+ str(thres_relative_sla) +'). Skipping winter max removal ...')        
            else:
                if verbosity >= 2:
                    print('Ablation phase max for '+ str(year) +' is '+ str(yearly_abl_maximum))        
                    
                # 2. Remove all accumulation period values > ablation period maximum
                if len(accphase_subset) > 0:
                
                    accphase_year_data = year_data.iloc[accphase_subset,:]
                    winter_maxima = np.where((accphase_year_data.SC_median >= yearly_abl_maximum))[0]
                    if len(winter_maxima) > 0:
                        # print(str(data.index))
                        if verbosity >= 2:
                            print('Found '+ str(len(winter_maxima)) +' maximum values in accumulation phase: '+ str(winter_maxima))
                        
                        # data_cleaned = data.copy()
                        # wm_dates = []
                        for winter_max in winter_maxima:
                            drop_ds = accphase_year_data.iloc[winter_max]
                            wm_date = drop_ds['LS_DATE']
                        
                            acc_max_ids.append(glacier_id)
                            acc_max_dates.append(wm_date)
                            if verbosity >= 2:
                                print('Dropping '+ str(wm_date) +' - '+ str(drop_ds['SC_median']))                    
                            
                            drop_counter += 1        
                        
                        # print(str(wm_dates))
                        #print(str(accphase_year_data.iloc[winter_maxima,:].SC_median))
                        
                        print('-> '+ str(data.index[winter_maxima]))                    
                            
                        # data.drop(data.index[winter_maxima], axis=0, inplace=True)                    
                        data = data[~data['LS_DATE'].isin(acc_max_dates)]
                        
                    
    
    
                if verbosity >= 1:
                    if drop_counter > 0:    
                        print('Removed '+ str(drop_counter) +' records.')
                    else:
                        print('No records with winter maxima found')
    
    # data_cleaned.drop(['year', 'month'], axis=1, inplace=True)
    # data_cleaned.to_csv(output_path +'/'+ filename, index=False)    
    
    if verbosity >= 2:
        print('\nWriting file '+ str(output_file) +' (' + str(data.shape[0]) +' rows and '+ str(data.shape[1]) +' cols)')
    data.to_hdf(output_file, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])
    n_runs += 1

# Append cleaned dataset to HDF file
# print('\nWriting output file. This may take a while ...')
# df_clean.to_hdf(output_file, key='TSLs', mode='w', format='table')    

acc_maxima = pd.DataFrame({'RGI_ID' : acc_max_ids, 'winter_max_date': acc_max_dates})
acc_maxima.to_hdf(output_file +'-max.h5', key='Winter_max', mode='a', format='table', data_columns=['RGI_ID'])

print('\nProcessing finished\n')
