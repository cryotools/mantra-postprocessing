#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Obtain the annual maximum ablation phase TSLA (SLA) for each glacier and year.
Write key metrics for each glacier and year to HDF5 file.

@author: David Loibl
"""

import pandas as pd
import numpy as np
import re
import os
import sys
from pathlib import Path
import datetime

verbosity       = 1         # Reporting level

abl_phase_begin = 7         # Month in which the ablation phase begins
abl_phase_end   = 10        # Month in which the abl. ph. ends (included)

sla_min_value   = 0.3       # Minimum TSL to be considered a possible SLA


start_time = datetime.datetime.now().timestamp()

if len(sys.argv) <= 1 or len(sys.argv) > 6:
    print("\nUsage: python maxima-per-glacier-and-year.py <input_file> <glacier_list_file> <lower_limit> <upper_limit> <output_path>\n")
    print("All arguments but <input_file> are optional and may be deactivated by setting them to 0\n")
    sys.exit(1)

i = 0
for i in range(len(sys.argv)):
    print('Arg '+ str(i) +' set to '+ str(sys.argv[i]))
    if sys.argv[i] == '0':
        sys.argv[i] = int(0)
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
        #glacier_list_file = re.sub('.h5$', '-glaciers.list', input_file)
        glacier_list_file = int(0)
else:    
    # glacier_list_file = re.sub('.h5$', '-glaciers.list', input_file)
    glacier_list_file = int(0)

# glacier_list_file = '/home/loibldav/Processing/topoCliF-GEE/raw/glaciers-HMA.list'

   

# OUTPUT PATH

if len(sys.argv) >= 6:
    if sys.argv[5] != 0:
        output_path = sys.argv[5]
        Path(output_path).mkdir(parents=True, exist_ok=True)
    else:    
        output_path = os.path.dirname(input_file)

    # > 0 -> Limit input df to n glaciers (for debugging)
else:    
    output_path = os.path.dirname(input_file)


exists = os.path.isfile(glacier_list_file)

if glacier_list_file != 0 and exists:
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
        print('Setting upper value to sys.argv[4]: '+ str(upper_limit))
    else:    
        upper_limit = n_glaciers
        print('Zero - Setting upper value to n_galciers: '+ str(n_glaciers))

    # > 0 -> Limit input df to n glaciers (for debugging)
else:    
    upper_limit = n_glaciers
    print('No arg - Setting upper value to n_galciers: '+ str(n_glaciers))

print('n_glaciers: '+ str(n_glaciers))
print('\nLowwer limit: '+ str(lower_limit) +', upper limit: '+ str(upper_limit))
glacier_ids = glacier_ids[lower_limit:upper_limit]

# OUTPUT FILE 

output_file = Path(input_file).stem
output_file = output_file +'-'+ str(lower_limit) +'-'+ str(upper_limit) +'-annual-maxima.h5'
# output_file = re.sub('.h5$', str(lower_limit) +'-'+ str(upper_limit) +'-annual-maxima.h5', input_file)
# output_file = '/home/loibldav/Processing/topoCliF-GEE/raw/TSL-preproc-noWinterMax.h5'    

n_rows = df_TSL.shape[0]
n_cols = df_TSL.shape[1]

print('Success. Input dataset contains '+ str(n_rows) +' rows and '+ str(n_cols) +' cols')
print('Found data for '+ str(n_glaciers) +' glaciers')

print('Filtering for ablation phase subset and normalized TSL threshold of '+ str(sla_min_value))
df_TSL.index = pd.to_datetime(df_TSL['LS_DATE'])

abl_phase_query = np.where((df_TSL.index.month >= abl_phase_begin) & 
                           (df_TSL.index.month <= abl_phase_end) &
                           (df_TSL['TSL_normalized'] > sla_min_value))[0]
    
df_TSL = df_TSL.iloc[abl_phase_query,:]

print('Filtered out '+ str(n_rows - df_TSL.shape[0]) +' rows ('+ str(df_TSL.shape[0]) +' remaining).')

'''
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
'''
n_glaciers_limited = len(glacier_ids)

'''
acc_max_ids = []
acc_max_dates = []
acc_maxima = pd.DataFrame({'RGI_ID' : [], 'winter_max_date': []})
'''
n_runs = 0
n_total = len(glacier_ids)


for glacier_id in glacier_ids:
    
    n_perc = np.around((n_runs + 1) / n_total * 100, decimals=3)
    print('\nProcessing glacier '+ str(glacier_id) +' ['+ str(n_runs + 1) +' of '+ str(n_total) +', '+ str(n_perc) +'%]')
    
    glacier = df_TSL[df_TSL.RGI_ID == glacier_id].copy()    
    glacier.index = pd.to_datetime(glacier['LS_DATE'])
    
    glacier_annual_maxima = glacier.loc[glacier.groupby(glacier.index.year)["SC_median"].idxmax()]
    
    keep_columns = ['RGI_ID', 'SC_median', 'TSL_normalized', 'LS_DATE', 'year', 'month']
    
    drop_columns = [i for i in glacier_annual_maxima.columns if i not in keep_columns] 
    
    # print('dropcols: '+ str(drop_columns))
    
    glacier_annual_maxima.drop(columns=drop_columns, inplace=True)
    
    if n_runs == 0:
        df_anmax = glacier_annual_maxima.copy()
    else:
        df_anmax = df_anmax.append(glacier_annual_maxima)
       
    if verbosity >= 2:
        print('glacier_annual_maxima'+ str(glacier_annual_maxima.head))
    '''
    if verbosity >= 2:
        print('\nWriting file '+ str(output_file) +' (' + str(glacier.shape[0]) +' rows and '+ str(glacier.shape[1]) +' cols)')
        print('to directory '+ str(output_path))
    
    # Append cleaned dataset to HDF file
    glacier_annual_maxima.to_hdf(output_path +'/'+ output_file, key='TSLs', mode='a', format='table', append=True, data_columns=['RGI_ID'])
    '''
    
    n_runs += 1

if verbosity >= 2:
    print('\nWriting file '+ str(output_file) +' (' + str(glacier.shape[0]) +' rows and '+ str(glacier.shape[1]) +' cols)')
    print('to directory '+ str(output_path))

# Append cleaned dataset to HDF file
df_anmax.to_hdf(output_path +'/'+ output_file, key='TSLs', mode='a', format='table', data_columns=['RGI_ID'])

end_time = datetime.datetime.now().timestamp()
proc_time = end_time - start_time

print('\nProcessing finished in '+ str(datetime.timedelta(seconds=proc_time)) +'\n')
