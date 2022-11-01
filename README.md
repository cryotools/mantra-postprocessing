# MANTRA Postprocessing scripts

This repository contains scripts to turn raw results from the ["MountAiN glacier Transient snowline Retrieval Algorithm"](https://github.com/cryotools/mantra) into analysis-ready data.

In case you have a Slurm-based HPC cluster at hand, you may use the Bash shell files to run scripts in parallel.

## Requirements

### Data

First, you will need to process some [MANTRA TSLA data](https://github.com/cryotools/mantra). An small example file is `./data/MANTRA/`.

Many of the scripts require a CSV file of Randolph Glacier Inventory (RGI) data to work. An example for High Mountain Asia is provided in `./data/RGI`. For other regions, you may [download the according shapefile](https://nsidc.org/data/nsidc-0770/versions/6), open it in your preferred GIS environment, and export the attribute table to CSV.

### Software

- Python 3 (a current version of [Anaconda](https://www.anaconda.com/) is recommended).
- Pandas and Numpy (will be included in Anaconda).

## Filter and clean raw GEE TSLA result data

### Step 1: Merge GEE files
Append all CSV files but make sure the header line is included only once:
```console
head -n 1 TSL-results-0001.csv > _TARGET_PATH_/TSL-results-merged.csv && tail -n+2 -q *.csv >> _TARGET_PATH_/TSL-results-merged.csv
```

### Step 2: Basic filtering and cleaning
Make sure `input_file` and `output_path` are set correctly in `tidy-raw-gee-tsl-csvs.py`. Then run it ...
```console
python tidy-raw-gee-tsl-csvs.py
```

### Step 3: Remove winter maxima
The script to remove winter maxima is designed to be run in parallel on a HPC cluster using the workload manager Slurm.
For convenience, you may want assign the path where the HDF5 file is located to a bash variable, here named `H5_PATH`.

```console
cd _PATH_TO_HDF5_FILE_
H5_PATH=$( pwd )

~/launch_rwm.sh $H5_PATH/TSLA-HMA-2020-07-filtered.h5 $H5_PATH/glaciers-HMA-full.list 500
```

### Step 4: Merge HDF5 files
Step 3 will result in a series of HDF5 files that need to be merged, subsequently.
In the directory containing the files, first create a file containing a list of all file names that should be merged, using e.g.
```console
ls *-noWinterMax.h5 > h5-files.list
```

And merge the HDF5 files by running:
```console
python merge-multiple-hdfs.py <input_dir> <output_file> <file_list>
```

## Optional scripts

### Append CSVs to existing HDF5

Edit and run `append_GEE_obs.py`. This is supposed to be used when you process additional MANTRA data and what to append it to an existing data set. For instance, you may want to add a new year to an existing set of TSLA timeseries.

### Append a preprocessed HDF5 to existing HDF5 file

Edit and run `append_preproc_TSLA_data.py`.


### Drop duplicates

Edit and run `drop_duplicates.py`. This is convenience script to ensure, well, that there are no duplicates in your dataset.

### Grid preprocessing

Edit and run `grid_preprocessing.py`. This will support to create a gridded dataset of TSLA anomalies, e.g. for EOF analyses.

### Obtain TSLA maxima per year and glacier

`python maxima-per-glacier-and-year.py <input_file> <glacier_list_file> <lower_limit> <upper_limit> <output_path>`

### Split big file to individual glaciers

Helper tool to convert a full TSL file into individual files per glacier.
```console
python plot-glacier-timeseries.py <tsl_file> <output_dir> <output_type>
```
With `tsl_file` being a TSL result file in HDF format, `output_dir` a valid directory to which the output will be written, and `output_type` either `csv` or `hdf5`.



# Citing MANTRA
If you publish work based on MANTRA, please cite as:

David Loibl (2022): MountAiN glacier Transient snowline Retrieval Algorithm (MANTRA) v0.8.2, doi: [10.5281/zenodo.7133644](https://doi.org/10.5281/zenodo.7133644)

# Acknoledgements
MANTRA was developed within the research project "TopoClimatic Forcing and non-linear dynamics in the climate change adaption of glaciers in High Asia" (TopoClif). TopoCliF and David Loibl's work within the project were funded by [DFG](https://gepris.dfg.de/gepris/projekt/356944332) under the ID LO 2285/1-1.

The development of filters identification of adequate thresholds was supported by [Inge Grünberg](https://orcid.org/0000-0002-5748-8102). Inge's contribution to this project was supported by [Geo.X](https://www.geo-x.net/), the research network for Geosciences in Berlin and Potsdam.
