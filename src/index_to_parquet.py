import os
from zipfile import ZipFile

import argparse
from pathlib import Path

from tqdm import tqdm

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
ProgressBar().register()

from read_meta import get_omids_list


def process_index_dump(index_path):
    if os.path.isdir('data/iris_in_meta'):
        metaparquet_path = Path('data/iris_in_meta')
    else:
        raise FileNotFoundError('Please run meta_to_parquet.py first')

    # unzip the internal archives
    if index_path.endswith('.zip'):
        with ZipFile(index_path, 'r') as zip_ref:
            zip_ref.extractall(index_path)
        index_path = index_path.replace('.zip', '')

    file_names = [Path(index_path / archive) for archive in os.listdir(index_path)]

    omids_list = get_omids_list(metaparquet_path)

    for archive in tqdm(file_names):
        zip_file = ZipFile(archive)

        csvs = ['zip://'+n for n in zip_file.namelist() if n.endswith('.csv')]

        ddf = dd.read_csv(csvs, storage_options={'fo': zip_file.filename}, usecols=['id', 'citing', 'cited'])
        ddf = ddf[ddf['cited'].isin(omids_list) | ddf['citing'].isin(omids_list)]
        ddf.to_parquet(index_path + zip_file.filename + '.parquet', write_index=False)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process zip file containing OpenCitations Index CSV files")
    parser.add_argument("-index", "--index_dump", type=str, help="Path to the OpenCitations Index dump folder")

    args = parser.parse_args()
    process_index_dump(args.index_dump)
