from pathlib import Path

import polars as pl


def read_meta(meta_path):
    if not Path(meta_path).exists():
        raise FileNotFoundError(f"Folder '{str(meta_path)}' does not exist. Please run the 'meta_to_parquet.py' script first")

    metaparquet_files = Path(meta_path) / '*.parquet'
    lf_iim = pl.scan_parquet(metaparquet_files)

    return lf_iim


def get_omids_list(metaparquet_path):
    omids_list = (
        read_meta(metaparquet_path)
        .select('omid')
        .collect()
    )['omid'].to_list()

    return omids_list
