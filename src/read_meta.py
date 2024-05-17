from pathlib import Path

import polars as pl


def read_meta(meta_path):
    metaparquet_files = meta_path / '*.parquet'
    lf_iim = pl.scan_parquet(metaparquet_files)

    return lf_iim


def get_omids_list(metaparquet_path):
    omids_list = (
        read_meta(metaparquet_path)
        .select('omid')
        .collect()
    )['omid'].to_list()

    return omids_list
