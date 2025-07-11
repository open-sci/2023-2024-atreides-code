from pathlib import Path

import polars as pl


def read_iris_in_meta():
    iim_path = Path("data/iris_in_meta")
    if not iim_path.exists():
        raise FileNotFoundError(
            f"Folder '{iim_path}' does not exist. Please create the 'iris_in_meta' dataset first."
        )
    lf_iim = pl.scan_parquet(str(iim_path / "*.parquet"))
    return lf_iim


def get_omids_list():
    return read_iris_in_meta().select("omid").collect().get_column("omid").to_list()
