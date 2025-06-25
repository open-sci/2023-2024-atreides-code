import os
import shutil
from pathlib import Path
from zipfile import ZipFile
import glob
import logging

import dask.dataframe as dd
import polars as pl
from tqdm import tqdm
from tqdm.dask import TqdmCallback

from iris_in_meta import get_omids_list

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


DATA_DIR = Path("data")
IRIS_IN_META_DIR = DATA_DIR / "iris_in_meta"
IRIS_IN_INDEX_DIR = DATA_DIR / "iris_in_index"


def unzip_index_dump(index_path: Path) -> Path:
    if not index_path.exists():
        raise FileNotFoundError(f"Index file {index_path} not found.")

    if index_path.suffix == ".zip":
        extraction_dir = index_path.with_suffix("")
        logging.info(f"Unzipping {index_path} to {extraction_dir}")
        with ZipFile(index_path, "r") as zip_ref:
            zip_ref.extractall(extraction_dir)
        return extraction_dir

    return index_path


def read_and_filter_zip(archive_path: Path, omids_list: set) -> dd.DataFrame:
    with ZipFile(archive_path) as zip_file:
        csv_files = [
            f"zip://{name}" for name in zip_file.namelist() if name.endswith(".csv")
        ]
        if not csv_files:
            logging.warning(f"No CSV files found in {archive_path}")
            return dd.from_pandas(dd.DataFrame(), npartitions=1)

        ddf = dd.read_csv(
            csv_files,
            storage_options={"fo": zip_file.filename},
            usecols=["id", "citing", "cited", "creation"],
            dtype={"creation": "string"},
        )
        return ddf[(ddf["cited"].isin(omids_list)) | (ddf["citing"].isin(omids_list))]


def create_iris_in_index(index_path_str: str) -> None:
    if not IRIS_IN_META_DIR.exists():
        raise FileNotFoundError(
            f"Folder '{IRIS_IN_META_DIR}' does not exist. Please create the 'iris_in_meta' dataset first."
        )

    index_path = Path(index_path_str)
    index_dir = unzip_index_dump(index_path)

    archives = [index_dir / f for f in os.listdir(index_dir) if f.endswith(".zip")]
    omids_list = set(get_omids_list())

    IRIS_IN_INDEX_DIR.mkdir(parents=True, exist_ok=True)

    for archive in tqdm(archives, desc="Processing OC Index archives", leave=False):
        with TqdmCallback(desc=archive.stem):
            ddf_filtered = read_and_filter_zip(archive, omids_list)
            if not len(ddf_filtered.index) == 0:
                archive_output_dir = IRIS_IN_INDEX_DIR / archive.stem
                ddf_filtered.to_parquet(archive_output_dir, write_index=False)

    parquet_files = glob.glob(str(IRIS_IN_INDEX_DIR / "*" / "*.parquet"))
    if parquet_files:
        final_df = (
            pl.scan_parquet(parquet_files)
            .filter(~pl.col("creation").is_null())
            .with_columns(
                pl.col("creation")
                .str.extract(r"\d{4}", 0)
                .cast(pl.Int32)
                .alias("citing_year")
            )
            .filter(pl.col("citing_year") <= 2024)
        )

        final_df.sink_parquet(IRIS_IN_INDEX_DIR / "iris_in_index.parquet")

    for item in IRIS_IN_INDEX_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
