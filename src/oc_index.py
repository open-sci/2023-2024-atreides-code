import io
import logging
import shutil
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Optional, Set
from zipfile import BadZipFile, ZipFile

import pandas as pd
import polars as pl
from tqdm import tqdm

from src.iris_in_meta import get_omids_list

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DATA_DIR = Path("data")
IRIS_IN_META_DIR = DATA_DIR / "iris_in_meta"
IRIS_IN_INDEX_DIR = DATA_DIR / "iris_in_index"
TEMP_PARQUET_DIR = IRIS_IN_INDEX_DIR / "tmp"

N_PROCESSES = cpu_count()


def process_single_zip(args: tuple[Path, Path, Set[str]]) -> tuple[str, int]:
    zip_path, temp_output_dir, omids_set = args

    try:
        filtered_chunks = []
        with ZipFile(zip_path, "r") as zf:
            csv_files_in_zip = [name for name in zf.namelist() if name.endswith(".csv")]

            for csv_filename in csv_files_in_zip:
                try:
                    with zf.open(csv_filename, "r") as csv_file:
                        text_file = io.TextIOWrapper(csv_file, encoding="utf-8")
                        reader = pd.read_csv(
                            text_file,
                            chunksize=100_000,
                            usecols=["id", "citing", "cited", "creation"],
                            dtype={
                                "id": "string",
                                "citing": "string",
                                "cited": "string",
                                "creation": "string",
                            },
                            low_memory=False,
                        )
                        for chunk in reader:
                            mask = chunk["cited"].isin(omids_set) | chunk[
                                "citing"
                            ].isin(omids_set)
                            if mask.any():
                                filtered_chunks.append(chunk[mask])
                except Exception as e:
                    logging.warning(
                        f"Could not process CSV '{csv_filename}' in '{zip_path.name}': {e}"
                    )

        if not filtered_chunks:
            return (zip_path.name, 0)

        df_combined = pd.concat(filtered_chunks, ignore_index=True)
        output_path = temp_output_dir / f"{zip_path.stem}.parquet"
        df_combined.to_parquet(output_path, engine="pyarrow")
        return (zip_path.name, len(df_combined))

    except BadZipFile:
        logging.error(f"Could not open {zip_path.name}, it may be corrupted.")
        return (zip_path.name, -1)
    except Exception as e:
        logging.error(
            f"A critical error occurred while processing {zip_path.name}: {e}"
        )
        return (zip_path.name, -1)


def create_iris_in_index(
    index_path_str: str, year_cutoff: Optional[int] = None
) -> None:
    if not IRIS_IN_META_DIR.exists():
        raise FileNotFoundError(
            f"Folder '{IRIS_IN_META_DIR}' does not exist. Please create the 'iris_in_meta' dataset first."
        )

    index_path = Path(index_path_str)
    if not index_path.exists():
        raise FileNotFoundError(f"Index dump file/folder '{index_path}' not found.")

    if index_path.is_file() and index_path.suffix == ".zip":
        index_dir = index_path.with_suffix("")
        if not index_dir.exists():
            logging.info(f"Unzipping main index dump: {index_path} -> {index_dir}")
            with ZipFile(index_path, "r") as zip_ref:
                zip_ref.extractall(index_dir)
    else:
        index_dir = index_path

    IRIS_IN_INDEX_DIR.mkdir(parents=True, exist_ok=True)
    if TEMP_PARQUET_DIR.exists():
        shutil.rmtree(TEMP_PARQUET_DIR)
    TEMP_PARQUET_DIR.mkdir()

    omids_list_set = set(get_omids_list())
    archives = sorted(list(index_dir.glob("*.zip")))

    if not archives:
        logging.warning(f"No .zip archives found in '{index_dir}'. Aborting.")
        return

    logging.info(
        f"Found {len(archives)} archives. Starting parallel processing with {N_PROCESSES} workers."
    )

    tasks = [(archive, TEMP_PARQUET_DIR, omids_list_set) for archive in archives]

    with Pool(processes=N_PROCESSES) as pool:
        results = list(
            tqdm(
                pool.imap_unordered(process_single_zip, tasks),
                total=len(tasks),
                desc="Processing OC Index Archives",
            )
        )

    logging.info("Finished processing.")

    intermediate_files = list(TEMP_PARQUET_DIR.glob("*.parquet"))
    if not intermediate_files:
        logging.warning("No matching data found. No final file will be created.")
        return

    final_lf = pl.scan_parquet(intermediate_files)

    if year_cutoff is not None:
        logging.info(f"Applying year cutoff: citing_year <= {year_cutoff}")
        final_lf = (
            final_lf.filter(~pl.col("creation").is_null())
            .with_columns(
                pl.col("creation")
                .str.extract(r"(\d{4})", 1)
                .cast(pl.Int32, strict=False)
                .alias("citing_year")
            )
            .filter(pl.col("citing_year") <= year_cutoff)
        )

    final_output_path = IRIS_IN_INDEX_DIR / "iris_in_index.parquet"
    final_lf.sink_parquet(final_output_path)

    logging.info(f"Iris In Index saved to '{final_output_path}'")

    logging.info("Cleaning up temporary directory...")
    shutil.rmtree(TEMP_PARQUET_DIR)
