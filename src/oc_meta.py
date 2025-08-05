import io
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, Union
from urllib.error import HTTPError
from zipfile import ZipFile

import polars as pl
from dotenv import load_dotenv
from requests import get
from SPARQLWrapper import JSON, SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed
from tqdm import tqdm

from src.iris import IRISDataset

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


DATA_DIR = Path("data")
IRIS_IN_META_DIR = DATA_DIR / "iris_in_meta"
IRIS_NOT_IN_META_DIR = DATA_DIR / "iris_not_in_meta"
IRIS_NO_ID_DIR = DATA_DIR / "iris_no_id"


def get_publication_type(doi, apikey):
    HTTP_HEADERS = {"authorization": apikey}
    API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/{}"

    response = get(API_CALL.format("doi:" + doi), headers=HTTP_HEADERS, timeout=10)

    try:
        return response.json()[0]["type"]
    except IndexError:
        return None


def search_for_titles(iris_path):
    output_dir = "data/iris_in_meta"
    os.makedirs(output_dir, exist_ok=True)
    load_dotenv()
    OC_APIKEY = os.getenv("OC_APIKEY")

    iris = IRISDataset(iris_path)

    df = iris.read(not_filtered=True)

    iris_noid_titles = (
        df.select("ITEM_ID", "IDE_DOI", "IDE_ISBN", "IDE_PMID", "TITLE")
        .filter(
            (
                pl.col("IDE_DOI").is_null()
                & pl.col("IDE_ISBN").is_null()
                & pl.col("IDE_PMID").is_null()
            ),
        )
        .drop("IDE_DOI", "IDE_ISBN", "IDE_PMID")
    )

    sparql = SPARQLWrapper("https://opencitations.net/meta/sparql")

    findings = []

    for iris_id, title in tqdm(iris_noid_titles.iter_rows(), total=len(iris_noid_titles)):
        title = title.replace("\r", " ").replace("\n", "").replace('"', "'")
        if len(title.split()) < 3:
            continue
        try:
            sparql.setQuery(f"""
                            PREFIX datacite: <http://purl.org/spar/datacite/>
                            PREFIX dcterms: <http://purl.org/dc/terms/>
                            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
                            PREFIX fabio: <http://purl.org/spar/fabio/>
                            SELECT ?entity ?doi ?type
                            WHERE {{
                                ?entity dcterms:title "{title}" ;
                                    a ?type.
                                ?entity datacite:hasIdentifier ?identifier.
                                ?identifier datacite:usesIdentifierScheme datacite:doi.
                                ?identifier literal:hasLiteralValue ?doi.
                            FILTER (?type != fabio:Expression)
                            }}""")
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            if results["results"]["bindings"]:
                for result in results["results"]["bindings"]:
                    entity = result["entity"]["value"]
                    doi = result["doi"]["value"]
                    pub_type = get_publication_type(doi, OC_APIKEY)
                    if pub_type:
                        findings.append(
                            {
                                "title": title,
                                "omid": entity.replace("https://w3id.org/oc/meta/", "omid:"),
                                "id": "doi:" + doi,
                                "type": type,
                                "iris_id": iris_id,
                            }
                        )

        except (QueryBadFormed, HTTPError):
            continue

    titles_df = pl.DataFrame(findings)

    titles_df.write_parquet(os.path.join(output_dir, "titles_noid.parquet"))


def _process_chunk(
    file: Union[str, io.BytesIO], iris_pids_lf: pl.LazyFrame
) -> Optional[pl.DataFrame]:
    df = (
        pl.scan_csv(
            file,
            schema_overrides={"pub_date": pl.String},
        )
        .select(["id", "title", "type", "pub_date"])
        .with_columns(
            (pl.col("id").str.extract(r"(omid:[^\s]+)")).alias("omid"),
            (pl.col("id").str.extract(r"((?:doi):[^\s\"]+)")).alias("doi"),
            (pl.col("id").str.extract(r"((?:pmid):[^\s\"]+)")).alias("pmid"),
            (pl.col("id").str.extract(r"((?:isbn):[^\s\"]+)")).alias("isbn"),
        )
        .with_columns(pl.coalesce([pl.col("doi"), pl.col("pmid"), pl.col("isbn")]).alias("id"))
        .drop(["doi", "pmid", "isbn"])
        .drop_nulls("id")
        .join(iris_pids_lf, on="id", how="inner")
        .collect()
    )

    return df


def create_iris_in_meta(
    archive_path: str, iris_path: Path, year_cutoff: Optional[int] = None
) -> None:
    iris = IRISDataset(iris_path)
    IRIS_IN_META_DIR.mkdir(parents=True, exist_ok=True)
    temp_parquet_dir = IRIS_IN_META_DIR / "temp_chunks"
    temp_parquet_dir.mkdir(exist_ok=True)

    iris_pids_lf = iris.get_pids(include_pub_year=True).lazy()

    preference_lf = pl.LazyFrame(
        {
            "type": ["journal article", "book chapter", "book chapter"],
            "iris_type": [35, 41, 42],
            "preference": [0, 1, 2],
        }
    )

    if archive_path.endswith(".zip"):
        _process_zip_archive(archive_path, iris_pids_lf, temp_parquet_dir)
    elif archive_path.endswith((".tar", ".tar.gz", ".tar.bz2")):
        _process_tar_archive(archive_path, iris_pids_lf, temp_parquet_dir)
    else:
        raise ValueError("Unsupported archive format. Please use .zip or .tar.")

    final_lf = (
        pl.scan_parquet(temp_parquet_dir / "*.parquet")
        .join(preference_lf, on=["type", "iris_type"], how="left")
        .sort(["preference", "pub_date"], descending=True, nulls_last=True, maintain_order=True)
        .group_by("id")
        .first()
        .drop(["preference"])
        .with_columns(pl.col("iris_type").replace_strict(iris.get_type_dict()))
        .rename({"type": "meta_type"})
    )

    if year_cutoff is not None:
        logging.info("Applying year cutoff: citing_year <= %s", year_cutoff)
        final_lf.join(
            iris.get_pub_years().lazy(),
            left_on="iris_id",
            right_on="ITEM_ID",
            how="left",
        )

        final_lf = final_lf.with_columns(
            pl.when(pl.col("pub_date").is_null())
            .then(pl.col("iris_pub_year"))
            .otherwise(pl.col("pub_date"))
            .alias("pub_date")
        ).drop("iris_pub_year")

        final_lf = (
            final_lf.with_columns(
                pl.col("pub_date")
                .str.extract(r"(\d{4})", 1)
                .cast(pl.Int32, strict=False)
                .alias("pub_year")
            )
            .filter(pl.col("pub_year") <= year_cutoff)
            .drop("pub_year")
        )

    output_file = IRIS_IN_META_DIR / "iris_in_meta.parquet"
    final_lf.sink_parquet(output_file)
    logging.info("Processing complete")
    logging.info("Iris In Meta saved to '%s'", output_file)
    for file in temp_parquet_dir.iterdir():
        file.unlink()
    temp_parquet_dir.rmdir()


def _process_zip_archive(zip_path: str, iris_pids_lf: pl.LazyFrame, temp_dir: Path):
    with ZipFile(zip_path, "r") as archive:
        csv_files = [f for f in archive.namelist() if f.endswith(".csv")]
        for csv_file in tqdm(csv_files, desc="Processing Meta CSV files"):
            with archive.open(csv_file, "r") as file:
                with tempfile.NamedTemporaryFile() as tf:
                    tf.write(file.read())
                    tf.seek(0)
                    os.makedirs(temp_dir, exist_ok=True)
                    df = _process_chunk(tf.name, iris_pids_lf)

                    if not df.is_empty():
                        df.write_parquet(
                            temp_dir / f"{os.path.basename(csv_file).replace('.csv', '.parquet')}"
                        )


def _save_batches(batched_dfs: list[pl.DataFrame], batch_idx: int, temp_dir: Path):
    if batched_dfs:
        batch_df = pl.concat(batched_dfs)
        batch_file = temp_dir / f"batch_{batch_idx:04d}.parquet"
        batch_df.write_parquet(batch_file)


def _process_tar_archive(
    tar_path: str, iris_pids_lf: pl.LazyFrame, temp_dir: Path, batch_size: int = 1000
):
    with tarfile.open(tar_path, "r:*") as archive:
        csv_members = (
            member for member in archive if member.isfile() and member.name.endswith(".csv")
        )

        batched_dfs = []
        batch_idx = 0

        for csv_member in tqdm(csv_members, desc="Processing Meta CSV files"):
            with archive.extractfile(csv_member) as file:
                df = _process_chunk(file, iris_pids_lf)
                if df is not None and not df.is_empty():
                    batched_dfs.append(df)

                    if len(batched_dfs) >= batch_size:
                        _save_batches(batched_dfs, batch_idx, temp_dir)
                        batch_idx += 1
                        batched_dfs = []

        _save_batches(batched_dfs, batch_idx, temp_dir)


def create_iris_not_in_meta(iris_path: Path):
    iim_file = IRIS_IN_META_DIR / "iris_in_meta.parquet"
    if not iim_file.exists():
        raise FileNotFoundError(f"'{iim_file}' not found. Please run `process_meta_archive` first.")

    output_path = IRIS_NOT_IN_META_DIR / "iris_not_in_meta.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    iris = IRISDataset(iris_path)
    iris_pids_lf = iris.get_pids(iris_path).lazy()
    iim_lf = pl.scan_parquet(iim_file).select("iris_id")

    iris_not_in_meta_df = iris_pids_lf.join(iim_lf, on="iris_id", how="anti").collect()

    iris_not_in_meta_df.write_parquet(output_path)
    logging.info("Iris Not In Meta saved to '%s'", output_path)


def create_iris_noid(iris_path: Path):
    output_path = IRIS_NO_ID_DIR / "iris_no_id.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    iris = IRISDataset(iris_path)

    iris_noid_df = iris.read(no_id=True)

    iris_noid_df.write_parquet(output_path)
    logging.info("Iris No ID saved to '%s'", output_path)
