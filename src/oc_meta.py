import os
import tempfile
from zipfile import ZipFile
import tarfile
from requests import get
from urllib.error import HTTPError

from pathlib import Path
from tqdm import tqdm

import polars as pl
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed
from dotenv import load_dotenv

from iris import read_iris, get_iris_type_dict, get_iris_pids


def get_type(doi, apikey):
    HTTP_HEADERS = {"authorization": apikey}
    API_CALL = "https://w3id.org/oc/meta/api/v1/metadata/{}"

    response = get(API_CALL.format("doi:" + doi), headers=HTTP_HEADERS)

    try:
        return response.json()[0]["type"]
    except IndexError:
        return None


def search_for_titles(iris_path):
    output_dir = "data/iris_in_meta"
    os.makedirs(output_dir, exist_ok=True)
    load_dotenv()
    OC_APIKEY = os.getenv("OC_APIKEY")

    df = read_iris(iris_path, not_filtered=True)

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

    for iris_id, title in tqdm(
        iris_noid_titles.iter_rows(), total=len(iris_noid_titles)
    ):
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
                    type = get_type(doi, OC_APIKEY)
                    if type:
                        findings.append(
                            {
                                "title": title,
                                "omid": entity.replace(
                                    "https://w3id.org/oc/meta/", "omid:"
                                ),
                                "id": "doi:" + doi,
                                "type": type,
                                "iris_id": iris_id,
                            }
                        )

        except (QueryBadFormed, HTTPError):
            continue

    titles_df = pl.DataFrame(findings)

    titles_df.write_parquet(os.path.join(output_dir, "titles_noid.parquet"))


def process_meta(meta_path, iris_path):
    if meta_path.endswith(".zip"):
        process_meta_zip(meta_path, iris_path)
    elif meta_path.endswith(".tar"):
        process_meta_tar(meta_path, iris_path)


def process_meta_tar(tar_path, iris_path):
    output_iim = Path("data/iris_in_meta")
    output_iim.mkdir(parents=True, exist_ok=True)

    dois_isbns_pmids_lf = get_iris_pids(iris_path).lazy()

    preference = pl.LazyFrame(
        {
            "type": ["journal article", "book chapter", "book chapter"],
            "iris_type": [35, 41, 42],
            "preference": [0, 1, 2],
        }
    )

    pbar = tqdm(desc="Processing OCMETA CSV files")

    with tarfile.open(tar_path, "r:*") as tar:
        while True:
            csv_member = tar.next()
            if csv_member is None:
                break
            if csv_member.isfile() and csv_member.name.endswith(".csv"):
                pbar.update(1)
                member_df = (
                    pl.scan_csv(
                        tar.extractfile(csv_member),
                        schema_overrides={"pub_date": pl.String},
                    )
                    .select(["id", "title", "type", "pub_date"])
                    .with_columns(
                        (pl.col("id").str.extract(r"(omid:[^\s]+)")).alias("omid"),
                        (pl.col("id").str.extract(r"((?:doi):[^\s\"]+)")).alias("doi"),
                        (pl.col("id").str.extract(r"((?:pmid):[^\s\"]+)")).alias(
                            "pmid"
                        ),
                        (pl.col("id").str.extract(r"((?:isbn):[^\s\"]+)")).alias(
                            "isbn"
                        ),
                    )
                    .with_columns(
                        pl.coalesce(
                            [pl.col("doi"), pl.col("pmid"), pl.col("isbn")]
                        ).alias("id")
                    )
                    .drop(["doi", "pmid", "isbn"])
                    .drop_nulls("id")
                    .join(dois_isbns_pmids_lf, on="id", how="inner")
                    .collect()
                )

                if not member_df.is_empty():
                    member_df.write_parquet(
                        os.path.join(
                            output_iim,
                            os.path.basename(csv_member.name).replace(
                                ".csv", ".parquet"
                            ),
                        )
                    )

    (
        pl.scan_parquet(output_iim / "*.parquet")
        .join(preference, on=["type", "iris_type"], how="left")
        .sort("preference", descending=True, nulls_last=True)
        .group_by("id")
        .first()
        .drop("preference")
        .with_columns(pl.col("iris_type").replace_strict(get_iris_type_dict(iris_path)))
        .rename({"type": "meta_type"})
    ).sink_parquet(output_iim / "iris_in_meta.parquet")

    for file in os.listdir(output_iim):
        if file != "iris_in_meta.parquet":
            os.remove(os.path.join(output_iim, file))

    print(f"Iris In Meta saved to '{output_iim}/iris_in_meta.parquet'")


def process_meta_zip(zip_path, iris_path):
    zip_file = ZipFile(zip_path)
    files_list = [
        zipfile for zipfile in zip_file.namelist() if zipfile.endswith(".csv")
    ]

    output_iim = Path("data/iris_in_meta")
    output_iim.mkdir(parents=True, exist_ok=True)

    dois_isbns_pmids_lf = get_iris_pids(iris_path).lazy()

    preference = pl.LazyFrame(
        {
            "type": ["journal article", "book chapter", "book chapter"],
            "iris_type": [35, 41, 42],
            "preference": [0, 1, 2],
        }
    )

    for csv_file in tqdm(files_list, desc="Processing Meta CSV files"):
        with zip_file.open(csv_file, "r") as file:
            # Source: https://vdavez.com/2024/01/how-to-use-scan_csv-with-a-file-like-object-in-polars/
            with tempfile.NamedTemporaryFile() as tf:
                tf.write(file.read())
                tf.seek(0)
                os.makedirs(output_iim, exist_ok=True)
                df = (
                    pl.scan_csv(tf.name)
                    .select(["id", "title", "type"])  # 'pub_date'
                    .with_columns(
                        (pl.col("id").str.extract(r"(omid:[^\s]+)")).alias("omid"),
                        (pl.col("id").str.extract(r"((?:doi):[^\s\"]+)")).alias("doi"),
                        (pl.col("id").str.extract(r"((?:pmid):[^\s\"]+)")).alias(
                            "pmid"
                        ),
                        (pl.col("id").str.extract(r"((?:isbn):[^\s\"]+)")).alias(
                            "isbn"
                        ),
                    )
                    .with_columns(
                        pl.coalesce(
                            [pl.col("doi"), pl.col("pmid"), pl.col("isbn")]
                        ).alias("id")
                    )
                    .drop(["doi", "pmid", "isbn"])
                    .drop_nulls("id")
                    .join(dois_isbns_pmids_lf, on="id", how="inner")
                    .collect()
                )

            if not df.is_empty():
                df.write_parquet(
                    os.path.join(
                        output_iim,
                        os.path.basename(csv_file).replace(".csv", ".parquet"),
                    )
                )

    (
        pl.scan_parquet(output_iim / "*.parquet")
        .join(preference, on=["type", "iris_type"], how="left")
        .sort("preference", descending=True, nulls_last=True)
        .group_by("id")
        .first()
        .drop("preference")
        .with_columns(pl.col("iris_type").replace_strict(get_iris_type_dict(iris_path)))
        .rename({"type": "meta_type"})
    ).sink_parquet(output_iim / "iris_in_meta.parquet")

    for file in os.listdir(output_iim):
        if file != "iris_in_meta.parquet":
            os.remove(os.path.join(output_iim, file))

    print(f"Iris In Meta saved to '{output_iim}/iris_in_meta.parquet'")


def create_iris_not_in_meta(iris_path):
    iim_path = Path("data/iris_in_meta")
    if not iim_path.exists():
        raise FileNotFoundError(
            "Dataset 'Iris in Meta' not found in the 'data/' folder. "
            "Please create the 'iris_in_meta' dataset first."
        )

    output_inim = Path("data/iris_not_in_meta")
    output_inim.mkdir(parents=True, exist_ok=True)

    dois_isbns_pmids_lf = get_iris_pids(iris_path).lazy()

    lf_iim = pl.scan_parquet(iim_path / "*.parquet")

    inim = dois_isbns_pmids_lf.lazy().join(lf_iim, on="iris_id", how="anti").collect()

    inim.write_parquet(output_inim / "iris_not_in_meta.parquet")

    print(f"Iris Not In Meta saved to '{output_inim}/iris_not_in_meta.parquet'")


def create_iris_noid(iris_path):
    output_inoid = Path("data/iris_no_id")
    output_inoid.mkdir(parents=True, exist_ok=True)

    iris_noid = read_iris(iris_path, no_id=True)

    iris_noid.write_parquet(output_inoid / "iris_no_id.parquet")

    print(f"Iris No ID saved to '{output_inoid}/iris_no_id.parquet'")
