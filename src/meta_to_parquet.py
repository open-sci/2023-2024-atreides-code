import os
import tempfile
from zipfile import ZipFile

import argparse
from tqdm import tqdm
import polars as pl
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

from get_iris_dois_isbns import get_iris_dois_pmids_isbns
from read_iris import read_iris


def search_for_titles():
    output_dir = "data/iris_in_meta"
    os.makedirs(output_dir, exist_ok=True)

    sparql = SPARQLWrapper("https://test.opencitations.net/meta/sparql")

    df = read_iris(not_filtered=True)

    iris_noid_titles = (
        df[['IDE_DOI', 'IDE_ISBN', 'IDE_PMID', 'TITLE']]
        .filter(
            pl.col('IDE_DOI').is_null() & pl.col(
                'IDE_ISBN').is_null() & pl.col('IDE_PMID').is_null()
        )
    )['TITLE'].to_list()

    iris_noid_titles_clean = [title.replace('"', "'") for title in iris_noid_titles if title is not None and len(title.split()) > 3]

    findings = []

    for title in tqdm(iris_noid_titles_clean[140:]):
        title = title.replace('\r', ' ').replace('\n', '')
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
                print(results["results"]["bindings"])
                for result in results["results"]["bindings"]:
                    entity = result["entity"]["value"]
                    doi = result["doi"]["value"]
                    type = result["type"]["value"]
                    findings.append({'title': title, 'entity': entity.replace("https://w3id.org/oc/meta/", 'omid:'), 'doi': "doi:"+doi, 'type': type})

        except QueryBadFormed as e:
            print(f"Error querying title '{title}': {e}")


def process_meta_zip(zip_path, search_for_titles=False):
    zip_file = ZipFile(zip_path)
    files_list = [zipfile for zipfile in zip_file.namelist() if zipfile.endswith('.csv')]
    output_dir = "data/iris_in_meta"

    dois_isbns_pmids_lf = get_iris_dois_pmids_isbns().lazy()

    for csv_file in tqdm(files_list, desc="Processing Meta CSV files"):
        with zip_file.open(csv_file, 'r') as file:
            # kudos to https://vdavez.com/2024/01/how-to-use-scan_csv-with-a-file-like-object-in-polars/
            with tempfile.NamedTemporaryFile() as tf:
                tf.write(file.read())
                tf.seek(0)
                os.makedirs(output_dir, exist_ok=True)
                df = (
                    pl.scan_csv(tf.name)
                    .select(['id', 'title', 'type'])
                    .with_columns(
                        (pl.col('id').str.extract(r"(omid:[^\s]+)")).alias('omid'),
                        (pl.col('id').str.extract(r"((?:doi):[^\s\"]+)")).alias('doi'),
                        (pl.col('id').str.extract(r"((?:pmid):[^\s\"]+)")).alias('pmid'),
                        (pl.col('id').str.extract(r"((?:isbn):[^\s\"]+)")).alias('isbn'),
                    )
                    .with_columns(
                        pl.coalesce([pl.col('doi'), pl.col('isbn'), pl.col('pmid')]).alias('id')
                    )
                    .drop(['doi', 'pmid', 'isbn'])
                    .drop_nulls('id')
                    .join(dois_isbns_pmids_lf, on='id', how='inner')
                    .collect(streaming=True)
                )

                if not df.is_empty():
                    df.write_parquet(os.path.join(output_dir, os.path.basename(csv_file).replace('.csv', '.parquet')))
                


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process zip file containing OpenCitations Meta CSV files")
    parser.add_argument("meta_zip_location", type=str, help="Path to the zip file")
    parser.add_argument("--search_for_titles", type=bool, default=False, help="Search for the entities without an id in IRIS by their title in Meta. WARNING: this will take ~3 and a half hours to complete.")
    args = parser.parse_args()
    process_meta_zip(args.meta_zip_location, args.search_for_titles)
