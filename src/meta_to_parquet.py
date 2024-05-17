import os
import tempfile
from zipfile import ZipFile

import argparse
from tqdm import tqdm
import polars as pl

from src.get_iris_dois_isbns import get_iris_dois_isbns


def process_meta_zip(zip_path, no_id=False):
    zip_file = ZipFile(zip_path)
    files_list = [zipfile for zipfile in zip_file.namelist() if zipfile.endswith('.csv')]
    output_dir = "data/iris_in_meta"
    output_dir2 = "data/iris_in_meta_no_id"

    dois_isbns_pmids = get_iris_dois_isbns()

    for csv_file in tqdm(files_list):
        with zip_file.open(csv_file, 'r') as file:
            # kudos to https://vdavez.com/2024/01/how-to-use-scan_csv-with-a-file-like-object-in-polars/
            with tempfile.NamedTemporaryFile() as tf:
                tf.write(file.read())
                tf.seek(0)
                os.makedirs(output_dir, exist_ok=True)
                os.makedirs(output_dir2, exist_ok=True)
                df = (
                    pl.scan_csv(tf.name)
                    .select(['id', 'title', 'type'])
                    .with_columns(
                        (pl.col('id').str.extract(r"((?:doi):[^\s\"]+)")).alias('doi'),
                        (pl.col('id').str.extract(r"((?:pmid):[^\s\"]+)")).alias('pmid'),
                        (pl.col('id').str.extract(r"((?:isbn):[^\s\"]+)")).alias('isbn'),
                    )
                    .with_columns(
                        pl.coalesce([pl.col('doi'), pl.col('isbn'), pl.col('pmid')]).alias('id')
                    )
                    .drop(['doi', 'isbn', 'pmid'])
                )

                lf_im = (
                    df
                    .drop_nulls('id')
                    .filter(
                        pl.col("id").is_in(dois_isbns_pmids)
                    )
                    .collect(streaming=True)
                )

                if not lf_im.is_empty():
                    lf_im.write_parquet(os.path.join(output_dir, os.path.basename(csv_file).replace('.csv', '.parquet')))

                
                if no_id:
                    lf_im_null = (
                        df
                        .filter(
                            pl.col("id").is_null()
                        )
                        .collect(streaming=True)
                    )


                    if not lf_im_null.is_empty():
                        lf_im_null.write_parquet(os.path.join(output_dir2, os.path.basename(csv_file).replace('.csv', '.parquet')))



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process zip file containing OpenCitations Meta CSV files")
    parser.add_argument("zip_location", type=str, help="Path to the zip file")
    args = parser.parse_args()
    process_meta_zip(args.zip_location)
