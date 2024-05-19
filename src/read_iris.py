from pathlib import Path

import polars as pl


def read_iris(iris_path, not_filtered=False):

    iris_path = Path(iris_path)

    if not iris_path.exists():
        raise FileNotFoundError(f"Folder '{str(iris_path)}' does not exist. Please download the IRIS dump and place it in the 'data/' folder.")

    df_iris_master = pl.read_csv('data/iris-data-2024-03-14/ODS_L1_IR_ITEM_MASTER_ALL.csv', columns=['ITEM_ID', 'YEAR_PUBLISHED', 'TITLE'] ,dtypes={'ITEM_ID': pl.Int32, 'YEAR_PUBLISHED': pl.Utf8, 'TITLE': pl.Utf8})
    df_iris_identifier = pl.read_csv('data/iris-data-2024-03-14/ODS_L1_IR_ITEM_IDENTIFIER.csv', columns=['ITEM_ID', 'IDE_DOI', 'IDE_ISBN', 'IDE_PMID'] ,dtypes={'ITEM_ID': pl.Int32, 'IDE_DOI': pl.Utf8, 'IDE_ISBN': pl.Utf8, 'IDE_PMID': pl.Utf8})

    df = df_iris_identifier.join(df_iris_master, on='ITEM_ID', how='inner')

    if not_filtered:
        return df

    df_filtered = df.filter(pl.col('IDE_DOI').is_not_null() | pl.col('IDE_ISBN').is_not_null() | pl.col('IDE_PMID').is_not_null())[
        ['ITEM_ID', 'IDE_DOI', 'IDE_ISBN', 'IDE_PMID']
    ]

    return df_filtered