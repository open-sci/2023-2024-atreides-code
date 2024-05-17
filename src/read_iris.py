import polars as pl


def read_iris():
    df_iris_master = pl.read_csv('data/iris-data-2024-03-14/ODS_L1_IR_ITEM_MASTER_ALL.csv', columns=['ITEM_ID', 'IDE_DOI', 'IDE_ISBN'] ,dtypes={'ITEM_ID': pl.Int32, 'IDE_DOI': pl.Utf8, 'IDE_ISBN': pl.Utf8})
    df_iris_identifier = pl.read_csv('data/iris-data-2024-03-14/ODS_L1_IR_ITEM_IDENTIFIER.csv', columns=['ITEM_ID', 'IDE_DOI', 'IDE_ISBN'] ,dtypes={'ITEM_ID': pl.Int32, 'IDE_DOI': pl.Utf8, 'IDE_ISBN': pl.Utf8})

    df = df_iris_identifier.join(df_iris_master, on='ITEM_ID', how='inner')

    df_filtered = df.filter(pl.col('IDE_DOI').is_not_null() | pl.col('IDE_ISBN').is_not_null())[
        ['ITEM_ID', 'IDE_DOI', 'IDE_ISBN']
    ]

    return df_filtered