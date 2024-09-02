from pathlib import Path
from zipfile import ZipFile

import polars as pl

def read_iris(iris_path, not_filtered=False, no_id=False):
    iris_path = Path(iris_path)
    
    if not iris_path.exists():
        raise FileNotFoundError(f"Folder '{str(iris_path)}' does not exist. Please download the IRIS dump and place it in the 'data/' folder.")

    if str(iris_path).endswith('.zip'):
        df_iris_master = pl.read_csv(ZipFile(iris_path).open("ODS_L1_IR_ITEM_MASTER_ALL.csv").read(), columns=['ITEM_ID', 'OWNING_COLLECTION', 'OWNING_COLLECTION_DES'])
        df_iris_identifier = pl.read_csv(ZipFile(iris_path).open("ODS_L1_IR_ITEM_IDENTIFIER.csv").read(), columns=['ITEM_ID', 'IDE_DOI', 'IDE_ISBN', 'IDE_PMID'], dtypes={'ITEM_ID': pl.Int64, 'IDE_DOI': pl.Utf8, 'IDE_ISBN': pl.Utf8, 'IDE_PMID': pl.Utf8})
    else:
        df_iris_master = pl.read_csv(iris_path / 'ODS_L1_IR_ITEM_MASTER_ALL.csv', columns=['ITEM_ID', 'OWNING_COLLECTION', 'OWNING_COLLECTION_DES'])
        df_iris_identifier = pl.read_csv(iris_path / 'ODS_L1_IR_ITEM_IDENTIFIER.csv', columns=['ITEM_ID', 'IDE_DOI', 'IDE_ISBN', 'IDE_PMID'], dtypes={'ITEM_ID': pl.Int64, 'IDE_DOI': pl.Utf8, 'IDE_ISBN': pl.Utf8, 'IDE_PMID': pl.Utf8})

    df = df_iris_identifier.join(df_iris_master, on='ITEM_ID', how='inner')

    if not_filtered:
        return df
    
    if no_id:
        if str(iris_path).endswith('.zip'):
            df_iris_description = pl.read_csv(ZipFile(iris_path).open('ODS_L1_IR_ITEM_DESCRIPTION.csv').read(), columns=['ITEM_ID', 'DES_ALLPEOPLE', 'DES_NUMBEROFAUTHORS'])
            df_iris_publisher = pl.read_csv(ZipFile(iris_path).open('ODS_L1_IR_ITEM_PUBLISHER.csv').read(), columns=['ITEM_ID', 'PUB_NAME', 'PUB_PLACE', 'PUB_COUNTRY'])
            df_iris_language = pl.read_csv(ZipFile(iris_path).open('ODS_L1_IR_ITEM_LANGUAGE.csv').read(), columns=['ITEM_ID', 'LAN_ISO'])
        else:
            df_iris_description = pl.read_csv(iris_path / 'ODS_L1_IR_ITEM_DESCRIPTION.csv', columns=['ITEM_ID', 'DES_ALLPEOPLE', 'DES_NUMBEROFAUTHORS'])
            df_iris_publisher = pl.read_csv(iris_path / 'ODS_L1_IR_ITEM_PUBLISHER.csv', columns=['ITEM_ID', 'PUB_NAME', 'PUB_PLACE', 'PUB_COUNTRY'])
            df_iris_language = pl.read_csv(iris_path / 'ODS_L1_IR_ITEM_LANGUAGE.csv', columns=['ITEM_ID', 'LAN_ISO'])
            
        noid_df = df.filter(pl.col('IDE_DOI').is_null() & pl.col('IDE_ISBN').is_null() & pl.col('IDE_PMID').is_null())
        noid_df = noid_df.join(df_iris_description, on='ITEM_ID', how='left')
        noid_df = noid_df.join(df_iris_publisher, on='ITEM_ID', how='left')
        noid_df = noid_df.join(df_iris_language, on='ITEM_ID', how='left')

        return noid_df

    df_filtered = df.filter(pl.col('IDE_DOI').is_not_null() | pl.col('IDE_ISBN').is_not_null() | pl.col('IDE_PMID').is_not_null()).drop('OWNING_COLLECTION_DES')

    return df_filtered


def get_iris_type_dict(iris_path):
    iris_df = read_iris(iris_path, not_filtered=True)
    type_df = iris_df[['OWNING_COLLECTION', 'OWNING_COLLECTION_DES']].drop_nulls('OWNING_COLLECTION').unique('OWNING_COLLECTION')
    type_dict = dict(type_df.sort(pl.col('OWNING_COLLECTION')).iter_rows())

    return type_dict