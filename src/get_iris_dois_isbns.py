import polars as pl

from src.read_iris import read_iris


def get_iris_dois_pmids_isbns(iris_path) -> pl.DataFrame:
    df_filtered = read_iris(iris_path)

    #filter and normalize the dois
    dois = df_filtered.select('ITEM_ID', 'IDE_DOI', 'OWNING_COLLECTION').drop_nulls('IDE_DOI')

    filtered_dois = (
        dois
        .with_columns(('doi:'+pl.col('IDE_DOI').str.extract(r'(10\.\d{4,}\/[^,\s;]*)').str.to_lowercase()).alias('id'))
        .drop_nulls('id')
        .drop('IDE_DOI')
        .rename({'ITEM_ID': 'iris_id'})
    )

    #filter and normalize the pmids
    pmids = df_filtered.select('ITEM_ID', 'IDE_PMID', 'OWNING_COLLECTION').drop_nulls('IDE_PMID').unique()

    filtered_pmids = (
        pmids
        .filter(
            ~pl.col('IDE_PMID').str.contains('PMC')
            )
        .with_columns(('pmid:'+pl.col('IDE_PMID').str.extract(r'0*([1-9][0-9]{1,8})', 1).str.to_lowercase()).alias('id'))
        .drop('IDE_PMID')
        .rename({'ITEM_ID': 'iris_id'})
    )

    #filter and normalize the isbns
    isbns = df_filtered.select('ITEM_ID', 'IDE_ISBN', 'OWNING_COLLECTION').drop_nulls('IDE_ISBN')

    filtered_isbns = (
        isbns
        .with_columns(
            ('isbn:'+pl.col('IDE_ISBN').str.extract_all(r'(ISBN[-]*(1[03])*[ ]*(: ){0,1})*(([0-9Xx][- ]*){13}|([0-9Xx][- ]*){10})').list.first().str.replace_all(r'[- ]', '').str.to_lowercase()).alias('id')
        )
        .drop_nulls('id')
        .drop('IDE_ISBN')
        .rename({'ITEM_ID': 'iris_id'})
    )


    dois_pmids_isbns_list = pl.concat([filtered_dois, filtered_pmids, filtered_isbns]).rename({'OWNING_COLLECTION': 'iris_type'})


    dois_pmids_isbns_filtered = dois_pmids_isbns_list.unique('iris_id', keep='first', maintain_order=True)
    dpi_dupes_id = dois_pmids_isbns_filtered.filter(pl.col("id").is_duplicated()).sort("id").with_columns(pl.col('iris_type'))#.replace(type_dict))

    doi_priority = {35: 1, 50: 2, 41: 3, 57: 4}
    isbn_priority = {49: 1, 35: 2}

    def apply_heuristic(group, priority):
        sorted_group = group.sort(pl.col("iris_type").replace(priority, default=float('inf')))
        return sorted_group.head(1)

    def handle_duplicates(df, prefix, priority=None, exclude_type=None):
        filtered_df = df.filter(pl.col('id').str.starts_with(prefix))
        
        if exclude_type is not None:
            keep_df = filtered_df.filter(pl.col('iris_type') != exclude_type)
        
        if priority:
            keep_df = filtered_df.group_by("id").map_groups(
                lambda group: apply_heuristic(group, priority)
            )
        else:
            keep_df = filtered_df.unique('id', keep='first', maintain_order=True)

        drop_df = filtered_df.join(keep_df, on='iris_id', how='anti').select('iris_id')
        
        return drop_df


    drop_doi = handle_duplicates(dpi_dupes_id, 'doi:', priority=doi_priority)
    drop_pmid = handle_duplicates(dpi_dupes_id, 'pmid:', exclude_type=40)
    drop_isbn = handle_duplicates(dpi_dupes_id, 'isbn:', priority=isbn_priority)

    all_drops = pl.concat([drop_doi, drop_pmid, drop_isbn])
    final_filtered_df = dois_pmids_isbns_filtered.join(all_drops, on='iris_id', how='anti')

    return final_filtered_df