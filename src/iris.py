from pathlib import Path
from zipfile import ZipFile

import polars as pl


def _read_csv_from_zip(
    zip_path: Path, filepath: Path, columns=None, dtypes=None
) -> pl.DataFrame:
    with ZipFile(zip_path) as z:
        with z.open(str(filepath)) as f:
            return pl.read_csv(f, columns=columns, dtypes=dtypes, ignore_errors=True)


def _read_csv_from_folder(
    folder_path: Path, filepath: Path, columns=None, dtypes=None
) -> pl.DataFrame:
    return pl.read_csv(
        folder_path / filepath, columns=columns, dtypes=dtypes, ignore_errors=True
    )


def read_iris(iris_path, not_filtered=False, no_id=False) -> pl.DataFrame:
    iris_path = Path(iris_path)

    subfolder = (
        Path("POSTPROCESS-iris-data-2025-05-27")
        if "2025-05-30" in iris_path.name
        else Path("")
    )

    if not iris_path.exists():
        raise FileNotFoundError(
            f"Folder or file '{iris_path}' does not exist. Please download the IRIS dump and place it in the 'data/' folder."
        )

    def read_csv(filename, columns=None, dtypes=None):
        if iris_path.suffix == ".zip":
            return _read_csv_from_zip(
                iris_path, subfolder / filename, columns=columns, dtypes=dtypes
            )
        else:
            return _read_csv_from_folder(
                iris_path / subfolder, filename, columns=columns, dtypes=dtypes
            )

    df_iris_master = read_csv(
        "ODS_L1_IR_ITEM_MASTER_ALL.csv",
        columns=["ITEM_ID", "OWNING_COLLECTION", "OWNING_COLLECTION_DES"],
    )
    df_iris_identifier = read_csv(
        "ODS_L1_IR_ITEM_IDENTIFIER.csv",
        columns=["ITEM_ID", "IDE_DOI", "IDE_ISBN", "IDE_PMID"],
        dtypes={
            "ITEM_ID": pl.Int64,
            "IDE_DOI": pl.Utf8,
            "IDE_ISBN": pl.Utf8,
            "IDE_PMID": pl.Utf8,
        },
    )

    df = df_iris_identifier.join(df_iris_master, on="ITEM_ID", how="inner")

    if not_filtered:
        return df

    if no_id:
        df_iris_description = read_csv(
            "ODS_L1_IR_ITEM_DESCRIPTION.csv",
            columns=["ITEM_ID", "DES_ALLPEOPLE", "DES_NUMBEROFAUTHORS"],
        )
        df_iris_date_author = read_csv(
            "ODS_L1_IR_ITEM_MASTER_ALL.csv",
            columns=["ITEM_ID", "DATE_ISSUED_YEAR", "TITLE"],
        )
        df_iris_publisher = read_csv(
            "ODS_L1_IR_ITEM_PUBLISHER.csv",
            columns=["ITEM_ID", "PUB_NAME", "PUB_PLACE", "PUB_COUNTRY"],
        )
        df_iris_language = read_csv(
            "ODS_L1_IR_ITEM_LANGUAGE.csv", columns=["ITEM_ID", "LAN_ISO"]
        )

        noid_df = df.filter(
            pl.col("IDE_DOI").is_null()
            & pl.col("IDE_ISBN").is_null()
            & pl.col("IDE_PMID").is_null()
        )
        for join_df in [
            df_iris_description,
            df_iris_date_author,
            df_iris_publisher,
            df_iris_language,
        ]:
            noid_df = noid_df.join(join_df, on="ITEM_ID", how="left")

        return noid_df

    df_filtered = df.filter(
        pl.col("IDE_DOI").is_not_null()
        | pl.col("IDE_ISBN").is_not_null()
        | pl.col("IDE_PMID").is_not_null()
    ).drop("OWNING_COLLECTION_DES")

    return df_filtered


def apply_heuristic(group, priority):
    sorted_group = group.sort(
        pl.col("iris_type").replace(priority, default=float("inf"))
    )
    return sorted_group.head(1)


def handle_duplicates(df, prefix, priority=None, exclude_type=None):
    filtered_df = df.filter(pl.col("id").str.starts_with(prefix))

    if exclude_type is not None:
        keep_df = filtered_df.filter(pl.col("iris_type") != exclude_type)

    if priority:
        keep_df = filtered_df.group_by("id").map_groups(
            lambda group: apply_heuristic(group, priority)
        )
    else:
        keep_df = filtered_df.unique("id", keep="first", maintain_order=True)

    #if prefix == "pmid:":
        #keep_df = keep_df.filter(pl.col("iris_type") != 40)

    drop_df = filtered_df.join(keep_df, on="iris_id", how="anti").select("iris_id")

    return drop_df


def filter_dois(df) -> pl.DataFrame:
    """
    Filter and normalize DOIs from the IRIS DataFrame.
    """
    dois = df.select("ITEM_ID", "IDE_DOI", "OWNING_COLLECTION").drop_nulls("IDE_DOI")

    filtered_dois = (
        dois.with_columns(
            (
                "doi:"
                + pl.col("IDE_DOI")
                .str.extract(r"(10\.\d{4,}\/[^,\s;]*)")
                .str.to_lowercase()
            ).alias("id")
        )
        .drop_nulls("id")
        .drop("IDE_DOI")
        .rename({"ITEM_ID": "iris_id"})
    )

    return filtered_dois


def filter_pmids(df) -> pl.DataFrame:
    """
    Filter and normalize PMIDs from the IRIS DataFrame.
    """
    pmids = df.select("ITEM_ID", "IDE_PMID", "OWNING_COLLECTION").drop_nulls("IDE_PMID")

    filtered_pmids = (
        pmids.filter(~pl.col("IDE_PMID").str.contains("PMC"))
        .with_columns(
            (
                "pmid:"
                + pl.col("IDE_PMID")
                .str.extract(r"0*([1-9][0-9]{1,8})", 1)
                .str.to_lowercase()
            ).alias("id")
        )
        .drop_nulls("id")
        .drop("IDE_PMID")
        .rename({"ITEM_ID": "iris_id"})
    )

    return filtered_pmids


def filter_isbns(df) -> pl.DataFrame:
    """
    Filter and normalize ISBNs from the IRIS DataFrame.
    """
    isbns = df.select("ITEM_ID", "IDE_ISBN", "OWNING_COLLECTION").drop_nulls("IDE_ISBN")

    filtered_isbns = (
        isbns.with_columns(
            (
                "isbn:"
                + pl.col("IDE_ISBN")
                .str.extract_all(
                    r"(ISBN[-]*(1[03])*[ ]*(: ){0,1})*(([0-9Xx][- ]*){13}|([0-9Xx][- ]*){10})"
                )
                .list.first()
                .str.replace_all(r"[- ]", "")
                .str.to_lowercase()
            ).alias("id")
        )
        .drop_nulls("id")
        .drop("IDE_ISBN")
        .rename({"ITEM_ID": "iris_id"})
    )

    return filtered_isbns


def get_iris_pids(iris_path) -> pl.DataFrame:
    df_filtered = read_iris(iris_path)

    filtered_dois = filter_dois(df_filtered)
    filtered_pmids = filter_pmids(df_filtered)
    filtered_isbns = filter_isbns(df_filtered)

    dois_pmids_isbns_list = pl.concat(
        [filtered_dois, filtered_pmids, filtered_isbns]
    ).rename({"OWNING_COLLECTION": "iris_type"})

    dois_pmids_isbns_filtered = dois_pmids_isbns_list.unique(
        "iris_id", keep="first", maintain_order=True
    )
    dpi_dupes_id = (
        dois_pmids_isbns_filtered.filter(pl.col("id").is_duplicated())
        .sort("id")
        .with_columns(pl.col("iris_type"))
    )#.replace(type_dict))

    doi_priority = {35: 1, 50: 2, 41: 3, 57: 4}
    pmid_priority = {35: 1}
    isbn_priority = {49: 1, 35: 2}

    drop_doi = handle_duplicates(dpi_dupes_id, "doi:", priority=doi_priority)
    drop_pmid = handle_duplicates(dpi_dupes_id, "pmid:", priority=pmid_priority)
    drop_isbn = handle_duplicates(dpi_dupes_id, "isbn:", priority=isbn_priority)

    all_drops = pl.concat([drop_doi, drop_pmid, drop_isbn])
    final_filtered_df = dois_pmids_isbns_filtered.join(
        all_drops, on="iris_id", how="anti"
    )

    return final_filtered_df


def get_iris_type_dict(iris_path):
    iris_df = read_iris(iris_path, not_filtered=True)
    type_df = (
        iris_df[["OWNING_COLLECTION", "OWNING_COLLECTION_DES"]]
        .drop_nulls("OWNING_COLLECTION")
        .unique("OWNING_COLLECTION")
    )
    type_dict = dict(type_df.sort(pl.col("OWNING_COLLECTION")).iter_rows())

    return type_dict
