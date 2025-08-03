from pathlib import Path
from zipfile import ZipFile

import polars as pl


class IRISDataset:
    def __init__(self, iris_path: str | Path):
        self.iris_path = Path(iris_path)
        print(f"Loading IRIS dataset from: {self.iris_path}")
        self.subfolder = (
            Path("POSTPROCESS-iris-data-2025-05-27")
            if "2025-05-30" in self.iris_path.name
            else Path("")
        )
        if not self.iris_path.exists():
            raise FileNotFoundError(
                f"Folder or file '{iris_path}' does not exist. "
                f"Please download the IRIS dump and place it in the 'data/' folder."
            )
        self._metadata_df = None

    def _read_csv_from_zip(self, filepath, columns=None, dtypes=None):
        with ZipFile(self.iris_path) as z:
            with z.open(str(filepath)) as f:
                return pl.read_csv(f, columns=columns, dtypes=dtypes, ignore_errors=True)

    def _read_csv_from_folder(self, filepath, columns=None, dtypes=None):
        return pl.read_csv(
            self.iris_path / self.subfolder / filepath,
            columns=columns,
            dtypes=dtypes,
            ignore_errors=True,
        )

    def _read_csv(self, filename, columns=None, dtypes=None):
        if self.iris_path.suffix == ".zip":
            return self._read_csv_from_zip(self.subfolder / filename, columns, dtypes)
        return self._read_csv_from_folder(filename, columns, dtypes)

    def read(self, not_filtered=False, no_id=False, metadata=False):
        df_master = self._read_csv(
            "ODS_L1_IR_ITEM_MASTER_ALL.csv",
            columns=[
                "ITEM_ID",
                "OWNING_COLLECTION",
                "OWNING_COLLECTION_DES",
                "DATE_ISSUED_YEAR",
            ],
        )
        df_identifier = self._read_csv(
            "ODS_L1_IR_ITEM_IDENTIFIER.csv",
            columns=["ITEM_ID", "IDE_DOI", "IDE_ISBN", "IDE_PMID"],
            dtypes={
                "ITEM_ID": pl.Int64,
                "IDE_DOI": pl.Utf8,
                "IDE_ISBN": pl.Utf8,
                "IDE_PMID": pl.Utf8,
            },
        )
        df = df_identifier.join(df_master, on="ITEM_ID", how="inner")

        if not_filtered:
            return df

        if metadata or no_id:
            for name, cols in {
                "ODS_L1_IR_ITEM_DESCRIPTION.csv": [
                    "ITEM_ID",
                    "DES_ALLPEOPLE",
                    "DES_NUMBEROFAUTHORS",
                ],
                "ODS_L1_IR_ITEM_MASTER_ALL.csv": ["ITEM_ID", "TITLE"],
                "ODS_L1_IR_ITEM_PUBLISHER.csv": [
                    "ITEM_ID",
                    "PUB_NAME",
                    "PUB_PLACE",
                    "PUB_COUNTRY",
                ],
                "ODS_L1_IR_ITEM_LANGUAGE.csv": ["ITEM_ID", "LAN_ISO"],
                "ODS_L1_IR_ITEM_RELATION.csv": [
                    "ITEM_ID",
                    "REL_ISPARTOFBOOK",
                    "REL_ISPARTOFJOURNAL",
                ],
            }.items():
                df = df.join(self._read_csv(name, columns=cols), on="ITEM_ID", how="left")

        if no_id:
            df = df.filter(
                pl.col("IDE_DOI").is_null()
                & pl.col("IDE_ISBN").is_null()
                & pl.col("IDE_PMID").is_null()
            )

            return df

        return df.filter(
            pl.col("IDE_DOI").is_not_null()
            | pl.col("IDE_ISBN").is_not_null()
            | pl.col("IDE_PMID").is_not_null()
        ).drop("OWNING_COLLECTION_DES")

    def get_metadata_df(self):
        if self._metadata_df is None:
            self._metadata_df = self.read(metadata=True).drop("IDE_DOI", "IDE_ISBN", "IDE_PMID")
        return self._metadata_df

    def _apply_heuristic(self, group, priority):
        # join metadata and compute null count
        group = group.join(
            self.get_metadata_df(), left_on="iris_id", right_on="ITEM_ID", how="inner"
        ).with_columns(
            pl.fold(acc=pl.lit(0), function=lambda acc, x: acc + x.is_null(), exprs=pl.all()).alias(
                "null_count"
            )
        )

        # sort by null_count within same iris_type + id and keep the first
        group = group.group_by(["iris_type", "id"], maintain_order=True).agg(
            pl.all().sort_by("null_count", descending=False).first()
        )

        # sorty by priority the remaining entities with the same id (but different type) and keep first
        group = group.group_by("id").map_groups(
            lambda g: g.sort(pl.col("iris_type").replace(priority, default=float("inf"))).head(1)
        )

        return group

    def _handle_duplicates(self, df, prefix, priority=None, exclude_type=None):
        filtered_df = df.filter(pl.col("id").str.starts_with(prefix))
        if exclude_type is not None:
            filtered_df = filtered_df.filter(pl.col("iris_type") != exclude_type)

        if priority:
            keep_df = filtered_df.group_by("id").map_groups(
                lambda group: self._apply_heuristic(group, priority)
            )
        else:
            keep_df = filtered_df.unique("id", keep="first", maintain_order=True)

        return filtered_df.join(keep_df, on="iris_id", how="anti").select("iris_id")

    def _filter_dois(self, df):
        return (
            df.select("ITEM_ID", "IDE_DOI", "OWNING_COLLECTION")
            .drop_nulls("IDE_DOI")
            .with_columns(
                (
                    "doi:"
                    + pl.col("IDE_DOI").str.extract(r"(10\.\d{4,}\/[^,\s;]*)").str.to_lowercase()
                ).alias("id")
            )
            .drop_nulls("id")
            .drop("IDE_DOI")
            .rename({"ITEM_ID": "iris_id"})
        )

    def _filter_pmids(self, df):
        return (
            df.select("ITEM_ID", "IDE_PMID", "OWNING_COLLECTION")
            .drop_nulls("IDE_PMID")
            .filter(~pl.col("IDE_PMID").str.contains("PMC"))
            .with_columns(
                (
                    "pmid:"
                    + pl.col("IDE_PMID").str.extract(r"0*([1-9][0-9]{1,8})", 1).str.to_lowercase()
                ).alias("id")
            )
            .drop_nulls("id")
            .drop("IDE_PMID")
            .rename({"ITEM_ID": "iris_id"})
        )

    def _filter_isbns(self, df):
        return (
            df.select("ITEM_ID", "IDE_ISBN", "OWNING_COLLECTION")
            .drop_nulls("IDE_ISBN")
            .with_columns(
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

    def get_pub_years(self):
        return self.read().select(
            pl.col("ITEM_ID").alias("ITEM_ID"),
            pl.col("DATE_ISSUED_YEAR").cast(pl.Int32, strict=False).alias("iris_pub_year"),
        )

    def get_pids(self, include_pub_year=False):
        df_filtered = self.read()

        dois = self._filter_dois(df_filtered)
        pmids = self._filter_pmids(df_filtered)
        isbns = self._filter_isbns(df_filtered)

        all_ids = pl.concat([dois, pmids, isbns]).rename({"OWNING_COLLECTION": "iris_type"})

        deduped_iris = all_ids.unique("iris_id", keep="first", maintain_order=True)
        duplicated_ids = (
            deduped_iris.filter(pl.col("id").is_duplicated())
            .sort("id")
            .with_columns(pl.col("iris_type"))
        )

        drop_doi = self._handle_duplicates(
            duplicated_ids, "doi:", priority={35: 1, 50: 2, 41: 3, 57: 4}
        )
        drop_pmid = self._handle_duplicates(duplicated_ids, "pmid:", priority={35: 1})
        drop_isbn = self._handle_duplicates(duplicated_ids, "isbn:", priority={50: 1, 49: 2, 35: 3})

        final_df = deduped_iris.join(
            pl.concat([drop_doi, drop_pmid, drop_isbn]), on="iris_id", how="anti"
        )

        if include_pub_year:
            final_df = final_df.join(
                self.get_pub_years(), left_on="iris_id", right_on="ITEM_ID", how="left"
            )

        return final_df

    def get_type_dict(self) -> dict:
        df = self.read(not_filtered=True)
        return dict(
            df[["OWNING_COLLECTION", "OWNING_COLLECTION_DES"]]
            .drop_nulls("OWNING_COLLECTION")
            .unique("OWNING_COLLECTION")
            .sort("OWNING_COLLECTION")
            .iter_rows()
        )
