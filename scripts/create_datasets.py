import argparse
import sys
from pathlib import Path

from src.oc_index import create_iris_in_index
from src.oc_meta import (
    create_iris_in_meta,
    create_iris_noid,
    create_iris_not_in_meta,
    search_for_titles,
)

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))


def main(parsed_args):
    if parsed_args.iris_in_meta:
        create_iris_in_meta(parsed_args.meta_path, parsed_args.iris_path)

    if parsed_args.iris_not_in_meta:
        create_iris_not_in_meta(parsed_args.iris_path)

    if parsed_args.iris_no_id:
        create_iris_noid(parsed_args.iris_path)

    if parsed_args.search_for_titles:
        search_for_titles(parsed_args.iris_path)

    if parsed_args.iris_in_index:
        if parsed_args.index_path is None:
            print(
                "Please provide the path to the OpenCitations Index dump folder by specifying the -index argument."
            )
            sys.exit(1)
        create_iris_in_index(parsed_args.index_path, parsed_args.year_cutoff)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process zip file containing OpenCitations Meta CSV files"
    )
    parser.add_argument(
        "-iris",
        "--iris_path",
        type=str,
        required=True,
        help="Path to the folder containing the IRIS CSV files",
    )
    parser.add_argument(
        "-meta",
        "--meta_path",
        type=str,
        required=True,
        help="Path to the zip file of the OpenCitations Meta dump",
    )
    parser.add_argument(
        "-index",
        "--index_path",
        type=str,
        help="Path to the OpenCitations Index dump folder",
    )

    parser.add_argument(
        "-iim",
        "--iris_in_meta",
        action="store_true",
        default=False,
        help="Create the Iris In Meta dataset containing all the entities with external IDs IRIS that are in Meta.",
    )
    parser.add_argument(
        "-iii",
        "--iris_in_index",
        action="store_true",
        default=False,
        help="Create the Iris In Index dataset containing all the entities with external IDs in IRIS that are in the OpenCitations Index.",
    )
    parser.add_argument(
        "-inim",
        "--iris_not_in_meta",
        action="store_true",
        default=False,
        help="Create the Iris Not In Meta dataset containing all the entities with external IDs IRIS that are not in Meta.",
    )
    parser.add_argument(
        "-inoid",
        "--iris_no_id",
        action="store_true",
        default=False,
        help="Create the Iris No ID dataset containing all the entities with no external IDs in IRIS.",
    )
    parser.add_argument(
        "-yc",
        "--year_cutoff",
        type=int,
        default=None,
        help="If not specified, all years will be included.",
    )
    parser.add_argument(
        "--search_for_titles",
        action="store_true",
        default=False,
        help="Search for the entities without an id in IRIS by their title in Meta. WARNING: this will take ~3 hours to complete.",
    )

    args = parser.parse_args()

    if not any(
        [
            args.iris_in_meta,
            args.iris_not_in_meta,
            args.iris_no_id,
            args.search_for_titles,
            args.iris_in_index,
        ]
    ):
        print(
            "Please pass a dataset specific argument to create a dataset. Use the -h flag for help."
        )
        exit(1)
    else:
        main(args)
