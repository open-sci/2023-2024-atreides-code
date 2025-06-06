import argparse

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))


from oc_meta import (
    process_meta,
    search_for_titles,
    create_iris_not_in_meta,
    create_iris_noid,
)
from oc_index import process_index_dump


def main(args):
    if args.iris_in_meta:
        process_meta(args.meta_path, args.iris_path)

    if args.iris_not_in_meta:
        create_iris_not_in_meta(args.iris_path)

    if args.iris_no_id:
        create_iris_noid(args.iris_path)

    if args.search_for_titles:
        search_for_titles(args.iris_path)

    if args.iris_in_index:
        if args.index_path is None:
            print(
                "Please provide the path to the OpenCitations Index dump folder by specifying the -index argument."
            )
            exit(1)
        process_index_dump(args.index_path)


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
