#!/usr/bin/env python3
import csv
import argparse
import sys
import os
import polars as pl
from typing import List, Dict, Optional
import time
import backoff
import requests
from requests_cache import CachedSession
from urllib.parse import urlencode
import logging
from tqdm import tqdm

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("crossref_search.log"),
        # logging.StreamHandler(sys.stdout)
    ],
)
logger = logging.getLogger(__name__)


class CrossrefClient:
    def __init__(self, email: str, output_file: str, cache_expire_after: int = 86400):
        self.session = CachedSession(
            "crossref_cache", expire_after=cache_expire_after, backend="sqlite"
        )
        self.headers = {"User-Agent": f"PythonCrossrefClient/1.0 (mailto:{email})"}
        self.base_url = "https://api.crossref.org/works"
        self.output_file = output_file

        self.last_id = self._load_last()

    def _load_last(self) -> Dict:
        if os.path.exists(self.output_file):
            last = pl.read_csv(self.output_file).select(pl.last("item_id")).item()
            return last
        return 0

    def _save_result(self, result: Dict):
        write_header = not os.path.exists(self.output_file)

        with open(self.output_file, "a", newline="\n", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())

            if write_header:
                writer.writeheader()

            writer.writerow(result)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.HTTPError),
        max_tries=5,
        max_time=30,
    )
    def _make_request(self, url: str, params: Dict) -> Dict:
        full_url = f"{url}?{urlencode(params)}"
        logger.info(f"Making request to: {full_url}")

        try:
            response = self.session.get(url, params=params, headers=self.headers)
            response.raise_for_status()

            if response.status_code == 429:
                logger.warning("Rate limited. Backing off...")
                time.sleep(10)
                return self._make_request(url, params)

            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return None
            raise

        except Exception as e:
            logger.error(f"Error making request: {str(e)}")
            raise

    def parse_authors(self, authors: List[Dict]) -> str:
        return ", ".join(
            [
                f"{author.get('given', '')} {author.get('family', '')}"
                for author in authors
                if "family" in author
            ]
        )

    def search_publications(self, input_df: pl.DataFrame):
        start_index = input_df["ITEM_ID"].to_list().index(self.last_id)
        logger.info(f"Starting from index {start_index}")

        with tqdm(total=len(input_df), initial=start_index, leave=True) as pbar:
            for row in input_df[start_index + 1 :].iter_rows():
                item_id = row[0]
                author = row[6]
                title = row[9]

                params = {
                    "query.bibliographic": f"{title}, {author}",
                    "rows": 2,
                    "select": "author,title,DOI,ISSN,score",
                }

                try:
                    logger.info(f"Searching for {author} - '{title}'")
                    response_data = self._make_request(self.base_url, params)

                    if response_data and "message" in response_data:
                        items = response_data["message"].get("items", [])

                        if items:
                            best_match = items[0]
                            logger.info(
                                f"Found {best_match.get('score')} - "
                                f"{self.parse_authors(best_match.get('author', []))} - "
                                f"'{best_match.get('title', [None])[0]}'"
                            )

                            ambiguous = len(items) > 1 and items[0].get(
                                "score", 0
                            ) == items[1].get("score", 0)

                            result = {
                                "item_id": item_id,
                                "matched_title": best_match.get("title", [None])[0],
                                "doi": best_match.get("DOI"),
                                "issn": best_match.get("ISSN", [None])[0]
                                if "ISSN" in best_match
                                else None,
                                "ambiguous_match": ambiguous,
                                "score": best_match.get("score"),
                            }
                        else:
                            logger.info(f"No match found for '{title}'")
                            continue

                        if best_match.get("score", 0) > 85:
                            self._save_result(result)

                    pbar.update(1)

                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing '{title}': {str(e)}")
                    pbar.update(1)
                    continue


def main():
    parser = argparse.ArgumentParser(
        description="Search for publications using Crossref API"
    )
    parser.add_argument(
        "output_file", help="Output CSV file path", default="crossref_results.csv"
    )
    parser.add_argument(
        "input_df",
        help="Input Dataframe path",
        default="../data/iris_no_id/iris_no_id.parquet",
    )
    parser.add_argument(
        "--cache-expire",
        type=int,
        default=86400,
        help="Cache expiration time in seconds (default: 86400)",
    )

    args = parser.parse_args()

    try:
        logger.info(f"Loading input file: {args.input_df}")
        input_df = pl.read_parquet(args.input_df)

        client = CrossrefClient(
            email="leonardo.zilli@studio.unibo.it",
            output_file=args.output_file,
            cache_expire_after=args.cache_expire,
        )

        logger.info("Starting publication search...")
        client.search_publications(input_df)
        logger.info("Search completed successfully")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
