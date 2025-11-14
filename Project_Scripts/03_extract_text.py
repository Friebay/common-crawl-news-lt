# -*- coding: utf-8 -*-
"""
Script for extracting and processing text content from WARC files.

Updated for improved maintainability and robustness.
"""
from tqdm import tqdm
import pandas as pd
import multiprocessing
import os
import logging
import trafilatura
import json
from urllib.parse import urlparse
from argparse import ArgumentParser
import functools

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# python Project_Scripts\03_extract_text.py data\unprocessed data\tlds.xlsx

def extract_top_level_domain(url):
    """Extract the top-level domain (TLD) from a URL."""
    try:
        parsed_url = urlparse(url)
        domain_parts = parsed_url.netloc.split('.')
        if len(domain_parts) > 1:
            return '.' + domain_parts[-1]
        return domain_parts[0]
    except Exception as e:
        logging.warning(f"Error extracting TLD from {url}: {e}")
        return None

def parse_file(filename, exclude_tlds):
    """Parse a single feather file and extract text using Trafilatura."""
    rows = []
    try:
        data = pd.read_feather(filename)
        data["TLD"] = data["URL"].apply(extract_top_level_domain)
        data = data.dropna(subset=["TLD"])
        data = data[data["TLD"].str.lower() == ".lt"]
        data = data.reset_index(drop=True)

        for _, row in tqdm(data.iterrows(), total=len(data), desc=f"Processing {os.path.basename(filename)}", leave=False):
            try:
                extracted = trafilatura.extract(
                    row["Content"],
                    include_comments=False,
                    deduplicate=True,
                    output_format="json",
                    with_metadata=True,
                    target_language="lt"
                )
                if extracted:
                    root = json.loads(extracted)
                    rows.append({
                        "id": row["ID"],
                        "text": root.get("raw_text"),
                        "url": row["URL"],
                        "excerpt": root.get("excerpt"),
                        "date": root.get("date"),
                        "tags": root.get("tags"),
                        "categories": root.get("categories"),
                        "title": root.get("title"),
                        "date_crawled": root.get("filedate"),
                        "hostname": root.get("hostname")
                    })
            except Exception as e:
                logging.warning(f"Error processing record {row['URL']}: {e}")

        if rows:
            output_df = pd.DataFrame(rows).dropna(subset=["text"]).drop_duplicates(subset=["text", "hostname"])
            output_file = filename.replace(".feather", "_processed.feather")
            output_df.to_feather(output_file)
            logging.info(f"Saved processed file: {output_file}")

    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")

def main(folder, tlds_file):
    """Main function to process all feather files in the given folder."""
    if not os.path.exists(folder):
        logging.error(f"Folder does not exist: {folder}")
        return

    exclude_tlds = pd.read_excel(tlds_file)
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".feather")]

    if not files:
        logging.warning(f"No feather files found in folder: {folder}")
        return

    logging.info(f"Processing {len(files)} files from folder: {folder}")
    with multiprocessing.Pool(processes=2) as pool:
        with tqdm(total=len(files), desc="Overall Progress") as pbar:
            for _ in pool.imap_unordered(functools.partial(parse_file, exclude_tlds=exclude_tlds), files):
                pbar.update()

if __name__ == "__main__":
    parser = ArgumentParser(description="Extract and process text from feather files.")
    parser.add_argument("folder", type=str, help="Folder containing feather files.")
    parser.add_argument("tlds_file", type=str, help="Path to the Excel file containing TLD exclusions.")
    args = parser.parse_args()

    main(args.folder, args.tlds_file)
