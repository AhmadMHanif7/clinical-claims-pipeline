import os
import csv
import requests
from urllib.parse import urlparse, unquote
import logging

# Paths
CSV_PATH = "../source files/ccda_pre_signed_urls.csv"
DATA_DIR = "../data"
LOG_PATH = "../logs/extract_data.csv"

# Setup logging
logging.basicConfig(
    filename=LOG_PATH,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Ensure data folder exists
os.makedirs(DATA_DIR, exist_ok=True)

def extract_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    return os.path.basename(unquote(path))

with open(CSV_PATH, newline="") as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader, None)  # Skip header
    logging.info(f"Skipped header: {header}")

    for i, row in enumerate(reader):
        if not row or not row[0].strip():
            logging.warning(f"Skipped empty row {i+2}")
            continue

        url = row[0].strip()
        if url.lower().startswith("http") and ".xml" in url.lower():
            try:
                response = requests.get(url)
                response.raise_for_status()

                file_name = extract_filename_from_url(url)
                file_path = os.path.join(DATA_DIR, file_name)

                with open(file_path, "wb") as f:
                    f.write(response.content)

                logging.info(f"Downloaded {file_name}")
            except Exception as e:
                logging.error(f"Failed to download {url} - {e}")
        else:
            logging.warning(f"Invalid or non-XML URL at row {i+2}: {url}")

print("Download Finished")