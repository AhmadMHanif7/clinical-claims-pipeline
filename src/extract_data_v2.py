import csv
import requests
from pathlib import Path
import logging

def setup_logger():
    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "extract_data.log"

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
        filemode="w"
    )

def download_xmls(csv_path, output_dir):
    setup_logger()
    logger = logging.getLogger()

    output_dir.mkdir(parents=True, exist_ok=True)

    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not row or not row[0].startswith("http"):
                continue
            url = row[0]
            try:
                filename = url.split("/")[-1].split("?")[0]
                response = requests.get(url)
                response.raise_for_status()
                with open(output_dir / filename, "wb") as f:
                    f.write(response.content)
                logger.info(f"Downloaded {filename}")
            except Exception as e:
                logger.error(f"Failed to download {url}: {e}")

    print("File Extraction Complete")

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "source files/ccda_pre_signed_urls.csv"
    output_dir = project_root / "data"
    download_xmls(csv_path, output_dir)