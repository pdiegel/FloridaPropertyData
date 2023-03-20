"""This module will download and unzip county dataframe files, and place
 them in their respective directories"""
import time
from .helpers import downloader, gzipconverter
from .logger import logger
import os

ROOT_DIR = r"\\Server\access"

DOWNLOAD_DICT = {
    "sarasota": [
        "https://www.sc-pa.com/downloads/SCPA_Parcels_Sales_CSV.zip",
        "https://www.sc-pa.com/downloads/SCPA_Detailed_Data.zip",
    ],
    "manatee": [
        "https://www.manateepao.gov/data/manatee_ccdf.zip",
        "https://www.manateepao.gov/data/subdivisions_in_manatee.csv",
    ],
    "charlotte": [
        "https://www.ccappraiser.com/downloads/charlotte.zip",
        "https://www.ccappraiser.com/downloads/condominiums.xlsx",
        "https://www.ccappraiser.com/downloads/subdivisions.xlsx",
    ],
}

FILES_TO_KEEP = {
    "sarasota": ["Sarasota.csv", "SubDivisionIndex.txt"],
    "manatee": ["manatee_ccdf.csv", "subdivisions_in_manatee.csv"],
    "charlotte": ["cd.txt", "condominiums.xlsx", "subdivisions.xlsx"],
}


def main(download_dict: dict, files_to_keep: dict) -> None:
    """
    Downloads all given county dataframes.

    Args:
        download_dict: dictionary containing County as the keys,
         and a list of download urls as the values
    """
    logger.info("Starting download process...")
    download_object = downloader.Downloader(download_dict, files_to_keep)
    download_object.download()
    logger.info("Download process completed.")
    logger.info("Starting GZIP conversion process...")
    file_path_list = gzipconverter.GZIPConverter.get_dataframe_file_paths()
    gzipconverter.GZIPConverter.convert_files_to_gzip(file_path_list)
    logger.info("GZIP conversion process completed.")


if __name__ == "__main__":
    logger.info("Starting program...")
    start = time.time()
    main(DOWNLOAD_DICT, FILES_TO_KEEP)
    end = time.time()
    logger.info(
        "Program completed successfully. Time elapsed: %.2f seconds.",
        end - start,
    )
