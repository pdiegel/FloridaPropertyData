"""This module handles downloading and unzipping county dataframe files."""
import os
import urllib.request
from ..helpers import unzipper
from ..logger import logger
from ..config import COUNTY_DATABASE_DIR


class Downloader:
    """This class handles file downloading"""

    data_folder: str = COUNTY_DATABASE_DIR

    def __init__(
        self: "Downloader", urls_to_download: dict, files_to_keep: dict
    ):
        """
        Initializes a Downloader object with a dictionary of URLs to download.

        Args:
            urls_to_download: A dictionary containing the URLs to download.
        """
        self.urls_to_download = urls_to_download
        self.files_to_keep = files_to_keep

    def get_county_data_path(self: "Downloader", county: str) -> os.path:
        """
        Returns the path to a county's County Dataframes folder.

        Args:
            county: A string representing the name of the county.

        Returns:
            A string representing the path to the county's County
            Dataframes folder.
        """
        county_data_path = os.path.join(Downloader.data_folder, county)
        return county_data_path

    def get_file_type(self: "Downloader", url: str) -> str:
        """
        Returns the file type of a given URL.

        Args:
            url: A string representing the URL.

        Returns:
            A string representing the file type.
        """
        file_type = url.split(".")[-1]
        return file_type

    def get_file_download_path(
        self: "Downloader", county_data_path: os.path, download_url: str
    ) -> os.path:
        """
        Generates and returns the path to download a file.

        Args:
            county_data_path: A string representing the path to the
            county's County Dataframes folder.
            download_url: A string representing the URL to download.

        Returns:
            A string representing the path to download the file.
        """
        if download_url.lower().endswith("zip"):
            file_name: str = "Temp.zip"
        else:
            file_name: str = download_url.split("/")[-1]
        file_download_path = os.path.join(county_data_path, file_name)
        return file_download_path

    def download(self: "Downloader") -> None:
        """
        Downloads the files from the URLs in the downloader object.
        """

        for county in self.urls_to_download:
            county_data_path = self.get_county_data_path(county)
            logger.info("Downloading files for %s county", county)

            for download_url in self.urls_to_download[county]:
                file_download_path = self.get_file_download_path(
                    county_data_path, download_url
                )
                logger.debug(
                    "Downloading file from %s to %s",
                    download_url,
                    file_download_path,
                )

                with urllib.request.urlopen(download_url) as response:
                    with open(file_download_path, "wb") as file:
                        file.write(response.read())

                if file_download_path.lower().endswith("zip"):
                    logger.debug(
                        "Unzipping %s to %s",
                        file_download_path,
                        county_data_path,
                    )
                    unzipper.Unzipper.unzip(
                        file_download_path, county_data_path
                    )
            self.remove_unneeded_files(county, county_data_path)

    def remove_unneeded_files(
        self: "Downloader", county: str, county_data_path: os.path
    ) -> None:
        """
        Removes all files in the given `county_data_path` directory that
        are not listed in`self.files_to_keep[county]`, except for files
        ending with the extension '.gzip'.

        Args:
            self: The `Downloader` instance calling this method.
            county (str): The name of the county whose County Dataframes
            files should be processed.
            county_data_path (os.PathLike): The path to the directory
            containing the county's County Dataframes files.

        Returns:
            None. This function modifies the file system directly.

        Raises:
            OSError: If there is an error removing one of the files.
        """
        files_to_keep = self.files_to_keep[county]

        for root, _, files in os.walk(county_data_path, topdown=False):
            for file in files:
                file_path = os.path.join(root, file)

                if file not in files_to_keep:
                    if not file.endswith(".gzip"):
                        logger.info("Removing %s", file_path)
                        os.remove(file_path)
                    else:
                        logger.info("Skipping %s", file_path)
                else:
                    logger.info("Keeping %s", file_path)
