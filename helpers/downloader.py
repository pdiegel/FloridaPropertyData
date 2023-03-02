'''This module handles downloading and unzipping county database files.'''
import os
import urllib.request
from helpers import unzipper


class Downloader:
    '''This class handles file downloading'''
    data_folder: str = 'data'

    def __init__(self: 'Downloader', urls_to_download: dict):
        """
        Initializes a Downloader object with a dictionary of URLs to download.

        Args:
            urls_to_download: A dictionary containing the URLs to download.
        """
        self.urls_to_download = urls_to_download

    def get_county_data_path(self: 'Downloader', county: str) -> os.path:
        """
        Returns the path to a county's data folder.

        Args:
            county: A string representing the name of the county.

        Returns:
            A string representing the path to the county's data folder.
        """
        county_data_path = os.path.join('data', county)
        return county_data_path

    def get_file_type(self: 'Downloader', url: str) -> str:
        """
        Returns the file type of a given URL.

        Args:
            url: A string representing the URL.

        Returns:
            A string representing the file type.
        """
        file_type = url.split('.')[-1]
        return file_type

    def get_file_download_path(self: 'Downloader', county_data_path: os.path,
                               download_url: str) -> os.path:
        """
        Generates and returns the path to download a file.

        Args:
            county_data_path: A string representing the path to the county's data folder.
            download_url: A string representing the URL to download.

        Returns:
            A string representing the path to download the file.
        """
        if download_url.lower().endswith('zip'):
            file_name: str = 'Temp.zip'
        else:
            file_name: str = download_url.split('/')[-1]
        file_download_path = os.path.join(
            county_data_path, file_name)
        return file_download_path

    def download(self: 'Downloader') -> None:
        """
        Downloads the files from the URLs in the downloader object.
        """
        for county in self.urls_to_download:
            for download_url in self.urls_to_download[county]:
                county_data_path = self.get_county_data_path(county)
                file_download_path = self.get_file_download_path(
                    county_data_path, download_url)
                with urllib.request.urlopen(download_url) as response:
                    with open(file_download_path, 'wb') as file:
                        file.write(response.read())
                if file_download_path.lower().endswith('zip'):
                    unzipper.Unzipper.unzip(
                        file_download_path, county_data_path)
