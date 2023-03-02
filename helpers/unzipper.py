'''This module contains the Unzipper class.'''
import os
from zipfile import ZipFile


class Unzipper:
    '''This class handles zip file extraction'''
    @staticmethod
    def unzip(file_download_path: os.path, destination: os.path) -> None:
        """
        Unzips the given file to the specified destination.

        Args:
            file_download_path: A string representing the path to the file to unzip.
            destination: A string representing the path to the destination folder.
        """
        with ZipFile(file_download_path, 'r') as zip_object:
            zip_object.extractall(destination)
        if os.path.exists(file_download_path):
            os.remove(file_download_path)
