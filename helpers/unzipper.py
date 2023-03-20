'''This module contains the Unzipper class.'''
import os
from zipfile import ZipFile
from ..logger import logger


class Unzipper:
    '''This class handles zip file extraction'''
    @staticmethod
    def unzip(file_download_path: os.path, destination: os.path) -> None:
        """
        Unzips the given file to the specified destination.

        Args:
            file_download_path: A string representing the path to the
            file to unzip.
            destination: A string representing the path to the
            destination folder.
        """
        logger.info('Unzipping %s to %s', file_download_path, destination)
        with ZipFile(file_download_path, 'r') as zip_object:
            zip_object.extractall(destination)

        if os.path.exists(file_download_path):
            os.remove(file_download_path)
        logger.info('Finished unzipping %s', file_download_path)
