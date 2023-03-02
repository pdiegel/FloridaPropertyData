'''This class handles downloading and unzipping county database files.'''
import os
import urllib.request
from zipfile import ZipFile
import pandas as pd
import yaml


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
                    Unzipper.unzip(file_download_path, county_data_path)


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


class GZIPConverter:
    '''This class handles file conversion to gzip format.'''
    @staticmethod
    def get_database_file_paths() -> list:
        """
        Returns a list of paths to the database files.

        Returns:
            A list of strings representing the paths to the database files.
        """
        database_file_paths: list = []
        for (root, _, files) in os.walk(Downloader.data_folder,
                                        topdown=True):
            for file in files:
                file_path = os.path.join(root, file)
                database_file_paths.append(file_path)
        return database_file_paths

    @staticmethod
    def convert_files_to_gzip(database_file_paths: list) -> None:
        """
        Converts a list of database files to gzip format.

        Args:
            database_file_paths: A list of strings representing the paths to
            the database files to convert.
        """
        for file_path in database_file_paths:
            file_parent_directory = '/'.join(file_path.split('/')[:-1])
            file_type: str = file_path.split('.')[-1]
            file_name: str = file_path.split('.')[0].split('/')[-1]
            sep: str = GZIPConverter.determine_file_delimiter(
                file_path, file_type)

            file_destination = os.path.join(
                file_parent_directory, f'{file_name}.gzip')

            temporary_df: pd.DataFrame = GZIPConverter.open_df(file_path, sep)
            temporary_df.fillna('0', inplace=True)
            temporary_df: pd.DataFrame = GZIPConverter.remove_df_blank_space(
                temporary_df)
            temporary_df: pd.DataFrame = GZIPConverter.format_pid_column(
                temporary_df)

            temporary_df.to_csv(file_destination, index=False,
                                compression='gzip', sep=sep)
            os.remove(file_path)
            print(f'{file_name}.{file_type} converted to gzip successfully')

    @staticmethod
    def determine_file_delimiter(file_path: os.path, file_type: str) -> str:
        """
        Determines the delimiter for a text file.

        Args:
            file_path: A string representing the path to the text file.
            file_type: A string representing the type of the text file.

        Returns:
            A string representing the delimiter used in the text file.
        """
        sep: str = ','
        if file_type == 'txt':
            with open(file_path, 'r', encoding='latin1') as f:
                if '|' in f.read():
                    sep: str = '|'
        return sep

    @staticmethod
    def open_df(file_path: os.path, sep: str) -> pd.DataFrame:
        """
        Opens a dataframe from the given text file.

        Args:
            file_path: A string representing the path to the text file.
            sep: A string representing the delimiter used in the text file.

        Returns:
            A pandas dataframe representing the contents of the text file.
        """
        columns_to_keep = GZIPConverter.get_columns_to_keep(file_path)

        if not columns_to_keep:
            dataframe = pd.read_csv(
                file_path, encoding='latin1', on_bad_lines='skip', dtype=str,
                sep=sep, skipinitialspace=True)
            return dataframe

        dataframe = pd.read_csv(
            file_path, encoding='latin1', on_bad_lines='skip', dtype=str,
            sep=sep, skipinitialspace=True, usecols=columns_to_keep)
        return dataframe

    @staticmethod
    def remove_df_blank_space(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Removes blank spaces from the given dataframe.

        Args:
            dataframe: A pandas dataframe to remove blank spaces from.

        Returns:
            A pandas dataframe with blank spaces removed.
        """
        for column in dataframe.columns:
            dataframe[column] = dataframe[column].str.strip()
        return dataframe

    @staticmethod
    def format_pid_column(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Formats the Parcel ID column in the given dataframe.

        Args:
            dataframe: A pandas dataframe to format the Parcel ID column.

        Returns:
            A pandas dataframe with the Parcel ID column formatted.
        """
        columns_to_check = ['account', 'Parcel ID',
                            'parcelid', 'ParcelID', 'ACCOUNT', 'PARID']
        for col in columns_to_check:
            if col in dataframe.columns:
                dataframe.rename({col: 'Parcel ID'},
                                 axis='columns', inplace=True)
        return dataframe

    @staticmethod
    def get_columns_to_keep(file_path: os.path) -> list:
        """
        Returns a list of database columns to keep for a given text file.

        Args:
            file_path: A string representing the path to the text file.

        Returns:
            A list of strings representing the names of the columns to keep.
        """
        columns_to_keep = []
        with open(r'helpers\column_mapping.yaml', 'r', encoding='utf-8') as yaml_file:
            column_mapping = yaml.safe_load(yaml_file)

        file_name = os.path.basename(file_path).lower()
        if file_name in column_mapping:
            columns_to_keep = column_mapping[file_name]

        return columns_to_keep


download_dict = {
    'sarasota': [
        'https://www.sc-pa.com/downloads/SCPA_Parcels_Sales_CSV.zip',
        'https://www.sc-pa.com/downloads/SCPA_Detailed_Data.zip'],
    'manatee': [
        'https://www.manateepao.gov/data/manatee_ccdf.zip',
        'https://www.manateepao.gov/data/subdivisions_in_manatee.csv'],
    'charlotte': [
        'https://www.ccappraiser.com/downloads/charlotte.zip'
    ]
}

downloader = Downloader(download_dict)
downloader.download()
file_path_list = GZIPConverter.get_database_file_paths()
GZIPConverter.convert_files_to_gzip(file_path_list)
