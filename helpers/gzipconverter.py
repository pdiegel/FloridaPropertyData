'''This module contains the GZIPConverter class.'''
import os
import pandas as pd
import yaml
from helpers import downloader


class GZIPConverter:
    '''This class handles file conversion to gzip format.'''
    @staticmethod
    def get_dataframe_file_paths() -> list:
        """
        Returns a list of paths to the dataframe files.

        Returns:
            A list of strings representing the paths to the dataframe files.
        """
        dataframe_file_paths: list = []
        for (root, _, files) in os.walk(downloader.Downloader.data_folder,
                                        topdown=True):
            for file in files:
                file_path = os.path.join(root, file)
                dataframe_file_paths.append(file_path)
        return dataframe_file_paths

    @staticmethod
    def convert_files_to_gzip(dataframe_file_paths: list) -> None:
        """
        Converts a list of dataframe files to gzip format.

        Args:
            dataframe_file_paths: A list of strings representing the paths to
            the dataframe files to convert.
        """
        for file_path in dataframe_file_paths:
            file_parent_directory = '/'.join(file_path.split('/')[:-1])
            file_type: str = file_path.split('.')[-1]
            file_name: str = file_path.split('.')[0].split('/')[-1]
            sep: str = GZIPConverter.determine_file_delimiter(
                file_path, file_type)

            file_destination = os.path.join(
                file_parent_directory, f'{file_name}.gzip')

            temporary_df: pd.DataFrame = GZIPConverter.open_df(
                file_path, sep=sep)
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
            with open(file_path, 'r', encoding='latin1') as file:
                if '|' in file.read():
                    sep: str = '|'
        return sep

    @staticmethod
    def open_df(file_path: os.path, compression_type: str = '', sep: str = ',') -> pd.DataFrame:
        """
        Opens a dataframe from the given text file.

        Args:
            file_path: A string representing the path to the text file.
            sep: A string representing the delimiter used in the text file.

        Returns:
            A pandas dataframe representing the contents of the text file.
        """
        columns_to_keep = GZIPConverter.get_columns_to_keep(file_path)
        print(file_path)
        if str(file_path).endswith('xlsx'):
            dataframe = pd.read_excel(
                file_path, dtype=str,
                usecols=columns_to_keep)
            return dataframe

        if compression_type:
            dataframe = pd.read_csv(
                file_path, dtype=str, sep=sep,  compression=compression_type)
            return dataframe

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
            dataframe[column] = dataframe[column].str.replace('nan', '0')
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
        Returns a list of dataframe columns to keep for a given text file.

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
