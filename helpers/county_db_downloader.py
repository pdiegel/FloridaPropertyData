'''This class handles downloading and unzipping county database files.'''
import os
import urllib.request
from zipfile import ZipFile


class Downloader():
    '''This class handles file downloading'''
    data_folder = 'data'

    def __init__(self: 'Downloader', urls_to_download: dict):
        self.urls_to_download = urls_to_download

    def get_county_data_path(self, county: str) -> os.path:
        '''Returns a county's data path'''
        county_data_path = os.path.join('data', county)
        return county_data_path

    def get_file_type(self, url: str) -> str:
        '''Returns a file's type.'''
        file_type = url.split('.')[-1]
        return file_type

    def get_file_download_path(self, county_data_path: os.path,
                               download_url: str) -> os.path:
        '''Generates and returns a file's download path.'''
        file_type = self.get_file_type(download_url)
        file_download_path = os.path.join(
            county_data_path, f'File.{file_type}')
        return file_download_path

    def download(self) -> None:
        '''Download a file from a given url.'''
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


class Unzipper():
    '''This class handles zip file extraction'''
    @staticmethod
    def unzip(file_download_path: os.path, destination: os.path) -> None:
        '''Unzip a File.'''
        with ZipFile(file_download_path, 'r') as zip_object:
            zip_object.extractall(destination)
        if os.path.exists(file_download_path):
            os.remove(file_download_path)


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

d = Downloader(download_dict)
d.download()
