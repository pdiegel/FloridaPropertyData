'''This module will download and unzip county dataframe files, and place
 them in their respective directories'''
from helpers import downloader, gzipconverter

DOWNLOAD_DICT = {
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


def main(download_dict: dict) -> None:
    '''
    Downloads all given county dataframes.

    Args:
        download_dict: dictionary containing County as the keys,
         and a list of download urls as the values
    '''
    download_object = downloader.Downloader(download_dict)
    download_object.download()
    file_path_list = gzipconverter.GZIPConverter.get_dataframe_file_paths()
    gzipconverter.GZIPConverter.convert_files_to_gzip(file_path_list)


if __name__ == '__main__':
    main(DOWNLOAD_DICT)
