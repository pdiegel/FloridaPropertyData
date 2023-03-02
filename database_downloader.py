'''This module will download and unzip county database files, and place
 them in their respective directories'''
import os
import urllib.request
from zipfile import ZipFile
import time
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'data')
TEMP_ZIP_DOWNLOAD_PATH = os.path.join(DATA_DIR, 'temp.zip')
COUNTIES = ['sarasota', 'manatee', 'charlotte']
COUNTY_DIRECTORIES = [os.path.join(DATA_DIR, county) for county in COUNTIES]


def determine_county(download_url: str) -> str:
    '''Returns the County of the download url'''
    if 'sc-pa' in download_url:
        return 'sarasota'
    elif 'manatee' in download_url:
        return 'manatee'
    elif 'ccappraiser' in download_url:
        return 'charlotte'


def download_db(download_url: str) -> None:
    '''Downloads and zips a file'''
    print(f'Downloading file from {download_url}..')
    response = urllib.request.urlopen(download_url)
    if download_url.endswith('zip'):
        temp_path = TEMP_ZIP_DOWNLOAD_PATH
    else:
        file_name = download_url.split('/')[-1]
        temp_path = os.path.join(DATA_DIR, file_name)
    file = open(temp_path, 'wb')
    file.write(response.read())
    file.close()
    print("Download completed successfully.")
    return temp_path


def unzip_file(file_path):
    if file_path.endswith('zip'):
        with ZipFile(file_path, 'r') as zObject:
            # Extracting all the members of the zip
            # into a specific location.
            zObject.extractall(DATA_DIR)
        if os.path.exists(file_path):
            os.remove(file_path)
        print('File unzipped successfully.')


def determine_file_delimiter(item_path):
    '''Returns a text file's delimiter'''
    sep = ','
    if item_path.endswith('txt'):
        with open(item_path, 'r', encoding='latin1') as f:
            if '|' in f.read():
                sep = '|'
    return sep


def open_df(path, sep):

    if 'Sarasota' in path:
        columns_to_keep = [
            "ACCOUNT", "LOCN", "LOCS", "LOCD", "UNIT", "LOCCITY", "LOCZIP",
            "SUBD", "BLOCK", "LOT", "OR_BOOK", "OR_PAGE", "LEGALREFER",
            "LEGAL1", "LEGAL2", "LEGAL3", "LEGAL4"
        ]
    elif 'manatee_ccdf' in path:
        columns_to_keep = [
            "PAR_LEGAL1", "PAR_LEGAL2", "PAR_LEGAL3", "PAR_SUBDIV_BLOCK",
            "PAR_SUBDIV_LOT", "PAR_SUBDIV_NAME", "PAR_SUBDIVISION", "PARID",
            "SITUS_ADDRESS", "SITUS_PLACE_CODE", "SITUS_POSTAL_CITY",
            "SITUS_POSTAL_ZIP", "SALE_BOOK_LAST", "SALE_PAGE_LAST",
            "SALE_INSTRNO_LAST"
        ]
    elif 'cd.' in path:
        columns_to_keep = [
            "account", "streetnumber", "streetname", "padZip", "SaleBook",
            "SalePage", "InstrumentNumber", "longlegal"
        ]
    else:
        df = pd.read_csv(
            path, encoding='latin1', on_bad_lines='skip', dtype=str,
            sep=sep, skipinitialspace=True)
        return df

    df = pd.read_csv(
        path, encoding='latin1', on_bad_lines='skip', dtype=str,
        sep=sep, skipinitialspace=True, usecols=columns_to_keep)
    return df


def remove_df_blank_space(df):
    for col in df.columns:
        df[col] = df[col].str.strip()
    return df


def format_pid_column(df):
    columns_to_check = ['account', 'Parcel ID',
                        'parcelid', 'ParcelID', 'ACCOUNT', 'PARID']
    for col in columns_to_check:
        if col in df.columns:
            df.rename({col: 'Parcel ID'}, axis='columns', inplace=True)
    return df


def convert_files_to_gzip():
    for directory in COUNTY_DIRECTORIES:
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            sep = determine_file_delimiter(item_path)

            if not os.path.isfile(item_path) or item_path.endswith('gzip'):
                continue

            file_name = item.split('.')[0]
            gzip_path = os.path.join(directory, f'{file_name}.gzip')

            temporary_df = open_df(item_path, sep)
            temporary_df.fillna('0', inplace=True)
            temporary_df = remove_df_blank_space(temporary_df)
            temporary_df = format_pid_column(temporary_df)

            temporary_df.to_csv(gzip_path, index=False,
                                compression='gzip', sep=sep)
            os.remove(item_path)
            print(f'File {item} converted to gzip successfully')


def move_unzipped_files(county):
    data_directory = DATA_DIR
    county_directory = os.path.join(data_directory, county)

    for item in os.listdir(data_directory):
        folder = ''
        item_path = os.path.join(data_directory, item)
        if os.path.isdir(item_path) and item not in COUNTIES:
            folder = item_path
        elif os.path.isfile(item_path):
            new_item_path = os.path.join(county_directory, item)
            os.replace(item_path, new_item_path)

        if folder:
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                new_item_path = os.path.join(county_directory, item)
                os.replace(item_path, new_item_path)
            os.rmdir(folder)
        print(f'Moving unzipped files from URL {item}..')


def main():
    start = time.time()
    db_download_urls = [
        'https://www.manateepao.gov/data/subdivisions_in_manatee.csv',
        'https://www.sc-pa.com/downloads/SCPA_Parcels_Sales_CSV.zip',
        'https://www.sc-pa.com/downloads/SCPA_Detailed_Data.zip',
        'https://www.manateepao.gov/data/manatee_ccdf.zip',
        'https://www.ccappraiser.com/downloads/charlotte.zip'
    ]

    file_num = 1

    for download_url in db_download_urls:
        temp_path = download_db(download_url)
        unzip_file(temp_path)
        county = determine_county(download_url)
        print(f'URL {file_num} is in {county} county..')
        move_unzipped_files(county)
        print()
        file_num += 1

    convert_files_to_gzip()
    end = time.time()
    print(f'Finished in {end-start} seconds.')
    print(f'Finished in {(end-start)/60} minutes.')


if __name__ == '__main__':
    main()
    # df = pd.read_csv(r"data\sarasota\Sarasota.gzip", compression='gzip')
    # print(df)
