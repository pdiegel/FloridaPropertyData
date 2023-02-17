import pandas as pd
import os
import urllib.request
from zipfile import ZipFile

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'data')
TEMP_DOWNLOAD_DESTINATION = os.path.join(DATA_DIR, 'temp.zip')
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
    response = urllib.request.urlopen(download_url)
    file = open(TEMP_DOWNLOAD_DESTINATION, 'wb')
    file.write(response.read())
    file.close()
    print(f"File {download_url} download completed.")


def unzip_file(file_path):
    with ZipFile(file_path, 'r') as zObject:
        # Extracting all the members of the zip
        # into a specific location.
        zObject.extractall(DATA_DIR)
    if os.path.exists(file_path):
        os.remove(file_path)


def convert_files_to_gzip():
    for directory in COUNTY_DIRECTORIES:
        for item in os.listdir(directory):
            sep = ','
            item_path = os.path.join(directory, item)
            if not os.path.isfile(item_path) or item_path.endswith('gzip'):
                continue

            if item_path.endswith('txt'):
                with open(item_path, 'r') as f:
                    if '|' in f.read():
                        sep = '|'

            formatted_item = item.split('.')[0]
            gzip_file = os.path.join(directory, f'{formatted_item}.gzip')

            df = pd.read_csv(item_path, encoding='latin1',
                             on_bad_lines='warn', dtype='str', sep=sep,
                             skipinitialspace=True)
            df.fillna('0', inplace=True)

            columns_to_check = ['account', 'Parcel ID',
                                'parcelid', 'ParcelID', 'ACCOUNT', 'PARID']
            for col in columns_to_check:
                if col in df.columns:
                    print(f'{formatted_item} Database:')
                    print(f'Removing extra space from {col} column.')
                    df[col] = df[col].str.strip()
            df.to_csv(gzip_file, index=False, compression='gzip', sep=sep)
            os.remove(item_path)


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


def main():
    db_download_urls = [
        'https://www.sc-pa.com/downloads/SCPA_Parcels_Sales_CSV.zip',
        'https://www.sc-pa.com/downloads/SCPA_Detailed_Data.zip',
        'https://www.manateepao.gov/data/manatee_ccdf.zip',
        'https://www.ccappraiser.com/downloads/charlotte.zip'
    ]

    file_num = 1

    for download_url in db_download_urls:
        print(f'URL {file_num} Starting..')
        download_db(download_url)
        print(f'URL {file_num} Downloaded..')
        unzip_file(TEMP_DOWNLOAD_DESTINATION)
        print(f'URL {file_num} Unzipped..')
        county = determine_county(download_url)
        print(f'URL {file_num} is in {county} county..')
        move_unzipped_files(county)
        print(f'Moving unzipped files from URL {file_num}..')
        print()
        file_num += 1
    print('Converting all files to gzip..')
    convert_files_to_gzip()
    print('Done.')


if __name__ == '__main__':
    main()
