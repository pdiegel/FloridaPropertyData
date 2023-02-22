import os
import pandas as pd
from helpers import databases, parcel

# Define constants
PARCEL_ID_COL = 'Parcel ID'

# Build file paths and set separators
data_dir = 'data'
county_files = {
    'Sarasota': (os.path.join(data_dir, 'sarasota', 'Sarasota.gzip'), ','),
    'Manatee': (os.path.join(data_dir, 'manatee', 'manatee_ccdf.gzip'), ','),
    'Charlotte': (os.path.join(data_dir, 'charlotte', 'cd.gzip'), '|'),
    # add more counties here
}

# Load the database for each county
county_databases = {
    county: pd.read_csv(file_path, compression='gzip', sep=sep, dtype=str)
    for county, (file_path, sep) in county_files.items()
}

# Create a ParcelDatabases object
county_parcel_databases = databases.ParcelDatabases(county_databases)

# Prompt the user for the parcel ID
parcel_id = "0138110106"

# Call the get_parcel_info method to get the parcel data

try:
    county, parcel_data = county_parcel_databases.get_parcel_info(parcel_id)
    p = parcel.Parcel(county, parcel_data)
    print(vars(p))
except ValueError:
    county, parcel_data = None, None
    print('The parcel ID provided is not valid for this county.')
