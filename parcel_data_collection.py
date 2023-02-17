# Load the database for each county
county_databases = {
    'Sarasota': pd.read_csv('county1_database.csv'),
    'Manatee': pd.read_csv('county2_database.csv'),
    'Charlotte': pd.read_csv('county3_database.csv'),
    # add more counties here
}

# Create a ParcelDatabase object
parcel_db = ParcelDatabase(county_databases)

# Prompt the user for the parcel ID and county
parcel_id = input("Enter the parcel ID: ")
county = input("Enter the county: ")

# Call the find_parcel_data method to get the parcel data
parcel_data = parcel_db.find_parcel_data(parcel_id, county)

# Display the parcel data if it was found
if parcel_data is not None:
    print(parcel_data)
