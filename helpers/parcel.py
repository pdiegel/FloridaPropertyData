import pandas as pd


class ParcelDatabase:
    def __init__(self, county_databases):
        self.county_databases = county_databases

    def find_parcel_data(self, parcel_id):
        if county in self.county_databases:
            county_database = self.county_databases[county]
            parcel_data = county_database[county_database['parcel_id'] == parcel_id]
            if not parcel_data.empty:
                return parcel_data
            else:
                print(
                    f"Parcel with ID {parcel_id} not found in {county} database.")
        else:
            print(f"No database found for {county} county.")

    def add_county_database(self, county, database):
        self.county_databases[county] = database

    def remove_county_database(self, county):
        del self.county_databases[county]
