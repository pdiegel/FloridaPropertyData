class ParcelDatabases:
    def __init__(self, county_databases):
        self.county_databases = county_databases

    def get_parcel_info(self, parcel_id):
        '''Returns a Parcel's information, given the Parcel ID'''
        for county, county_database in self.county_databases.items():
            parcel_data = county_database.loc[
                county_database['Parcel ID'] == parcel_id]
            if not parcel_data.empty:
                print(f'{county} County Retrieved.')
                return county, parcel_data

        raise ValueError('Invalid Parcel ID provided.')
