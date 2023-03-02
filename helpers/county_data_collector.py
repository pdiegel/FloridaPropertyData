from abc import ABC, abstractmethod
import os


class CountyDatabase(ABC):
    data_folder = 'data'

    @abstractmethod
    def get_parcel_data(self) -> dict:
        '''Returns a Parcel's data, given the Parcel ID'''

        raise ValueError('Invalid Parcel ID provided.')


class Sarasota(CountyDatabase):
    county = 'sarasota'

    county_data_folder = os.path.join(
        CountyDatabase.data_folder, county)

    main_database = os.path.join(
        county_data_folder, 'Parcel_Sales_CSV', 'Sarasota.gzip')

    def __init__(self, parcel_id):
        self.parcel_id = parcel_id
        self.county = Sarasota.county

    def get_parcel_data(self):
        for county, county_database in self.county_databases.items():
            parcel_data = county_database.loc[
                county_database['Parcel ID'] == parcel_id]
            if not parcel_data.empty:
                print(f'{county} County Retrieved.')
                return county, parcel_data


class Manatee(CountyDatabase):
    county = 'manatee'

    county_data_folder = os.path.join(
        CountyDatabase.data_folder, county)

    main_database = os.path.join(
        county_data_folder, 'manatee_ccdf.gzip')

    def __init__(self, parcel_id):
        self.parcel_id = parcel_id
        self.county = Manatee.county

    def get_parcel_data(self):
        for county, county_database in self.county_databases.items():
            parcel_data = county_database.loc[
                county_database['Parcel ID'] == parcel_id]
            if not parcel_data.empty:
                print(f'{county} County Retrieved.')
                return county, parcel_data


class Charlotte(CountyDatabase):
    county = 'charlotte'

    county_data_folder = os.path.join(
        CountyDatabase.data_folder, county)

    main_database = os.path.join(
        county_data_folder, 'cd.gzip')

    def __init__(self, parcel_id):
        self.parcel_id = parcel_id
        self.county = Charlotte.county

    def get_parcel_data(self):
        for county, county_database in self.county_databases.items():
            parcel_data = county_database.loc[
                county_database['Parcel ID'] == parcel_id]
            if not parcel_data.empty:
                print(f'{county} County Retrieved.')
                return county, parcel_data
