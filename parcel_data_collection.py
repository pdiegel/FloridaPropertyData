from helpers import county_data_collector as collector
import pandas as pd
from typing import Union


class ParcelDataCollection:
    database_classes = [
        collector.Sarasota,
        collector.Manatee,
        collector.Charlotte
    ]

    def __init__(self, parcel_id: str):
        self.parcel_id = parcel_id
        self.county_database_class = self.get_county_database_class()

    def get_county_database_class(self) -> collector.CountyDatabase:
        '''Returns the county database class that the given parcel_id is in.'''
        for county_database_class in ParcelDataCollection.database_classes:
            county_database = pd.read_csv(
                county_database_class.main_database,
                compression='gzip', dtype=str)
            try:
                if self.parcel_id in county_database['Parcel ID'].unique():
                    return county_database_class
            except KeyError:
                print('Invalid Parcel ID..', self.parcel_id)


PARCEL_ID = '00230800'

parcel_data = ParcelDataCollection(PARCEL_ID)
print(parcel_data.get_county_database_class())
