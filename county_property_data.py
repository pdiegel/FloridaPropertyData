'''This module handles connection with the county dataframe classes.'''
import logging
from helpers import county_dataframe as county_df


class ParcelDataCollection:
    '''This class handles county parcel data collection.

    Attributes:main_dataframe
        dataframe_classes (list): A list of county dataframe classes to search.
    '''
    dataframe_classes = {
        'sarasota': county_df.Sarasota,
        'manatee': county_df.Manatee
        # 'charlotte': county_df.Charlotte
    }

    def __init__(self, parcel_id: str):
        '''Initialize a new ParcelDataCollection instance.

        Args:
            parcel_id (str): The parcel ID to search for.
        '''
        self.parcel_id = parcel_id
        self.county_dataframe_class = self.get_county_dataframe_class()
        self.parcel_data = self.get_parcel_data()

    def get_county_dataframe_class(self) -> county_df.CountyDataframe:
        '''Return the county dataframe class that the given parcel_id is in.

        Returns:
            A county dataframe class if the parcel ID is found in one of the
            county dataframes, or None if it is not found in any of them.
        '''
        logger = logging.getLogger(__name__)
        for county, dataframe_class in ParcelDataCollection.dataframe_classes.items():
            county_dataframe = dataframe_class.main_dataframe
            if self.parcel_id in county_dataframe['Parcel ID'].unique():
                logger.info('Parcel ID %r found in %r county.',
                            self.parcel_id, county)
                return dataframe_class(self.parcel_id)

            logger.info('Parcel ID %r not in %r county.',
                        self.parcel_id, county)

        logger.warning('Parcel ID %r not found in any county dataframe.', {
                       self.parcel_id})
        return None

    def get_parcel_data(self) -> dict:
        '''Retrieve the parcel data associated with the given parcel ID.

        This method calls the `find_parcel_data` method of the county dataframe class
        to retrieve the parcel data associated with the parcel ID specified in the
        constructor.

        Returns:
            A dictionary containing the parcel data, or an empty dictionary if the
            parcel data could not be found.
        '''
        parcel_data = self.county_dataframe_class.parcel_data
        return parcel_data


PARCEL_ID = '1947400154'
PARCEL_ID = '0594011650'

logging.basicConfig(level=logging.INFO)
parcel = ParcelDataCollection(PARCEL_ID)
print(parcel.parcel_data)
