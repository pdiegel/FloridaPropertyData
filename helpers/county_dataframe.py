'''This module provides a set of classes for accessing county property
data in a standardized way.

This module provides the `CountyDataframe` abstract base class, which 
defines a common interface for accessing county property data. To create
a new county dataframe class, you should subclass this class and
implement the abstract `find_parcel_data` method.

The `CountyDataframe` class is intended to be used as a base class for 
specific county property dataframe classes. Each county dataframe class
should implement the `find_parcel_data` method, which retrieves the
parcel data associated with the parcel ID specified in the constructor
and returns it as a dictionary. This module also provides concrete
county dataframe classes that extend the `CountyDataframe` base class
and implement the `find_parcel_data` method for specific counties.

Classes:
    CountyDataframe: An abstract base class representing a county
    property dataframe.
    Charlotte: A concrete county dataframe class for Charlotte County.
    Manatee: A concrete county dataframe class for Manatee County.
    Sarasota: A concrete county dataframe class for Sarasota County.

Raises:
    ValueError: If an invalid parcel ID is provided to the
    `find_parcel_data` method.

Attributes:
    data_folder (str): The path to the directory where county data is
    stored.
    parcel_data_structure (dict): A dictionary that defines the
    structure of the parcel data returned by the `find_parcel_data`
    method of subclasses.

Abstract methods:
    find_parcel_data(self) -> dict: This abstract method retrieves the
    parcel data associated with the parcel ID specified in the
    constructor and returns it as a dictionary.

Methods:
    find_location_data(self, parcel_dataframe: pd.DataFrame) -> None: 
    This method finds and formats the address data for the parcel data
    dictionary.
    
    find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
    This method finds and formats the property type and subdivision data
    for the parcel data dictionary.
    
    find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
    This method finds and formats the legal description data for the
    parcel data dictionary.

Concrete subclasses:
    Sarasota: A concrete county dataframe class for Sarasota County.
    It provides an implementation for the abstract `find_parcel_data`
    method specific to Sarasota County, and implements the methods
    `find_location_data`, `find_subdivision_data`, and `find_legal_data`
    to extract specific data for this county.


'''
from abc import ABC, abstractmethod
import os
import pandas as pd
from helpers import misc, gzipconverter


class CountyDataframe(ABC):
    '''Abstract base class representing a county property dataframe.

    This class defines an interface for accessing county property data.
    To create a new county dataframe class, you should subclass this
    class and implement the abstract `find_parcel_data` method.

    Attributes:
        data_folder (str): The path to the directory where county data
        is stored.
    '''
    data_folder = 'data'
    parcel_data_structure = {
        'address_number': '',
        'street': '',
        'direction': '',
        'address': '',
        'city': '',
        'zip_code': '',
        'subdivision': '',
        'lot': '',
        'block': '',
        'unit': '',
        'plat_book': '',
        'plat_page': '',
        'property_type': '',
        'or_book': '',
        'or_page': '',
        'or_instrument': '',
        'legal_description': ''
    }

    @abstractmethod
    def find_parcel_data(self) -> None:
        '''Abstract method that retrieves the parcel data associated
        with the parcel ID.

        This method should retrieve the parcel data associated with the
        parcel ID specified in the constructor and return it as a
        dictionary.

        Returns:
            A dictionary containing the parcel data.
        '''

    @abstractmethod
    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Abstract method that finds and formats address data for a
        given parcel.'''

    @abstractmethod
    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Abstract method that finds and formats property type and
        subdivision data for a given parcel.'''

    @abstractmethod
    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Abstract method that finds and formats legal description data
        for a given parcel.'''


class Sarasota(CountyDataframe):
    '''Concrete county dataframe class representing Sarasota county.

    This class implements the abstract `find_parcel_data` method and
    contains methods to extract data of a given parcel.

    Attributes:
        county (str): The name of the county.

        county_data_folder (str): The path to the directory where county 
        data is stored.

        main_dataframe_path (os.path): The path to the main dataframe
        file.

        subdivision_lookup_path (os.path): The path to the subdivision 
        lookup file.

        main_dataframe (pd.DataFrame): The main dataframe containing 
        parcel data.

        subdivision_lookup_dataframe (pd.DataFrame): The dataframe 
        containing subdivision data.
    '''
    county_data_folder = os.path.join(
        CountyDataframe.data_folder, 'sarasota')

    main_dataframe_path = os.path.join(
        county_data_folder, 'Parcel_Sales_CSV', 'Sarasota.gzip')

    subdivision_lookup_path = os.path.join(
        county_data_folder, 'SubDivisionIndex.gzip')

    main_dataframe = gzipconverter.GZIPConverter.open_df(
        main_dataframe_path, 'gzip')

    subdivision_lookup_dataframe = gzipconverter.GZIPConverter.open_df(
        subdivision_lookup_path, 'gzip')

    def __init__(self, parcel_id: str):
        '''Initialize a new Sarasota county dataframe object with the
        specified parcel ID.

        Args:
            parcel_id (str): The parcel ID to retrieve data for.

        Raises:
            ValueError: If an invalid parcel ID is provided.
        '''
        self.parcel_id = parcel_id
        self.county = 'sarasota'
        self.main_dataframe = Sarasota.main_dataframe
        self.subdivision_lookup_dataframe = Sarasota.subdivision_lookup_dataframe
        self.parcel_data = CountyDataframe.parcel_data_structure
        self.find_parcel_data()

    def find_parcel_data(self) -> None:
        '''Retrieve parcel data associated with the specified parcel ID
        and update parcel data dictionary.

        Returns:
            None.
        '''
        parcel_dataframe = self.main_dataframe.loc[
            self.main_dataframe['Parcel ID'] == self.parcel_id]

        self.find_location_data(parcel_dataframe)
        self.find_subdivision_data(parcel_dataframe)
        self.find_legal_data(parcel_dataframe)

    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Extract and format location data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        '''
        address_number = misc.convert_to_string(parcel_dataframe['LOCN'])
        street = misc.convert_to_string(parcel_dataframe['LOCS'])
        direction = misc.convert_to_string(parcel_dataframe['LOCD'])
        city = misc.convert_to_string(parcel_dataframe['LOCCITY'])
        zip_code = misc.convert_to_string(parcel_dataframe['LOCZIP'])

        self.parcel_data['address_number'] = address_number
        self.parcel_data['street'] = street
        self.parcel_data['city'] = city
        self.parcel_data['zip_code'] = zip_code

        if direction != 'NAN':
            address = f'{address_number} {direction} {street}'
            self.parcel_data['direction'] = direction
        else:
            address = f'{address_number} {street}'
        self.parcel_data['address'] = address

    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Extract and format subdivision data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        '''
        subdivision_code = misc.convert_to_string(parcel_dataframe['SUBD'])
        if not subdivision_code:
            self.parcel_data['property_type'] = 'Metes & Bounds'
            return

        subdivision_dataframe = self.subdivision_lookup_dataframe.loc[
            self.subdivision_lookup_dataframe['Number'] == subdivision_code]

        lot = misc.convert_to_string(parcel_dataframe['LOT'])
        block = misc.convert_to_string(parcel_dataframe['BLOCK'])
        unit = misc.convert_to_string(parcel_dataframe['UNIT'])
        subdivision = misc.convert_to_string(subdivision_dataframe['Name'])
        plat_book = misc.convert_to_string(subdivision_dataframe['PlatBk1'])
        plat_page = misc.convert_to_string(subdivision_dataframe['PlatPg1'])

        if lot != 'NAN':
            self.parcel_data['lot'] = lot
        if block != 'NAN':
            self.parcel_data['block'] = block

        if subdivision != 'NAN':
            self.parcel_data['subdivision'] = subdivision
            self.parcel_data['plat_book'] = plat_book
            self.parcel_data['plat_page'] = plat_page

        if unit != 'NAN':
            self.parcel_data['property_type'] = 'Condo'
            self.parcel_data['unit'] = unit
        else:
            self.parcel_data['property_type'] = 'Subdivision'

    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Finds and formats legal data for the parcel data dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): A dataframe containing
            parcel data.

        Returns:
            None.
        '''
        or_book = misc.convert_to_string(parcel_dataframe['OR_BOOK'])
        or_page = misc.convert_to_string(parcel_dataframe['OR_PAGE'])
        or_instrument = misc.convert_to_string(parcel_dataframe['LEGALREFER'])
        legal_description_columns = ['LEGAL1', 'LEGAL2', 'LEGAL3', 'LEGAL4']
        legal_description = ''
        for column in legal_description_columns:
            line = misc.convert_to_string(parcel_dataframe[column])
            if line:
                legal_description = f'{legal_description} {line}'

        legal_description = misc.convert_to_string(legal_description)

        if or_book != 'NAN':
            self.parcel_data['or_book'] = or_book

        if or_page != 'NAN':
            self.parcel_data['or_page'] = or_page

        if or_instrument != 'NAN':
            self.parcel_data['or_instrument'] = or_instrument

        self.parcel_data['legal_description'] = legal_description


class Manatee(CountyDataframe):
    '''Concrete county dataframe class representing Manatee county.

    This class implements the abstract `find_parcel_data` method and
    contains methods to extract data of a given parcel.

    Attributes:
        county (str): The name of the county.

        county_data_folder (str): The path to the directory where county 
        data is stored.

        main_dataframe_path (os.path): The path to the main dataframe
        file.

        subdivision_lookup_path (os.path): The path to the subdivision 
        lookup file.

        main_dataframe (pd.DataFrame): The main dataframe containing 
        parcel data.

        subdivision_lookup_dataframe (pd.DataFrame): The dataframe 
        containing subdivision data.
    '''
    county_data_folder = os.path.join(
        CountyDataframe.data_folder, 'manatee')

    main_dataframe_path = os.path.join(
        county_data_folder, 'manatee_ccdf.gzip')

    subdivision_lookup_path = os.path.join(
        county_data_folder, 'subdivisions_in_manatee.gzip')

    main_dataframe = gzipconverter.GZIPConverter.open_df(
        main_dataframe_path, 'gzip')

    subdivision_lookup_dataframe = gzipconverter.GZIPConverter.open_df(
        subdivision_lookup_path, 'gzip')

    def __init__(self, parcel_id: str):
        '''Initialize a new Manatee county dataframe object with the
        specified parcel ID.

        Args:
            parcel_id (str): The parcel ID to retrieve data for.

        Raises:
            ValueError: If an invalid parcel ID is provided.
        '''
        self.parcel_id = parcel_id
        self.county = 'manatee'
        self.main_dataframe = Manatee.main_dataframe
        self.subdivision_lookup_dataframe = Manatee.subdivision_lookup_dataframe
        self.parcel_data = CountyDataframe.parcel_data_structure
        self.find_parcel_data()

    def find_parcel_data(self) -> None:
        '''Retrieve parcel data associated with the specified parcel ID
        and update parcel data dictionary.

        Returns:
            None.
        '''
        parcel_dataframe = self.main_dataframe.loc[
            self.main_dataframe['Parcel ID'] == self.parcel_id]

        self.find_location_data(parcel_dataframe)
        self.find_subdivision_data(parcel_dataframe)
        self.find_legal_data(parcel_dataframe)

    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Extract and format location data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        '''
        address_number = misc.convert_to_string(
            parcel_dataframe['SITUS_ADDRESS_NUM'])
        street_name = misc.convert_to_string(
            parcel_dataframe['SITUS_STREET_NAME'])
        street_suffix = misc.convert_to_string(
            parcel_dataframe['SITUS_STREET_SUF'])
        street = f'{street_name} {street_suffix}'
        address = misc.convert_to_string(parcel_dataframe['SITUS_ADDRESS'])
        city = misc.convert_to_string(parcel_dataframe['SITUS_POSTAL_CITY'])
        zip_code = misc.convert_to_string(parcel_dataframe['SITUS_POSTAL_ZIP'])
        direction = misc.convert_to_string(parcel_dataframe['SITUS_POSTDIR'])

        self.parcel_data['address_number'] = address_number
        self.parcel_data['street'] = street
        self.parcel_data['address'] = address
        self.parcel_data['city'] = city
        self.parcel_data['zip_code'] = zip_code

        if direction:
            self.parcel_data['direction'] = direction

    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Extract and format subdivision data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        '''
        subdivision_code = misc.convert_to_string(
            parcel_dataframe['PAR_SUBDIVISION'])
        print(subdivision_code)
        if not subdivision_code:
            self.parcel_data['property_type'] = 'Metes & Bounds'
            return

        subdivision_dataframe = self.subdivision_lookup_dataframe.loc[
            self.subdivision_lookup_dataframe['SUBDNUM'] == subdivision_code]

        lot = misc.convert_to_string(parcel_dataframe['PAR_SUBDIV_LOT'])
        block = misc.convert_to_string(parcel_dataframe['PAR_SUBDIV_BLOCK'])
        subdivision = misc.convert_to_string(subdivision_dataframe['NAME'])
        plat_book = misc.convert_to_string(subdivision_dataframe['BOOK'])
        plat_page = misc.convert_to_string(subdivision_dataframe['PAGE'])
        property_type = misc.convert_to_string(subdivision_dataframe['TYPE'])

        self.parcel_data['block'] = block
        self.parcel_data['subdivision'] = subdivision
        self.parcel_data['plat_book'] = plat_book
        self.parcel_data['plat_page'] = plat_page

        if 'CONDO' in property_type:
            self.parcel_data['property_type'] = 'Condo'
            self.parcel_data['unit'] = lot
        else:
            self.parcel_data['property_type'] = 'Subdivision'
            self.parcel_data['lot'] = lot

    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        '''Finds and formats legal data for the parcel data dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): A dataframe containing
            parcel data.

        Returns:
            None.
        '''
        or_book = misc.convert_to_string(parcel_dataframe['SALE_BOOK_LAST'])
        or_page = misc.convert_to_string(parcel_dataframe['SALE_PAGE_LAST'])
        or_instrument = misc.convert_to_string(
            parcel_dataframe['SALE_INSTRNO_LAST'])
        legal_description_columns = ['PAR_LEGAL1', 'PAR_LEGAL2', 'PAR_LEGAL3']
        legal_description = ''
        for column in legal_description_columns:
            line = misc.convert_to_string(parcel_dataframe[column])
            if line:
                legal_description = f'{legal_description} {line}'

        legal_description = misc.convert_to_string(legal_description)

        if or_book:
            self.parcel_data['or_book'] = or_book

        if or_page:
            self.parcel_data['or_page'] = or_page

        if or_instrument:
            self.parcel_data['or_instrument'] = or_instrument

        self.parcel_data['legal_description'] = legal_description
