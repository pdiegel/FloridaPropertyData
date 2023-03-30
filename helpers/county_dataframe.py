"""This module provides a set of classes for accessing county property
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


"""
from abc import ABC, abstractmethod
import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from ..helpers import misc, gzipconverter
from ..logger import logger
from ..config import COUNTY_DATABASE_DIR


class CountyDataframe(ABC):
    """Abstract base class representing a county property dataframe.

    This class defines an interface for accessing county property data.
    To create a new county dataframe class, you should subclass this
    class and implement the abstract `find_parcel_data` method.
    """

    parcel_data_structure = {
        "address_number": "",
        "street": "",
        "direction": "",
        "address": "",
        "city": "",
        "zip_code": "",
        "subdivision": "",
        "lot": "",
        "block": "",
        "unit": "",
        "plat_book": "",
        "plat_page": "",
        "property_type": "",
        "or_book": "",
        "or_page": "",
        "or_instrument": "",
        "legal_description": "",
    }
    fema_url = "https://msc.fema.gov/portal/search?AddressQuery={}\
#searchresultsanchor"

    @abstractmethod
    def find_parcel_data(self) -> None:
        """Abstract method that retrieves the parcel data associated
        with the parcel ID.

        This method should retrieve the parcel data associated with the
        parcel ID specified in the constructor and return it as a
        dictionary.

        Returns:
            A dictionary containing the parcel data.
        """

    @abstractmethod
    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Abstract method that finds and formats address data for a
        given parcel."""

    @abstractmethod
    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Abstract method that finds and formats property type and
        subdivision data for a given parcel."""

    @abstractmethod
    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Abstract method that finds and formats legal description data
        for a given parcel."""

    @abstractmethod
    def find_links(self) -> dict:
        """
        Abstract method that finds relevant links for a given object
        instance and its associated data.

        Returns:
            A dictionary containing relevant links related to the object
            instance and its associated data.
            The dictionary should contain the following keys:
                - 'property appraiser': URL to view the property
                appraisal information
                - 'property map': URL to view the property on a map
                - 'fema': URL to view the property's FEMA information
                - 'deed': URL to view the property's deed information
                - 'subdivision': URL to view the property's subdivision
                information, if available
                - Other keys as relevant for the particular
                implementation of the method

        Raises:
            NotImplementedError: This method is not implemented.
        """
        pass


class Sarasota(CountyDataframe):
    """Concrete county dataframe class representing Sarasota county.

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
    """

    county_data_folder = os.path.join(COUNTY_DATABASE_DIR, "sarasota")

    main_dataframe_path = os.path.join(
        county_data_folder, "Parcel_Sales_CSV", "Sarasota.gzip"
    )

    subdivision_lookup_path = os.path.join(
        county_data_folder, "SubDivisionIndex.gzip"
    )

    main_dataframe = gzipconverter.GZIPConverter.open_df(
        main_dataframe_path, "gzip"
    )

    subdivision_lookup_df = gzipconverter.GZIPConverter.open_df(
        subdivision_lookup_path, "gzip"
    )

    def __init__(self, parcel_id: str):
        """Initialize a new Sarasota county dataframe object with the
        specified parcel ID.

        Args:
            parcel_id (str): The parcel ID to retrieve data for.

        Raises:
            ValueError: If an invalid parcel ID is provided.
        """
        self.parcel_id = parcel_id
        self.county = "sarasota"
        self.main_dataframe = Sarasota.main_dataframe
        self.subdivision_lookup_dataframe = Sarasota.subdivision_lookup_df
        self.parcel_data = CountyDataframe.parcel_data_structure
        self.find_parcel_data()
        self.links = self.find_links()

    def find_parcel_data(self) -> None:
        """Retrieve parcel data associated with the specified parcel ID
        and update parcel data dictionary.

        Returns:
            None.
        """
        parcel_dataframe = self.main_dataframe.loc[
            self.main_dataframe["Parcel ID"] == self.parcel_id
        ]

        self.find_location_data(parcel_dataframe)
        self.find_subdivision_data(parcel_dataframe)
        self.find_legal_data(parcel_dataframe)

    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format location data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        address_number = misc.convert_to_string(parcel_dataframe["LOCN"])
        street = misc.convert_to_string(parcel_dataframe["LOCS"])
        direction = misc.convert_to_string(parcel_dataframe["LOCD"])
        city = misc.convert_to_string(parcel_dataframe["LOCCITY"])
        zip_code = misc.convert_to_string(parcel_dataframe["LOCZIP"])

        self.parcel_data["address_number"] = address_number
        self.parcel_data["street"] = street
        self.parcel_data["city"] = city
        self.parcel_data["zip_code"] = zip_code

        if direction != "NAN":
            address = f"{address_number} {direction} {street}"
            self.parcel_data["direction"] = direction
        else:
            address = f"{address_number} {street}"
        self.parcel_data["address"] = address

    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format subdivision data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        subdivision_code = misc.convert_to_string(parcel_dataframe["SUBD"])
        if subdivision_code:
            while len(subdivision_code) < 4:
                subdivision_code = f"0{subdivision_code}"
            logger.info("Subdivision Code = %s", subdivision_code)
        else:
            self.parcel_data["property_type"] = "Metes & Bounds"
            return

        subdivision_dataframe = self.subdivision_lookup_dataframe.loc[
            self.subdivision_lookup_dataframe["Number"] == subdivision_code
        ]

        lot = misc.convert_to_string(parcel_dataframe["LOT"])
        block = misc.convert_to_string(parcel_dataframe["BLOCK"])
        unit = misc.convert_to_string(parcel_dataframe["UNIT"])
        subdivision = misc.convert_to_string(subdivision_dataframe["Name"])
        plat_book = misc.convert_to_string(subdivision_dataframe["PlatBk1"])
        plat_page = misc.convert_to_string(subdivision_dataframe["PlatPg1"])

        if len(plat_book) > 3:
            plat_book = misc.convert_to_string(
                subdivision_dataframe["PlatBk2"]
            )
        if len(plat_page) > 3:
            plat_page = misc.convert_to_string(
                subdivision_dataframe["PlatPg2"]
            )

        if lot != "NAN":
            self.parcel_data["lot"] = lot
        if block != "NAN":
            self.parcel_data["block"] = block

        if subdivision != "NAN":
            self.parcel_data["subdivision"] = subdivision
            self.parcel_data["plat_book"] = plat_book
            self.parcel_data["plat_page"] = plat_page

        if unit != "NAN":
            self.parcel_data["property_type"] = "Condo"
            self.parcel_data["unit"] = unit
        else:
            self.parcel_data["property_type"] = "Subdivision"

    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Finds and formats legal data for the parcel data dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): A dataframe containing
            parcel data.

        Returns:
            None.
        """
        or_book = misc.convert_to_string(parcel_dataframe["OR_BOOK"])
        or_page = misc.convert_to_string(parcel_dataframe["OR_PAGE"])
        or_instrument = misc.convert_to_string(parcel_dataframe["LEGALREFER"])
        legal_description_columns = ["LEGAL1", "LEGAL2", "LEGAL3", "LEGAL4"]
        legal_description = ""
        for column in legal_description_columns:
            line = misc.convert_to_string(parcel_dataframe[column])
            if line:
                legal_description = f"{legal_description} {line}"

        legal_description = misc.convert_to_string(legal_description)

        if or_book != "NAN":
            self.parcel_data["or_book"] = or_book

        if or_page != "NAN":
            self.parcel_data["or_page"] = or_page

        if or_instrument != "NAN":
            self.parcel_data["or_instrument"] = or_instrument

        self.parcel_data["legal_description"] = legal_description

    def find_links(self):
        """
                Method to find relevant links for a given parcel ID and
                its associated data.

        Args:
            self: An instance of the class that this method belongs to.

        Returns:
            A dictionary containing relevant links related to the given
            parcel ID and its associated data.
            The dictionary contains the following keys:
                - 'property appraiser': URL to view the property
                appraisal information
                - 'property map': URL to view the property on a map
                - 'fema': URL to view the property's FEMA information
                - 'deed': URL to view the property's deed information
                - 'subdivision': URL to view the property's subdivision
                information, if available
                - 'condo': URL to view the property's condo information,
                if available

            If 'deed' information cannot be found, its value in the
            returned dictionary is an empty string.
            If 'subdivision' information is not available, its value in
            the returned dictionary is None.

        Notes:
            - The URLs for 'property appraiser', 'property map', 'fema',
            'deed', 'subdivision', and 'condo' are specific to the
            Sarasota County in Florida.
            - The links returned are based on the parcel ID and
            associated data passed to the method.

        """
        appraiser_url = "https://www.sc-pa.com/propertysearch/parcel/\
details/{}"
        apraiser_map_url = "https://ags3.scgov.net/scpa/?esearch={}&slayer=0"
        clerk_of_court_url = "https://secure.sarasotaclerk.com/viewtiff.aspx?"
        or_instrument_deed_url = clerk_of_court_url + "intrnum={}"
        or_book_page_deed_url = clerk_of_court_url + "book={}&page={}"
        subdivision_url = clerk_of_court_url + "intrnum=SUBDIVBK{}PG{}"
        condo_url = clerk_of_court_url + "intrnum=CONDOBK{}PG{}"

        parcel_id = self.parcel_id
        parcel_data = self.parcel_data
        links = {}
        links["property appraiser"] = appraiser_url.format(parcel_id)
        links["property map"] = apraiser_map_url.format(parcel_id)

        fema_address = f'{parcel_data["address"]}\
 {parcel_data["city"]}'.replace(
            " ", "%20"
        )
        links["fema"] = CountyDataframe.fema_url.format(fema_address)

        or_book = parcel_data["or_book"]
        is_condo = bool(parcel_data["unit"])

        if or_book:
            or_page = parcel_data["or_page"]
            links["deed"] = or_book_page_deed_url.format(or_book, or_page)
        else:
            or_inst = parcel_data["or_instrument"]
            links["deed"] = or_instrument_deed_url.format(or_inst)

        if is_condo:
            links["condo"] = condo_url.format(
                parcel_data["plat_book"], parcel_data["plat_page"]
            )
        else:
            if parcel_data["plat_book"]:
                links["subdivision"] = subdivision_url.format(
                    parcel_data["plat_book"], parcel_data["plat_page"]
                )
            else:
                links["subdivision"] = None

        return links


class Manatee(CountyDataframe):
    """Concrete county dataframe class representing Manatee county.

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
    """

    county_data_folder = os.path.join(COUNTY_DATABASE_DIR, "manatee")

    main_dataframe_path = os.path.join(county_data_folder, "manatee_ccdf.gzip")

    subdivision_lookup_path = os.path.join(
        county_data_folder, "subdivisions_in_manatee.gzip"
    )

    main_dataframe = gzipconverter.GZIPConverter.open_df(
        main_dataframe_path, "gzip"
    )

    subdivision_lookup_df = gzipconverter.GZIPConverter.open_df(
        subdivision_lookup_path, "gzip"
    )

    def __init__(self, parcel_id: str):
        """Initialize a new Manatee county dataframe object with the
        specified parcel ID.

        Args:
            parcel_id (str): The parcel ID to retrieve data for.

        Raises:
            ValueError: If an invalid parcel ID is provided.
        """
        self.parcel_id = parcel_id
        self.county = "manatee"
        self.main_dataframe = Manatee.main_dataframe
        self.subdivision_lookup_dataframe = Manatee.subdivision_lookup_df
        self.parcel_data = CountyDataframe.parcel_data_structure
        self.find_parcel_data()
        self.links = self.find_links()

    def find_parcel_data(self) -> None:
        """Retrieve parcel data associated with the specified parcel ID
        and update parcel data dictionary.

        Returns:
            None.
        """
        parcel_dataframe = self.main_dataframe.loc[
            self.main_dataframe["Parcel ID"] == self.parcel_id
        ]

        self.find_location_data(parcel_dataframe)
        self.find_subdivision_data(parcel_dataframe)
        self.find_legal_data(parcel_dataframe)

    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format location data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        address_number = misc.convert_to_string(
            parcel_dataframe["SITUS_ADDRESS_NUM"]
        )
        street_name = misc.convert_to_string(
            parcel_dataframe["SITUS_STREET_NAME"]
        )
        street_suffix = misc.convert_to_string(
            parcel_dataframe["SITUS_STREET_SUF"]
        )
        street = f"{street_name} {street_suffix}"
        address = misc.convert_to_string(parcel_dataframe["SITUS_ADDRESS"])
        city = misc.convert_to_string(parcel_dataframe["SITUS_POSTAL_CITY"])
        zip_code = misc.convert_to_string(parcel_dataframe["SITUS_POSTAL_ZIP"])
        direction = misc.convert_to_string(parcel_dataframe["SITUS_POSTDIR"])

        self.parcel_data["address_number"] = address_number
        self.parcel_data["street"] = street
        self.parcel_data["address"] = address
        self.parcel_data["city"] = city
        self.parcel_data["zip_code"] = zip_code

        if direction:
            self.parcel_data["direction"] = direction

    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format subdivision data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        subdivision_code = misc.convert_to_string(
            parcel_dataframe["PAR_SUBDIVISION"]
        )
        if not subdivision_code:
            self.parcel_data["property_type"] = "Metes & Bounds"
            return

        subdivision_dataframe = self.subdivision_lookup_dataframe.loc[
            self.subdivision_lookup_dataframe["SUBDNUM"] == subdivision_code
        ]

        lot = misc.convert_to_string(parcel_dataframe["PAR_SUBDIV_LOT"])
        block = misc.convert_to_string(parcel_dataframe["PAR_SUBDIV_BLOCK"])
        subdivision = misc.convert_to_string(subdivision_dataframe["NAME"])
        plat_book = misc.convert_to_string(subdivision_dataframe["BOOK"])
        plat_page = misc.convert_to_string(subdivision_dataframe["PAGE"])
        property_type = misc.convert_to_string(subdivision_dataframe["TYPE"])

        self.parcel_data["block"] = block
        self.parcel_data["subdivision"] = subdivision
        self.parcel_data["plat_book"] = plat_book
        self.parcel_data["plat_page"] = plat_page

        if "CONDO" in property_type:
            self.parcel_data["property_type"] = "Condo"
            self.parcel_data["unit"] = lot
        else:
            self.parcel_data["property_type"] = "Subdivision"
            self.parcel_data["lot"] = lot

    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Finds and formats legal data for the parcel data dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): A dataframe containing
            parcel data.

        Returns:
            None.
        """
        or_book = misc.convert_to_string(parcel_dataframe["SALE_BOOK_LAST"])
        or_page = misc.convert_to_string(parcel_dataframe["SALE_PAGE_LAST"])
        or_instrument = misc.convert_to_string(
            parcel_dataframe["SALE_INSTRNO_LAST"]
        )
        legal_description_columns = ["PAR_LEGAL1", "PAR_LEGAL2", "PAR_LEGAL3"]
        legal_description = ""
        for column in legal_description_columns:
            line = misc.convert_to_string(parcel_dataframe[column])
            if line:
                legal_description = f"{legal_description} {line}"

        legal_description = misc.convert_to_string(legal_description)

        if or_book:
            self.parcel_data["or_book"] = or_book

        if or_page:
            self.parcel_data["or_page"] = or_page

        if or_instrument:
            self.parcel_data["or_instrument"] = or_instrument

        self.parcel_data["legal_description"] = legal_description

    def find_links(self) -> dict:
        """
        Method to find relevant links for a given parcel ID and its
        associated data.

        Args:
        self: An instance of the class that this method belongs to.

        Returns:
        A dictionary containing relevant links related to the given
        parcel ID and its associated data.
        The dictionary contains the following keys:
        - 'property appraiser': URL to view the property appraisal
        information
        - 'property map': Empty string
        - 'fema': URL to view the property's FEMA information
        - 'deed': URL to view the property's deed information
        - 'subdivision': URL to view the property's subdivision
        information, if available

        If 'deed' information cannot be found, its value in the returned
        dictionary is an empty string.
        If 'subdivision' information is not available, its value in the
        returned dictionary is an empty string.
        """

        appraiser_url = "https://www.manateepao.gov/parcel/?parid={}"
        or_instrument_deed_url = "https://records.manateeclerk.com/\
OfficialRecords/Search/InstrumentNumber?instrumentNumber={}"
        or_book_page_deed_url = "https://records.manateeclerk.com/\
OfficialRecords/Search/InstrumentBookPage/{}/{}/"
        subdivision_url = "https://records.manateeclerk.com/PlatRecords/Search\
/Results?searchType=plat&platBook={}&platPage={}&page=0&pageSize=0"

        parcel_id = self.parcel_id
        parcel_data = self.parcel_data
        links = {}
        links["property appraiser"] = appraiser_url.format(parcel_id)
        links["property map"] = ""
        fema_address = f'{parcel_data["address"]}\
 {parcel_data["city"]}'.replace(
            " ", "%20"
        )
        links["fema"] = CountyDataframe.fema_url.format(fema_address)

        or_book = parcel_data.get("or_book", "")
        or_page = parcel_data.get("or_page", "")
        or_inst = parcel_data.get("or_instrument", "")
        plat_book = parcel_data.get("plat_book", "")
        plat_page = parcel_data.get("plat_page", "")

        if or_book and or_page:
            links["deed"] = or_book_page_deed_url.format(or_book, or_page)
        elif or_inst:
            links["deed"] = or_instrument_deed_url.format(or_inst)
        else:
            links["deed"] = ""

        if plat_book:
            links["subdivision"] = subdivision_url.format(plat_book, plat_page)
        else:
            links["subdivision"] = ""

        return links


class Charlotte(CountyDataframe):
    """Concrete county dataframe class representing Charlotte county.

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
    """

    county_data_folder = os.path.join(COUNTY_DATABASE_DIR, "charlotte")

    main_dataframe_path = os.path.join(county_data_folder, "cd.gzip")

    main_dataframe = gzipconverter.GZIPConverter.open_df(
        main_dataframe_path, "gzip", "|"
    )

    subdivision_lookup_path = os.path.join(
        county_data_folder, "subdivisions.gzip"
    )

    subdivision_lookup_df = gzipconverter.GZIPConverter.open_df(
        subdivision_lookup_path, "gzip"
    )

    def __init__(self, parcel_id: str):
        """Initialize a new Charlotte county dataframe object with the
        specified parcel ID.

        Args:
            parcel_id (str): The parcel ID to retrieve data for.

        Raises:
            ValueError: If an invalid parcel ID is provided.
        """
        self.parcel_id = parcel_id
        self.county = "charlotte"
        self.appraiser_link = f"https://www.ccappraiser.com/Show_Parcel.asp?\
acct={parcel_id}%20%20&gen=T&tax=T&bld=T&oth=T&sal=T&lnd=T&leg=T"
        self.main_dataframe = Charlotte.main_dataframe
        self.subdivision_lookup_dataframe = Charlotte.subdivision_lookup_df
        self.parcel_data = CountyDataframe.parcel_data_structure
        self.find_parcel_data()
        self.links = self.find_links()

    def find_parcel_data(self) -> None:
        """Retrieve parcel data associated with the specified parcel ID
        and update parcel data dictionary.

        Returns:
            None.
        """
        parcel_dataframe = self.main_dataframe.loc[
            self.main_dataframe["Parcel ID"] == self.parcel_id
        ]

        self.find_location_data(parcel_dataframe)
        self.find_subdivision_data(parcel_dataframe)
        self.find_legal_data(parcel_dataframe)

    def find_location_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format location data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        address_number = misc.convert_to_string(
            parcel_dataframe["streetnumber"]
        )
        street_name = misc.convert_to_string(parcel_dataframe["streetname"])
        if address_number:
            address = f"{address_number} {street_name}"
        else:
            address = street_name
        city = misc.convert_to_string(self.find_city())
        zip_code = misc.convert_to_string(parcel_dataframe["padZip"])

        self.parcel_data["address_number"] = address_number
        self.parcel_data["city"] = city
        self.parcel_data["street"] = street_name
        self.parcel_data["address"] = address
        self.parcel_data["zip_code"] = zip_code

    def find_subdivision_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Extract and format subdivision data for the parcel data
        dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): The dataframe containing
            parcel data.

        Returns:
            None.
        """
        # The format of short_legal is subdivision, section, block, lot and
        # must adhere to the following layout: pch 011 1337 0002
        short_legal = misc.convert_to_string(parcel_dataframe["shortlegal"])
        logger.info("Legal Description = %s", short_legal)
        if "ZZZ" in short_legal:
            self.parcel_data["property_type"] = "Metes & Bounds"
            return

        subdivision_code = short_legal[:3]
        section = short_legal[4:7].lstrip("0")
        block = short_legal[8:12].lstrip("0")
        lot = short_legal[13:17].lstrip("0")

        subdivision_dataframe = self.subdivision_lookup_dataframe.loc[
            self.subdivision_lookup_dataframe["Designator"] == subdivision_code
        ]

        mask = subdivision_dataframe["Subdivision Name"].str.contains(section)
        subdivision_row = subdivision_dataframe[mask]

        subdivision = misc.convert_to_string(
            subdivision_row["Subdivision Name"]
        )
        if subdivision:
            self.parcel_data["property_type"] = "Subdivision"
            self.parcel_data["subdivision"] = subdivision
            try:
                plat_book, plat_page = self.find_plat_information(subdivision)
                self.parcel_data["plat_book"] = plat_book
                self.parcel_data["plat_page"] = plat_page
            except ValueError as error:
                logger.error(error)
                # Handle the error here, such as by displaying a message
                # to the user or exiting the program
            else:
                # Use the plat_book and plat_page values here
                logger.info(
                    "Plat information found: Book %s,\
 Page %s",
                    plat_book,
                    plat_page,
                )

        self.parcel_data["lot"] = lot
        self.parcel_data["block"] = block

    def find_legal_data(self, parcel_dataframe: pd.DataFrame) -> None:
        """Finds and formats legal data for the parcel data dictionary.

        Args:
            parcel_dataframe (pd.DataFrame): A dataframe containing
            parcel data.

        Returns:
            None.
        """
        or_book = misc.convert_to_string(parcel_dataframe["SaleBook"])
        or_page = misc.convert_to_string(parcel_dataframe["SalePage"])
        or_instrument = misc.convert_to_string(
            parcel_dataframe["InstrumentNumber"]
        )
        legal_description = misc.convert_to_string(
            parcel_dataframe["longlegal"]
        )

        if or_book:
            self.parcel_data["or_book"] = or_book

        if or_page:
            self.parcel_data["or_page"] = or_page

        if or_instrument:
            self.parcel_data["or_instrument"] = or_instrument

        self.parcel_data["legal_description"] = legal_description

    def find_city(self):
        """Extracts the city name from the property information page of
        an appraiser website.

        Returns:
            A string containing the name of the city where the property
            is located.

        Raises:
            requests.exceptions.RequestException: If an error occurs
            while fetching the property information page.
        """
        url = self.appraiser_link
        page = requests.get(url, timeout=5)
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find("strong", text=re.compile("Property City & Zip:"))
        return results.parent.parent.find_all("div")[1].text[:-6].strip()

    def find_plat_information(self, subdivision_name: str) -> tuple[str, str]:
        """Searches the Charlotte Clerk of Courts website for plat
        information for the given subdivision name.

        Args:
            subdivision_name (str): The name of the subdivision to
            search for.

        Returns:
            tuple[str, str]: A tuple containing the book number and page
            number for the subdivision's plat information.

        Raises:
            ValueError: If no plat information is found for the given
            subdivision name.

        """
        with sync_playwright() as playwright:
            with playwright.chromium.launch() as browser:
                with browser.new_context() as context:
                    page = context.new_page()
                    page.goto(
                        "https://clerkportal.charlotteclerk.com/PlatCondo\
/PlatCondoIndex"
                    )
                    page.locator(".k-header").get_by_role("listbox").click()
                    page.locator("#searchPlatDroptDown_listbox").get_by_text(
                        "Description"
                    ).click()
                    page.locator("#searchPlatText").fill(subdivision_name)
                    page.locator("#searchPlatButton").click()
                    page.wait_for_timeout(50)
                    plat_book = (
                        page.locator(".k-grid-content")
                        .locator("td")
                        .nth(1)
                        .inner_text()
                    )
                    plat_page = (
                        page.locator(".k-grid-content")
                        .locator("td")
                        .nth(2)
                        .inner_text()
                    )

                    if not plat_book or not plat_page:
                        raise ValueError("Unable to find plat information")

                    return plat_book, plat_page

    def find_links(self) -> dict:
        """
        Method to find relevant links for a given parcel ID and its
        associated data.

        Args:
        self: An instance of the class that this method belongs to.

        Returns:
        A dictionary containing relevant links related to the given
        parcel ID and its associated data.
        The dictionary contains the following keys:
        - 'property appraiser': URL to view the property appraisal
        information
        - 'property map': URL to view the property on a map
        - 'fema': URL to view the property's FEMA information
        - 'deed': URL to view the property's deed information
        - 'subdivision': Empty string

        If 'deed' information cannot be found, its value in the returned
        dictionary is an empty string.
        """

        appraiser_url = "https://www.ccappraiser.com/Show_Parcel.asp?acct={}\
%20%20&gen=T&tax=T&bld=T&oth=T&sal=T&lnd=T&leg=T"
        property_map_url = "https://agis.charlottecountyfl.gov/ccgis/?acct={}"
        or_instrument_deed_url = "https://recording.charlotteclerk.com/Render/\
ViewDocuments?inFromSearch=INSTRUMENTNUMBER&inDirectReverse=&inFirstName=&inLa\
stName=&inMiddleName=&inBusinessName=&inBookType=&inBook=&inPage=&inStartDate=\
&inEndDate=&inCaseNumber=&inInstrumentNumber={}&inLegal=&inDocumentTypeIds="
        or_book_page_deed_url = "https://recording.charlotteclerk.com/Render/V\
iewDocuments?inFromSearch=BOOKPAGE&inDirectReverse=&inFilterCriteria=&inCompre\
ssedName=&inBookType=O&inBook={}&inPage={}&inStartDate=&inEndDate=&inCaseNu\
mber=&inInstrumentNumber=&inLegal=&inDocumentTypeIds="

        parcel_id = self.parcel_id
        parcel_data = self.parcel_data
        links = {}
        links["property appraiser"] = appraiser_url.format(parcel_id)
        links["property map"] = property_map_url.format(parcel_id)
        fema_address = f'{parcel_data["address"]}\
 {parcel_data["city"]}'.replace(
            " ", "%20"
        )
        links["fema"] = CountyDataframe.fema_url.format(fema_address)

        or_book = parcel_data.get("or_book", "")
        or_page = parcel_data.get("or_page", "")
        or_inst = parcel_data.get("or_instrument", "")

        if or_book and or_page:
            links["deed"] = or_book_page_deed_url.format(or_book, or_page)
        elif or_inst:
            links["deed"] = or_instrument_deed_url.format(or_inst)
        else:
            links["deed"] = ""

        links["subdivision"] = ""

        return links
