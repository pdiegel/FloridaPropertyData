import pandas as pd
import os


class Parcel:
    def __init__(self, county, df):
        self.county = county
        self.parcel_id = ''
        self.property_address = ''
        self.city = ''
        self.zip_code = ''
        self.legal_description = ''
        self.lot = ''
        self.block = ''
        self.subdivision_code = ''
        self.subdivision_name = ''
        self.or_book = ''
        self.or_page = ''
        self.or_inst = ''
        self.get_parcel_attributes(df)

    def get_sarasota_subdivision(self, subdivision_code):
        subdivision_index_df_path = os.path.join(
            'data', 'sarasota', 'SubDivisionIndex.gzip')
        subdivision_index_df = pd.read_csv(
            subdivision_index_df_path, compression='gzip', dtype=str,
            usecols=['Number', 'Name', 'PlatPg1', 'PlatBk1'])

        parcel_subdivision_data = subdivision_index_df.loc[
            subdivision_index_df['Number'] == subdivision_code]

        plat_book = parcel_subdivision_data.iloc[0]['PlatBk1']
        plat_page = parcel_subdivision_data.iloc[0]['PlatPg1']
        subdivision_name = parcel_subdivision_data.iloc[0]['Name']
        return [plat_book, plat_page, subdivision_name]

    def get_parcel_attributes(self, df):
        if self.county == 'Manatee':
            self.parcel_id = df.iloc[0]['Parcel ID']
            self.property_address = df.iloc[0]['SITUS_ADDRESS']
            self.city = df.iloc[0]['SITUS_POSTAL_CITY']
            self.zip_code = df.iloc[0]['SITUS_POSTAL_ZIP']
            self.legal_description = df.iloc[0]['PAR_LEGAL1']
            self.lot = df.iloc[0]['PAR_SUBDIV_LOT']
            self.block = df.iloc[0]['PAR_SUBDIV_BLOCK']
            self.subdivision_code = df.iloc[0]['PAR_SUBDIVISION']
            self.subdivision_name = df.iloc[0]['PAR_SUBDIV_NAME']
            self.or_book = df.iloc[0]['SALE_BOOK_LAST']
            self.or_page = df.iloc[0]['SALE_PAGE_LAST']
            self.or_inst = df.iloc[0]['SALE_INSTRNO_LAST']
        elif self.county == 'Sarasota':
            self.parcel_id = df.iloc[0]['Parcel ID']
            property_address = f"{df.iloc[0]['LOCN']} {df.iloc[0]['LOCD']}\
                 {df.iloc[0]['LOCS']}"
            self.property_address = property_address
            self.city = df.iloc[0]['LOCCITY']
            self.zip_code = df.iloc[0]['LOCZIP']
            self.legal_description = df.iloc[0]['LEGAL1']
            self.lot = df.iloc[0]['LOT']
            self.block = df.iloc[0]['BLOCK']
            self.subdivision_code = df.iloc[0]['SUBD']
            subdivision_data = self.get_sarasota_subdivision(
                self.subdivision_code)
            self.plat_book = subdivision_data[0]
            self.plat_page = subdivision_data[1]
            self.subdivision_name = subdivision_data[2]
            self.or_book = df.iloc[0]['OR_BOOK']
            self.or_page = df.iloc[0]['OR_PAGE']
            self.or_inst = df.iloc[0]['LEGALREFER']
