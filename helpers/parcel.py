class Parcel:
    def __init__(self, df):
        print(df.columns)
        self.parcel_id = df.iloc[0]['Parcel ID']
        self.property_address = df.iloc[0]['SITUS_ADDRESS']
        self.city = df.iloc[0]['SITUS_POSTAL_CITY']
        self.zip_code = df.iloc[0]['SITUS_POSTAL_ZIP']
        self.legal_description = df.iloc[0]['PAR_LEGAL1']
        self.lot = df.iloc[0]['PAR_SUBDIV_LOT']
        self.block = df.iloc[0]['PAR_SUBDIV_BLOCK']
        self.subdivision_name = df.iloc[0]['PAR_SUBDIV_NAME']
        self.subdivision_code = df.iloc[0]['PAR_SUBDIVISION']
        self.or_book = df.iloc[0]['SALE_BOOK_LAST']
        self.or_page = df.iloc[0]['SALE_PAGE_LAST']
        self.or_inst = df.iloc[0]['SALE_INSTRNO_LAST']
