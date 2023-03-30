import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
COUNTY_DATABASE_DIR = os.path.join(ROOT_DIR, "county_data")
HELPERS_DIR = os.path.join(ROOT_DIR, "helpers")
COLUMN_MAPPER = os.path.join(HELPERS_DIR, "column_mapping.yaml")
COUNTIES = ["sarasota", "manatee", "charlotte"]

if not os.path.exists(COUNTY_DATABASE_DIR):
    os.mkdir(COUNTY_DATABASE_DIR)

for county in COUNTIES:
    if not os.path.exists(os.path.join(COUNTY_DATABASE_DIR, county)):
        os.mkdir(os.path.join(COUNTY_DATABASE_DIR, county))
