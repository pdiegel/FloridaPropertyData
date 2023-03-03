# Florida County Property Data Retrieval and Processing

This repository contains Python code for retrieving and processing property data for specific counties in Florida, using their Parcel ID number. The code is organized into a module that defines an abstract base class for county property data, concrete subclasses for specific counties, and helper functions for data processing.

## Overview

The goal of this project is to create a Python-based tool that can retrieve and process property data for specific counties in Florida. The tool is designed to have a standardized way of downloading dataframes for each county that is specified with a county name, and URL. Once the dataframes are downloaded, they are unzipped (if applicable) and converted to gzip format. This ensures that the data is in a standardized format, which makes it easier to process and analyze.

The tool can be used by individuals or organizations that need to retrieve property data for specific counties in Florida. This could include real estate agents, property investors, and local government officials. The tool is flexible and can be easily customized to meet the specific needs of each user.

## Usage

To use the tool, follow these steps:

1.  Clone the repository and navigate to the root directory in your terminal or command prompt.
2.  Open the Python interpreter or create a Python script in your preferred IDE or text editor.
3.  Import the `ParcelDataCollection` class from the module:
``` python
from county_property_data import ParcelDataCollection
```
4. Create an instance of the `ParcelDataCollection` class and pass in the Parcel ID number as a string:
``` python
parcel_id = "123456789"
county_data = ParcelDataCollection(parcel_id)
```    
5. The ParcelDataCollection class will search through each county dataframe class in the module and return an instance of the county's dataframe class, if available. At the moment, there are three concrete dataframe classes available: Sarasota, Manatee, and Charlotte counties. The dataframe class will instantiate, and an instance variable dictionary parcel_data will be created. This variable contains much of the parcel's information.
6. You can access the parcel data by calling the instance variable `parcel_data`. For example, to print the parcel's property address:
    ``` python
    print(county_data.parcel_data['address'])
    ```
    This will output the Property Address associated with the Parcel ID number you provided.

That's it! With these simple steps, you can easily retrieve property data for specific counties in Florida using the Parcel ID number. For more advanced usage, please see the documentation.

## Contributing

If you are interested in contributing to the project, please see the CONTRIBUTING.md file for more information.

## License

This project is licensed under the MIT License. See the LICENSE.md file for more information.

## Contact

If you have any questions or concerns, please contact us at [philipdiegel@gmail.com].
