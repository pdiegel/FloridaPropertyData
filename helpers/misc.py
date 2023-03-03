"""
The misc module provides a collection of miscellaneous utility
functions, including a function to convert strings to a standardized
format.

Functions:
- convert_to_string(string: Union[str, pandas.Series]) -> str: Converts
a string to uppercase, removes leading zeros and whitespace, and returns
the result. Accepts a pandas Series object as input as well.
"""
from pandas import Series


def convert_to_string(string: str) -> str:
    """
    Converts a string to uppercase, removes leading zeros and
    whitespace, and returns the result.

    Args:
        string (str or pandas.Series): A string to be converted.

    Returns:
        str: The input string converted to uppercase, with leading zeros
        and whitespace removed.

    Raises:
        TypeError: If the input argument is not a string or a pandas
        Series.
    """
    if isinstance(string, Series):
        string = string.item()
    elif not isinstance(string, str):
        raise TypeError(f"Expected str or pandas.Series, got {type(string)}")
    string = str(string)
    return string.upper().lstrip('0').strip()
