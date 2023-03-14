'''
This module sets up a logger that can be used to write log messages to a
file.

The logger writes log messages at various severity levels (DEBUG, INFO,
WARNING, ERROR, CRITICAL) and writes them to a file called 'myapp.log'
in the current working directory. The logger can be imported and used in
other Python modules in the same project to write log messages.

Example usage:

    # Import the logger
    from logger import logger

    # Write log messages
    logger.debug('Debug message')
    logger.info('Info message')
    logger.warning('Warning message')
    logger.error('Error message')
    logger.critical('Critical message')
'''
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('myapp.log')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
