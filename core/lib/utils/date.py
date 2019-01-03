
from re import match
from datetime import datetime


__author__ = 'Daniel Bonhaure'


def validate_date_string(date_string):
    is_valid_date = False

    if isinstance(date_string, str):
        if match(r'^\d{4}-\d{2}-\d{2}$', date_string):
            try:
                datetime.strptime(date_string, '%Y-%m-%d')
                is_valid_date = True
            except ValueError:
                is_valid_date = False

    return is_valid_date
