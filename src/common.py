"""Functions used in multiple modules."""

from time import sleep

from typing import Callable
import plyvel
from flask import current_app


def setup_database(function):
    """Decorator that opens database before its usage, and closes its after."""
    def wrapper(*args, **kwargs):
        database = None
        db_location = ''
        try:
            db_location = current_app.config['DB_LOCATION']
        except RuntimeError:
            db_location = args[0]
        while True:
            try:
                database = plyvel.DB(db_location)
                break
            except plyvel._plyvel.IOError:
                sleep(0.5)
                continue
        value = function(*args, **kwargs, db=database)
        database.close()
        return value
    return wrapper
