"""Functions used in multiple modules."""

from typing import Callable, Dict
import plyvel
from flask import current_app


def setup_database(function) -> Callable:
    """Decorator that opens database before its usage, and closes its after."""
    def wrapper(*args, **kwargs):
        database = None
        db_location = ''
        db_lock = None
        try:
            db_location = current_app.config['DB_LOCATION']
        except RuntimeError:
            db_location = args[0]
        try:
            db_lock = current_app.config['DB_LOCK']
        except RuntimeError:
            db_lock = args[1]
        db_lock.acquire()
        database = plyvel.DB(db_location)
        value = function(*args, **kwargs, db=database)
        database.close()
        db_lock.release()
        return value
    return wrapper
