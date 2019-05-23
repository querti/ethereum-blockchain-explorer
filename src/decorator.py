"""DB lock decorator functions used in endpoint functions."""
from time import sleep
import logging
import itertools

from typing import Callable, Any, Union, List
from flask import current_app
import rocksdb

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

RETRY_LIMIT = 10
RETRY_SLEEP = 2


def setup_database(function) -> Callable:
    """Decorator that handles exclusive access, and DB opening/closing."""
    def wrapper(*args, **kwargs):
        db_path = current_app.config['DB_LOCATION']
        db_lock = current_app.config['DB_LOCK']
        db_lock.acquire()
        print('lock start')
        db = rocksdb.DB(db_path, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        value = function(*args, **kwargs, db=db)
        del db
        db_lock.release()
        print('lock end')
        return value
    return wrapper


def db_get_wrapper(db: Any, key: bytes) -> Union[bytes, None]:
    """
    Wrapper to attempt multiple gets due to possible file not found errors.

    This error is likely caused by the DB reorganization.
    It could be a bug in RocksDB or its Python wrapper.

    Args:
        db: Database instance.
        key: Key of the searched entry.

    Returns:
        Return value of the DB query.
    """
    counter = 0
    while True:
        try:
            result = db.get(key)
            return result
        except rocksdb.errors.RocksIOError as e:
            if counter >= RETRY_LIMIT:
                LOG.info('Too many failed retries. Stopping.')
                raise e
            if 'No such file or directory' in str(e):
                LOG.info('DB lookup failed. Retrying.')
                sleep(RETRY_SLEEP)
                counter += 1


def db_iter_wrapper(db: Any, prefix: str) -> List:
    """
    Wrapper to attempt multiple iterator lookups due to possible file not found errors.

    This error is likely caused by the DB reorganization.
    It could be a bug in RocksDB or its Python wrapper.

    Args:
        db: Database instance.
        prefix: prefix of the iterator.

    Returns:
        Returned iteration results.
    """
    counter = 0
    while True:
        try:
            it = db.iteritems()
            it.seek(prefix.encode())
            returned_list = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())
            return returned_list
        except rocksdb.errors.RocksIOError as e:
            if counter >= RETRY_LIMIT:
                LOG.info('Too many failed retries. Stopping.')
                raise e
            if 'No such file or directory' in str(e):
                LOG.info('DB lookup failed. Retrying.')
                sleep(RETRY_SLEEP)
                counter += 1
