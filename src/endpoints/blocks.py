"""Functions for gathering block information from the database."""

import plyvel
from flask import current_app

from src.common import setup_database

@setup_database
def read_block(blockHash: str, db=None) -> None:
    """Read one block by its hash."""
    item = db.get(blockHash.encode())
    if item is not None:
        item = item.decode().split(';')
        ret = {'item1': item[0],
               'item2': item[1],
               'item3': item[2]}
    else:
        #TODO: nejako spravit 404???
        ret = None
    return ret
