"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app

from src.common import setup_database


@setup_database
def read_block(block_hash: str, db=None) -> None:
    """
    Read one block by its hash.

    Args:
        block_hash: Unique hash of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    item = db.get(block_hash.encode())
    if item is not None:
        item = item.decode().split(';')
        ret = {'item1': item[0],
               'item2': item[1],
               'item3': item[2]}
    else:
        # TODO: nejako spravit 404???
        ret = None
    return ret


@setup_database
def get_hash_by_index(block_index: str, db=None) -> None:
    """
    Get block hash by its index.

    Args:
        block_index: Index of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_blocks_by_time(limit: str,
                       block_start: str,
                       block_end: str, db=None) -> None:
    """
    Get multiple blocks by datetime range.

    Args:
        limit: Maximum blocks to gether.
        block_start: Beginning datetime.
        block_end: End datetime.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_blocks_by_indexes(limit: str,
                          index_start: str,
                          index_end: str, db=None) -> None:
    """
    Get multiple blocks by index range.

    Args:
        limit: Maximum blocks to gether.
        index_start: Beginning index.
        index_end: End index.
        db: Database instance (meant to be filled by the decorator).
    """
    pass
