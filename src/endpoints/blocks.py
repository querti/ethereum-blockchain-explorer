"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app

from src.common import setup_database
from src.database_gatherer import DatabaseGatherer


@setup_database
def read_block(block_hash: str, db=None) -> None:
    """
    Read one block by its hash.

    Args:
        block_hash: Unique hash of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    block = gatherer.get_block_by_hash(block_hash)
    if block is None:
        return 'Block with hash {} not found'.format(block_hash), 404

    return block


@setup_database
def get_hash_by_index(block_index: str, db=None) -> None:
    """
    Get block hash by its index.

    Args:
        block_index: Index of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    block_index = gatherer.get_block_index_by_hash(block_index)
    if block_index is None:
        return 'Block with index {} not found'.format(block_index), 404

    return block_index


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
    gatherer = DatabaseGatherer(db)
    blocks = gatherer.get_blocks_by_datetime(limit, block_start, block_end)
    if blocks is None:
        return 'No blocks in timeframe {} - {} have been found'.format(block_start, block_end), 404

    return blocks


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
    gatherer = DatabaseGatherer(db)
    blocks = gatherer.get_blocks_by_indexes(index_start, index_end)
    if blocks is None:
        return 'No blocks in index range {}-{} have been found'.format(index_start, index_end), 404

    return blocks
