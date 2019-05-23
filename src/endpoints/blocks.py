"""Functions for gathering block information from the database."""

import logging
import time
from typing import Any

from src.database_gatherer import DatabaseGatherer
from src.decorator import setup_database

LOG = logging.getLogger()


@setup_database
def read_block(block_hash: str, db: Any = None) -> None:
    """
    Read one block by its hash.

    Args:
        block_hash: Unique hash of the block.
        db: Read-only database instance.
    """
    gatherer = DatabaseGatherer(db)
    block = gatherer.get_block_by_hash(block_hash.lower())
    if block is None:
        return 'Block with hash {} not found'.format(block_hash), 404

    return block


@setup_database
def get_hash_by_index(block_index: str, db: Any = None) -> None:
    """
    Get block hash by its index.

    Args:
        block_index: Index of the block.
        db: Read-only database instance.
    """
    try:
        int(block_index)
    except ValueError:
        return 'Index {} couldn\'t be parsed'.format(block_index), 400

    gatherer = DatabaseGatherer(db)
    block_hash = gatherer.get_block_hash_by_index(block_index)
    if block_hash is None:
        return 'Block with index {} not found'.format(block_index), 404

    return block_hash


@setup_database
def get_blocks_by_time(limit: str = '0',
                       block_start: str = '0',
                       block_end: str = '',
                       db: Any = None) -> None:
    """
    Get multiple blocks by datetime range.

    Args:
        limit: Maximum blocks to gether.
        block_start: Beginning datetime.
        block_end: End datetime.
        db: Read-only database instance.
    """
    if block_end == '':
        block_end = str(int(time.time()) + 1000000)
    try:
        int_limit = int(limit)
    except ValueError:
        return 'Limit {} couldn\'t be parsed'.format(limit), 400
    try:
        int_block_start = int(block_start)
    except ValueError:
        return 'Start datetime {} couldn\'t be parsed'.format(block_start), 400
    try:
        int_block_end = int(block_end)
    except ValueError:
        return 'Start datetime {} couldn\'t be parsed'.format(block_end), 400

    if int_block_start > int_block_end:
        return 'Start datetime larger than end datetime.', 400

    gatherer = DatabaseGatherer(db)
    blocks = gatherer.get_blocks_by_datetime(int_limit, int_block_start, int_block_end)
    if blocks is None:
        return 'No blocks in timeframe {} - {} have been found'.format(block_start, block_end), 404

    return blocks


@setup_database
def get_blocks_by_indexes(index_start: str = 0,
                          index_end: str = 'max',
                          db: Any = None) -> None:
    """
    Get multiple blocks by index range.

    Args:
        index_start: Beginning index.
        index_end: End index.
        db: Read-only database instance.
    """
    try:
        int_index_start = int(index_start)
    except ValueError:
        return 'Start index {} couldn\'t be parsed'.format(index_start), 400
    if index_end == 'max':
        with open('./data/progress.txt', 'r') as f:
            index_end, _ = f.read().split('\n')
    try:
        int_index_end = int(index_end)
    except ValueError:
        return 'End index {} couldn\'t be parsed'.format(index_end), 400

    if int_index_start > int_index_end:
        return 'Start index larger than end index.', 400

    gatherer = DatabaseGatherer(db)
    blocks = gatherer.get_blocks_by_indexes(index_start, index_end)
    if blocks is None:
        return 'No blocks in index range {}-{} have been found'.format(index_start, index_end), 404

    return blocks
