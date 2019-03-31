"""Functions for gathering block information from the database."""

import logging
import time

from flask import current_app

from src.database_gatherer import DatabaseGatherer

LOG = logging.getLogger()


def read_block(block_hash: str) -> None:
    """
    Read one block by its hash.

    Args:
        block_hash: Unique hash of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    db = current_app.config['DB']
    gatherer = DatabaseGatherer(db)
    block = gatherer.get_block_by_hash(block_hash)
    if block is None:
        return 'Block with hash {} not found'.format(block_hash), 404

    return block


def get_hash_by_index(block_index: str) -> None:
    """
    Get block hash by its index.

    Args:
        block_index: Index of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    db = current_app.config['DB']
    try:
        int(block_index)
    except ValueError:
        return 'Index {} couldn\'t be parsed'.format(block_index), 400

    gatherer = DatabaseGatherer(db)
    block_hash = gatherer.get_block_index_by_hash(block_index)
    if block_hash is None:
        return 'Block with index {} not found'.format(block_index), 404

    return block_hash


def get_blocks_by_time(limit: str = '0',
                       block_start: str = '0',
                       block_end: str = '') -> None:
    """
    Get multiple blocks by datetime range.

    Args:
        limit: Maximum blocks to gether.
        block_start: Beginning datetime.
        block_end: End datetime.
        db: Database instance (meant to be filled by the decorator).
    """
    db = current_app.config['DB']
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


def get_blocks_by_indexes(index_start: str = 0,
                          index_end: str = 'max', db=None) -> None:
    """
    Get multiple blocks by index range.

    Args:
        index_start: Beginning index.
        index_end: End index.
        db: Database instance (meant to be filled by the decorator).
    """
    db = current_app.config['DB']
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
