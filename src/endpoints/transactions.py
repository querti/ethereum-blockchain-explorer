"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app
from typing import List

from src.common import setup_database


@setup_database
def read_transaction(tx_hash: str, db=None) -> None:
    """
    Get transaction by its hash.

    Args:
        tx_hash: Hash of the transaction.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_transactions_by_bhash(block_hash: str, db=None) -> None:
    """
    Get transactions of a block by its hash.

    Args:
        block_hash: Hash of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_transactions_by_bindex(block_index: str, db=None) -> None:
    """
    Get transactions of a block by its index.

    Args:
        block_index: Index of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_transactions_by_address(address: str,
                                time_from: str,
                                time_to: str,
                                val_from: str,
                                val_to: str, db=None) -> None:
    """
    Get transactions of an address.

    Args:
        address: Ethereum address.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        db: Database instance (meant to be filled by the decorator).
    """
    pass


@setup_database
def get_transactions_by_addresses(address: List[str],
                                  time_from: str,
                                  time_to: str,
                                  val_from: str,
                                  val_to: str, db=None) -> None:
    """
    Get transactions of multiple addresses.

    Args:
        address: Multiple Ethereum addresses.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        db: Database instance (meant to be filled by the decorator).
    """
    pass
