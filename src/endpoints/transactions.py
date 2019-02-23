"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app
from typing import List

from src.common import setup_database
from src.database_gatherer import DatabaseGatherer


@setup_database
def read_transaction(tx_hash: str, db=None) -> None:
    """
    Get transaction by its hash.

    Args:
        tx_hash: Hash of the transaction.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    transaction = gatherer.get_transaction_by_hash(tx_hash)
    if transaction is None:
        return 'Transaction with hash {} not found'.format(tx_hash), 404

    return transaction


@setup_database
def get_transactions_by_bhash(block_hash: str, db=None) -> None:
    """
    Get transactions of a block by its hash.

    Args:
        block_hash: Hash of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_block_by_hash(block_hash)
    if transactions is None:
        return 'Block with hash {} not found'.format(block_hash), 404

    return transactions


@setup_database
def get_transactions_by_bindex(block_index: str, db=None) -> None:
    """
    Get transactions of a block by its index.

    Args:
        block_index: Index of the block.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_block_by_index(block_index)
    if transactions is None:
        return 'Block with index {} not found'.format(block_index), 404

    return transactions


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
    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_address(address, time_from, time_to,
                                                        val_from, val_to)
    if transactions is None:
        return 'No transactions of address {} found'.format(address), 404

    return transactions


@setup_database
def get_transactions_by_addresses(addresses: List[str],
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
    gatherer = DatabaseGatherer(db)
    transactions = []
    for address in addresses:
        transactions += gatherer.get_transactions_of_address(address, time_from, time_to,
                                                             val_from, val_to)
    if transactions == []:
        return 'No transactions of requested addresses found', 404

    return transactions
