"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app
from typing import List

from src.common import setup_database
from src.database_gatherer import DatabaseGatherer


@setup_database
def read_address(address: str,
                 time_from: str,
                 time_to: str,
                 val_from: str,
                 val_to: str,
                 no_tx_list: str, db=None) -> None:
    """
    Get information about an address, including its transactions.

    Args:
        address: Ethereum address.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    full_address = gatherer.get_address(address, time_from, time_to,
                                        val_from, val_to, no_tx_list)
    if full_address is None:
        return 'Address {} found'.format(address), 404

    return full_address


@setup_database
def get_balance(address: str, db=None) -> None:
    """
    Get balance of an address.

    Args:
        address: Ethereum address.
    """
    gatherer = DatabaseGatherer(db)
    balance = gatherer.get_balance(address)
    if balance is None:
        return 'Address {} found'.format(address), 404

    return balance


@setup_database
def read_addresses(addresses: List[str],
                   time_from: str,
                   time_to: str,
                   val_from: str,
                   val_to: str,
                   no_tx_list: str, db=None) -> None:
    """
    Get information about multiple addresses, including their transactions.

    Args:
        address: Ethereum addresses.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Database instance (meant to be filled by the decorator).
    """
    gatherer = DatabaseGatherer(db)
    full_addresses = []
    for address in full_addresses:
        full_addresses.append(gatherer.get_address(address, time_from, time_to,
                                                   val_from, val_to, no_tx_list))
    if full_addresses == []:
        return 'None of the requested addresses found', 404

    return full_addresses

@setup_database
def get_token(address: str, db=None) -> None:
    """
    Get information about a token specified by its address.

    Args:
        address: Specified token address.
    """
    gatherer = DatabaseGatherer(db)
    token = gatherer.get_token(address)
    if token is None:
        return 'Token contract with address {} not found'.format(address), 404

    return token