"""Functions for gathering block information from the database."""

# import plyvel
# from flask import current_app
from typing import List

from src.common import setup_database


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
    pass


@setup_database
def get_balance(address: str) -> None:
    """
    Get balance of an address.

    Args:
        address: Ethereum address.
    """
    pass


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
    pass
