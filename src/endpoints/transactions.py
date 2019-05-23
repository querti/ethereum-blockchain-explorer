"""Functions for gathering block information from the database."""

from typing import List, Any
import time

from src.database_gatherer import DatabaseGatherer
from src.decorator import setup_database


@setup_database
def read_transaction(tx_hash: str, db: Any = None) -> None:
    """
    Get transaction by its hash.

    Args:
        tx_hash: Hash of the transaction.
        db: Read-only database instance.
    """
    gatherer = DatabaseGatherer(db)
    transaction = gatherer.get_transaction_by_hash(tx_hash.lower())
    if transaction is None:
        return 'Transaction with hash {} not found'.format(tx_hash), 404

    return transaction


@setup_database
def get_transactions_by_bhash(block_hash: str, db: Any = None) -> None:
    """
    Get transactions of a block by its hash.

    Args:
        block_hash: Hash of the block.
        db: Read-only database instance.
    """
    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_block_by_hash(block_hash.lower())
    if transactions is None:
        return 'Block with hash {} not found'.format(block_hash), 404

    return transactions


@setup_database
def get_transactions_by_bindex(block_index: str, db: Any = None) -> None:
    """
    Get transactions of a block by its index.

    Args:
        block_index: Index of the block.
        db: Read-only database instance.
    """
    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_block_by_index(block_index)
    if transactions is None:
        return 'Block with index {} not found'.format(block_index), 404

    return transactions


@setup_database
def get_transactions_by_address(address: str,
                                time_from: str = '0',
                                time_to: str = '',
                                val_from: str = '0',
                                val_to: str = '',
                                no_tx_list: str = '',
                                db: Any = None) -> None:
    """
    Get transactions of an address.

    Args:
        address: Ethereum address.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    try:
        int_val_from = int(val_from)
    except ValueError:
        return 'Minimum value {} couldn\'t be parsed.'.format(val_from), 400

    if val_to == '':
        val_to = str(1000000000000000000000000000000)
    try:
        int_val_to = int(val_to)
    except ValueError:
        return 'Maximum value {} couldn\'t be parsed.'.format(val_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400
    if int_val_from > int_val_to:
        return 'Minimum value is larger than maximum value', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_transactions_of_address(address.lower(), int_time_from,
                                                        int_time_to, int_val_from, int_val_to,
                                                        int_no_tx_list)
    if transactions is None:
        return 'No transactions of address {} found'.format(address), 404

    return transactions


@setup_database
def get_transactions_by_addresses(addresses: List[str],
                                  time_from: str = '0',
                                  time_to: str = '',
                                  val_from: str = '0',
                                  val_to: str = '',
                                  no_tx_list: str = '',
                                  db: Any = None) -> None:
    """
    Get transactions of multiple addresses.

    Args:
        address: Multiple Ethereum addresses.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    try:
        int_val_from = int(val_from)
    except ValueError:
        return 'Minimum value {} couldn\'t be parsed.'.format(val_from), 400

    if val_to == '':
        val_to = str(1000000000000000000000000000000)
    try:
        int_val_to = int(val_to)
    except ValueError:
        return 'Maximum value {} couldn\'t be parsed.'.format(val_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400
    if int_val_from > int_val_to:
        return 'Minimum value is larger than maximum value', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = []
    for address in addresses:
        new_transactions = gatherer.get_transactions_of_address(address.lower(),
                                                                int_time_from, int_time_to,
                                                                int_val_from, int_val_to,
                                                                int_no_tx_list)
        if new_transactions is None:
            return 'Address {} has not been found'.format(address), 400
        transactions += new_transactions

    if transactions == []:
        return 'No transactions of requested addresses found', 404

    return transactions


@setup_database
def get_internal_transactions_by_address(address: str,
                                         time_from: str = '0',
                                         time_to: str = '',
                                         val_from: str = '0',
                                         val_to: str = '',
                                         no_tx_list: str = '',
                                         db: Any = None) -> None:
    """
    Get internal transactions of an address.

    Args:
        address: Ethereum address.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    try:
        int_val_from = int(val_from)
    except ValueError:
        return 'Minimum value {} couldn\'t be parsed.'.format(val_from), 400

    if val_to == '':
        val_to = str(1000000000000000000000000000000)
    try:
        int_val_to = int(val_to)
    except ValueError:
        return 'Maximum value {} couldn\'t be parsed.'.format(val_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400
    if int_val_from > int_val_to:
        return 'Minimum value is larger than maximum value', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_internal_txs_of_address(address.lower(), int_time_from,
                                                        int_time_to, int_val_from, int_val_to,
                                                        int_no_tx_list)
    if transactions is None:
        return 'No internal transactions of address {} found'.format(address), 404

    return transactions


@setup_database
def get_internal_transactions_by_addresses(addresses: List[str],
                                           time_from: str = '0',
                                           time_to: str = '',
                                           val_from: str = '0',
                                           val_to: str = '',
                                           no_tx_list: str = '',
                                           db: Any = None) -> None:
    """
    Get internal transactions of multiple addresses.

    Args:
        address: Multiple Ethereum addresses.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    try:
        int_val_from = int(val_from)
    except ValueError:
        return 'Minimum value {} couldn\'t be parsed.'.format(val_from), 400

    if val_to == '':
        val_to = str(1000000000000000000000000000000)
    try:
        int_val_to = int(val_to)
    except ValueError:
        return 'Maximum value {} couldn\'t be parsed.'.format(val_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400
    if int_val_from > int_val_to:
        return 'Minimum value is larger than maximum value', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = []
    for address in addresses:
        new_transactions = gatherer.get_internal_txs_of_address(address.lower(),
                                                                int_time_from, int_time_to,
                                                                int_val_from, int_val_to,
                                                                int_no_tx_list)
        if new_transactions is None:
            return 'Address {} has not been found'.format(address), 400
        transactions += new_transactions

    if transactions == []:
        return 'No transactions of requested addresses found', 404

    return transactions


@setup_database
def get_token_transactions_by_address(address: str,
                                      time_from: str = '0',
                                      time_to: str = '',
                                      no_tx_list: str = '',
                                      db: Any = None) -> None:
    """
    Get token transactions of an address.

    Args:
        address: Ethereum address.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = gatherer.get_token_txs_of_address(address.lower(), int_time_from,
                                                     int_time_to, int_no_tx_list)
    if transactions is None:
        return 'No token transactions of address {} found'.format(address), 404

    return transactions


@setup_database
def get_token_transactions_by_addresses(addresses: List[str],
                                        time_from: str = '0',
                                        time_to: str = '',
                                        no_tx_list: str = '',
                                        db: Any = None) -> None:
    """
    Get token transactions of multiple addresses.

    Args:
        address: Multiple Ethereum addresses.
        time_from: Beginning datetime to take transactions from.
        time_to: Ending datetime to take transactions from.
        val_from: Minimum transferred currency of the transactions.
        val_to: Maximum transferred currency of transactions.
        no_tx_list: Maximum transactions to gather.
        db: Read-only database instance.
    """
    try:
        int_time_from = int(time_from)
    except ValueError:
        return 'Start time {} couldn\'t be parsed.'.format(time_from), 400

    if time_to == '':
        time_to = str(int(time.time()) + 1000000)
    try:
        int_time_to = int(time_to)
    except ValueError:
        return 'End time {} couldn\'t be parsed.'.format(time_to), 400

    if int_time_from > int_time_to:
        return 'Minimum time is larger than maximum time', 400

    if no_tx_list == '':
        no_tx_list = str(1000000000000000000000000000000)
    try:
        int_no_tx_list = int(no_tx_list)
    except ValueError:
        return 'Maximum number of transactions {} couldn\'t be parsed.'.format(no_tx_list), 400

    gatherer = DatabaseGatherer(db)
    transactions = []
    for address in addresses:
        new_transactions = gatherer.get_token_txs_of_address(address.lower(),
                                                             int_time_from, int_time_to,
                                                             int_no_tx_list)
        if new_transactions is None:
            return 'Address {} has not been found'.format(address), 400
        transactions += new_transactions

    if transactions == []:
        return 'No transactions of requested addresses found', 404

    return transactions
