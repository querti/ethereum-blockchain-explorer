"""Gathers requested information from the database."""
# from multiprocessing import Lock
from typing import Any, List, Dict, Union
import logging

import src.coder as coder

LOG = logging.getLogger()


class DatabaseGatherer:
    """Class that gathers the requested information from the database."""

    def __init__(self, db: Any) -> None:
        """
        Initialization.

        Args:
                db: Database instance.
        """
        self.db = db
        self.blocks_db = db.prefixed_db(b'block-')
        self.block_hash_db = db.prefixed_db(b'hash-block-')
        self.block_timestamp_db = db.prefixed_db(b'timestamp-block')
        self.transaction_db = db.prefixed_db(b'transaction-')
        self.tx_hash_db = db.prefixed_db(b'tx-hash-')
        self.address_db = db.prefixed_db(b'address-')

    def get_block_by_hash(self, block_hash: str) -> Union[Dict[str, str], None]:
        """
        Retrieves block information specified by its hash.

        Args:
            block_hash: Block hash.

        Returns:
            Dictionary representing the blockchain block.
        """
        block_index = self.block_hash_db.get(block_hash.encode()).decode()
        if block_index is None:
            return None

        raw_block = self.blocks_db.get(block_index.encode())
        block = coder.decode_block(raw_block)
        transaction_range = block['transactionIndexRange'].split('-')
        del block['transactionIndexRange']
        transactions = []  # type: List[Dict[str, Any]]

        it = self.transaction_db.iterator(start=transaction_range[0].encode(),
                                          end=transaction_range[0].encode(),
                                          include_key=False)
        for raw_tx in it:
            transactions.append(coder.decode_transaction(raw_tx))
            block['transactions'] = transactions

        return block

    def get_block_index_by_hash(self, block_index: str) -> Union[str, None]:
        """
        Retrieves block hash by its index.

        Args:
            block_index: Index of a block.

        Returns:
            Block hash.
        """
        raw_block = self.blocks_db.get(block_index.encode())
        if raw_block is None:
            return None
        block = coder.decode_block(raw_block)
        return block['hash']

    def get_blocks_by_datetime(self, limit: str, block_start: str,
                               block_end: str) -> Union[List[Dict[str, Any]], None]:
        """
        Retrieves multiple blocks based on specified datetime range.

        Args:
            limit: Maximum blocks to gether.
            block_start: Beginning datetime.
            block_end: End datetime.

        Returns:
            List of block dictionaries.
        """
        block_indexes = []  # type: List[str]
        blocks = []  # type: List[Dict[str, Any]]
        counter = 0

        it = self.block_timestamp_db.iterator(start=block_start.encode(),
                                              end=block_end.encode(),
                                              include_key=False)
        for block_index in it:
            counter += 1
            if counter > int(limit):
                break
            block_indexes.append(block_index.decode())

        if blocks == []:
            return None

        it1 = self.blocks_db.iterator(start=block_indexes[0],
                                      end=block_indexes[-1],
                                      include_key=False)
        for raw_block in it1:
            block = coder.decode_block(raw_block)
            transaction_range = block['transactionIndexRange'].split('-')
            del block['transactionIndexRange']
            transactions = []  # type: List[Dict[str, Any]]
            it2 = self.transaction_db.iterator(start=transaction_range[0].encode(),
                                               end=transaction_range[0].encode(),
                                               include_key=False)
            for raw_tx in it2:
                transactions.append(coder.decode_transaction(raw_tx))
                block['transactions'] = transactions
                blocks.append(block)

        return blocks

    def get_blocks_by_indexes(self, index_start: str,
                              index_end: str) -> Union[List[Dict[str, Any]], None]:
        """
        Retrieves a list of blocks specified by an index range.

        Args:
            index_start: First index.
            index_end: Last index.

        Returns:
            A list of blocks.
        """
        blocks = []

        it1 = self.blocks_db.iterator(start=index_start.encode(),
                                      end=index_end.encode(),
                                      include_key=False)
        for raw_block in it1:
            block = coder.decode_block(raw_block)
            transaction_range = block['transactionIndexRange'].split('-')
            del block['transactionIndexRange']
            transactions = []  # type: List[Dict[str, Any]]

            it2 = self.transaction_db.iterator(start=transaction_range[0].encode(),
                                               end=transaction_range[0].encode(),
                                               include_key=False)
            for raw_tx in it2:
                transactions.append(coder.decode_transaction(raw_tx))
                block['transactions'] = transactions

            blocks.append(block)

        if blocks == []:
            return None
        return blocks

    def get_transaction_by_hash(self, tx_hash) -> Union[Dict[str, Any], None]:
        """
        Retrieves transaction specified by its hash.

        Args:
            tx_hash: Hash of the transaction.

        Returns:
            The desired transaction.
        """
        tx_index = self.tx_hash_db.get(tx_hash.encode()).decode()
        if tx_index is None:
            return None

        raw_tx = self.transaction_db(tx_index.encode())
        transaction = coder.decode_transaction(raw_tx)

        return transaction

    def get_transactions_of_block_by_hash(self,
                                          block_hash: str) -> Union[List[Dict[str, Any]], None]:
        """
        Gets list of transactions belonging to specified block.

        Args:
            block_hash: hash of the block.

        Returns:
            List of specified block transactions.
        """
        block_index = self.block_hash_db.get(block_hash.encode()).decode()
        if block_index is None:
            return None

        raw_block = self.blocks_db.get(block_index.encode())
        block = coder.decode_block(raw_block)
        transaction_range = block['transactionIndexRange'].split('-')
        transactions = []  # type: List[Dict[str, Any]]

        it = self.transaction_db.iterator(start=transaction_range[0].encode(),
                                          end=transaction_range[0].encode(),
                                          include_key=False)
        for raw_tx in it:
            transactions.append(coder.decode_transaction(raw_tx))

        return transactions

    def get_transactions_of_block_by_index(self,
                                           block_index: str) -> Union[List[Dict[str, Any]], None]:
        """
        Gets list of transactions belonging to specified block.

        Args:
            block_index: index of the block.

        Returns:
            List of specified block transactions.
        """
        raw_block = self.blocks_db.get(block_index.encode())
        if raw_block is None:
            return None
        block = coder.decode_block(raw_block)
        transaction_range = block['transactionIndexRange'].split('-')
        transactions = []  # type: List[Dict[str, Any]]

        it = self.transaction_db.iterator(start=transaction_range[0].encode(),
                                          end=transaction_range[0].encode(),
                                          include_key=False)
        for raw_tx in it:
            transactions.append(coder.decode_transaction(raw_tx))

        return transactions

    def get_transactions_of_address(self, addr: str, time_from: str, time_to: str,
                                    val_from: str,
                                    val_to: str) -> Union[List[Dict[str, Any]], None]:
        """
        Get transactions of specified address, with filtering by time and transferred capital.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            val_from: Minimum transferred currency of the transactions.
            val_to: Maximum transferred currency of transactions.

        Returns:
            List of address transactions.
        """
        # TODO OPTIONAL ARGUMENT WHAT VALUE???
        raw_address = self.address_db.get(addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)
        input_transactions = []
        output_transactions = []

        for transaction in address['inputTransactionIndexes'].split('|'):
            tx_index, timestamp, value = transaction.split('+')
            if (int(time_from) <= int(timestamp) and int(time_to) >= int(timestamp)
                    and int(val_from) <= int(value) and int(val_to) >= int(value)):
                raw_tx = self.transaction_db.get(tx_index.encode())
                input_transactions.append(coder.decode_transaction(raw_tx))

        for transaction in address['outputTransactionIndexes'].split('|'):
            tx_index, timestamp, value = transaction.split('+')
            if (int(time_from) <= int(timestamp) and int(time_to) >= int(timestamp)
                    and int(val_from) <= int(value) and int(val_to) >= int(value)):
                raw_tx = self.transaction_db.get(tx_index.encode())
                output_transactions.append(coder.decode_transaction(raw_tx))

        return input_transactions + output_transactions

    def get_address(self, addr: str, time_from: str, time_to: str,
                    val_from: str, val_to: str,
                    no_tx_list: str) -> Union[List[Dict[str, Any]], None]:
        """
        Get information of an address, with the possibility of filtering/limiting transactions.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            val_from: Minimum transferred currency of the transactions.
            val_to: Maximum transferred currency of transactions.
            no_tx_list: Maximum transactions to return

        Returns:
            Address information along with its transactions.
        """
        # TODO: tie pocty transakcii adresy by som asi nevracal...
        raw_address = self.address_db.get(addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)
        input_transactions = []
        output_transactions = []
        counter = 0

        for transaction in address['inputTransactionIndexes'].split('|'):
            tx_index, timestamp, value = transaction.split('+')
            if (int(time_from) <= int(timestamp) and int(time_to) >= int(timestamp)
                    and int(val_from) <= int(value) and int(val_to) >= int(value)):
                counter += 1
                if counter > int(no_tx_list):
                    break
                raw_tx = self.transaction_db.get(tx_index.encode())
                input_transactions.append(coder.decode_transaction(raw_tx))

        for transaction in address['outputTransactionIndexes'].split('|'):
            tx_index, timestamp, value = transaction.split('+')
            if (int(time_from) <= int(timestamp) and int(time_to) >= int(timestamp)
                    and int(val_from) <= int(value) and int(val_to) >= int(value)):
                counter += 1
                if counter > int(no_tx_list):
                    break
                raw_tx = self.transaction_db.get(tx_index.encode())
                output_transactions.append(coder.decode_transaction(raw_tx))

        del address['inputTransactionIndexes']
        del address['outputTransactionIndexes']
        address['inputTransactions'] = input_transactions
        address['outputTransactions'] = output_transactions
        return address

    def get_balance(self, addr: str) -> Union[str, None]:
        """
        Get balance of an address.

        Args:
            addr: Requested address.

        Returns:
            Current balance of an address.
        """
        raw_address = self.address_db.get(addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        return address['balance']
