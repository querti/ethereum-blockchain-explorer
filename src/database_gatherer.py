"""Gathers requested information from the database."""
# from multiprocessing import Lock
from typing import Any, List, Dict, Union
import logging
from time import sleep

import rocksdb

import src.coder as coder
from src.decorator import db_get_wrapper, db_iter_wrapper

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


class DatabaseGatherer:
    """Class that gathers the requested information from the database."""

    def __init__(self, db: Any) -> None:
        """
        Initialization.

        Args:
            db: Database instance.
        """
        self.db = db

    def get_block_by_hash(self, block_hash: str) -> Union[Dict[str, str], None]:
        """
        Retrieves block information specified by its hash.

        Args:
            block_hash: Block hash.

        Returns:
            Dictionary representing the blockchain block.
        """
        block_index = db_get_wrapper(self.db, b'hash-block-' + block_hash.encode())
        if block_index is None:
            LOG.info('Block of specified hash not found.')
            return None

        raw_block = db_get_wrapper(self.db, b'block-' + block_index)
        block = coder.decode_block(raw_block)
        block['number'] = block_index.decode()
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return block

        for tx_hash in transaction_hashes:
            transaction = self.get_transaction_by_hash(tx_hash)
            transactions.append(transaction)  # type: ignore

        block['transactions'] = transactions

        return block

    def get_block_hash_by_index(self, block_index: str) -> Union[str, None]:
        """
        Retrieves block hash by its index.

        Args:
            block_index: Index of a block.

        Returns:
            Block hash.
        """
        raw_block = db_get_wrapper(self.db, b'block-' + block_index.encode())
        if raw_block is None:
            return None
        block = coder.decode_block(raw_block)
        return block['hash']

    def get_blocks_by_datetime(self, limit: int, block_start: int,
                               block_end: int) -> Union[List[Dict[str, Any]], None]:
        """
        Retrieves multiple blocks based on specified datetime range.

        Args:
            limit: Maximum blocks to gather.
            block_start: Beginning datetime.
            block_end: End datetime.

        Returns:
            List of block dictionaries.
        """
        block_indexes = []  # type: List[int]
        blocks = []  # type: List[Dict[str, Any]]
        retry_counter = 0
        while True:
            try:
                it = self.db.iteritems()
                it.seek(b'timestamp-block-' + str(block_start).encode())
                counter = 0
                while True:
                    data = it.get()
                    timestamp = int(data[0].decode().split('-')[-1])
                    block_index = int(data[1].decode())
                    it.__next__()
                    if timestamp < block_start:
                        continue
                    if timestamp > block_end:
                        break
                    block_indexes.append(block_index)
                    counter += 1
                    if (counter >= limit and limit > 0):
                        break
                break
            except rocksdb.errors.RocksIOError as e:
                if retry_counter >= 10:
                    LOG.info('Too many failed retries. Stopping.')
                    raise e
                if 'No such file or directory' in str(e):
                    LOG.info('DB lookup failed. Retrying.')
                    sleep(2)
                    retry_counter += 1

        if block_indexes == []:
            return None

        # Since DB is working with string-numbers things might be kind of tricky
        block_indexes.sort()
        for block_index in range(block_indexes[0], block_indexes[-1] + 1):
            raw_block = db_get_wrapper(self.db, b'block-' + str(block_index).encode())
            block = coder.decode_block(raw_block)
            block['number'] = block_index
            transaction_hashes = block['transactions'].split('+')
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                transaction = self.get_transaction_by_hash(tx_hash)
                transactions.append(transaction)  # type: ignore

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

        for block_index in range(int(index_start), int(index_end) + 1):
            raw_block = db_get_wrapper(self.db, b'block-' + str(block_index).encode())
            block = coder.decode_block(raw_block)
            block['number'] = block_index
            transaction_hashes = block['transactions'].split('+')
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                transaction = self.get_transaction_by_hash(tx_hash)
                transactions.append(transaction)  # type: ignore

            block['transactions'] = transactions
            blocks.append(block)

        return blocks

    def get_transaction_by_hash(self, tx_hash) -> Union[Dict[str, Any], None]:
        """
        Retrieves transaction specified by its hash.

        Args:
            tx_hash: Hash of the transaction.

        Returns:
            The desired transaction.
        """
        raw_tx = db_get_wrapper(self.db, b'transaction-' + tx_hash.encode())
        if raw_tx is None:
            LOG.info('Transaction of given hash not found.')
            return None
        transaction = coder.decode_transaction(raw_tx)
        transaction['hash'] = tx_hash

        internal_tx_indexes = []  # type: List[Any]
        if transaction['internalTxIndex'] > 0:
            prefix = 'associated-data-' + tx_hash + '-tit-'
            print('OOO')
            internal_tx_indexes = db_iter_wrapper(self.db, prefix)
            print('OOOO')
        internal_transactions = []
        for tx_index in internal_tx_indexes:
            tx_decoded = tx_index.decode()
            raw_tx = db_get_wrapper(self.db, b'internal-tx-' + tx_decoded.encode())
            int_tx = coder.decode_internal_tx(raw_tx)
            internal_transactions.append(int_tx)

        transaction.pop('internalTxIndex', None)
        transaction['internalTransactions'] = internal_transactions

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
        block_index = db_get_wrapper(self.db, b'hash-block-' + block_hash.encode())
        if block_index is None:
            LOG.info('Block of specified hash not found.')
            return None

        raw_block = db_get_wrapper(self.db, b'block-' + block_index)
        block = coder.decode_block(raw_block)
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return []

        for tx_hash in transaction_hashes:
            transaction = self.get_transaction_by_hash(tx_hash)
            transactions.append(transaction)  # type: ignore

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
        raw_block = db_get_wrapper(self.db, b'block-' + block_index.encode())
        block = coder.decode_block(raw_block)
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return []

        for tx_hash in transaction_hashes:
            transaction = self.get_transaction_by_hash(tx_hash)
            transactions.append(transaction)  # type: ignore

        return transactions

    def get_transactions_of_address(self, addr: str, time_from: int, time_to: int,
                                    val_from: int, val_to: int, no_tx_list: int,
                                    internal=False) -> Any:
        """
        Get transactions of specified address, with filtering by time and transferred capital.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            val_from: Minimum transferred currency of the transactions.
            val_to: Maximum transferred currency of transactions.
            no_tx_list: Maximum transactions to return.
            internal: Whether this method was called internally.

        Returns:
            List of address transactions.
        """
        raw_address = db_get_wrapper(self.db, b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        input_tx_hashes = []  # type: List[bytes]
        output_tx_hashes = []  # type: List[bytes]

        if address['inputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-i-'
            input_tx_hashes = db_iter_wrapper(self.db, prefix)
        if address['outputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-o-'
            output_tx_hashes = db_iter_wrapper(self.db, prefix)
        found_txs = 0

        input_transactions = []
        for tx_data in input_tx_hashes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = tx_data.decode()
            tx_hash, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                transaction = self.get_transaction_by_hash(tx_hash)
                transaction.pop('internalTransactions', None)  # type: ignore
                input_transactions.append(transaction)
                found_txs += 1

        output_transactions = []
        for tx_data in output_tx_hashes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = tx_data.decode()
            tx_hash, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                transaction = self.get_transaction_by_hash(tx_hash)
                transaction.pop('internalTransactions', None)  # type: ignore
                output_transactions.append(transaction)
                found_txs += 1

        if internal:
            return (input_transactions, output_transactions)
        else:
            return input_transactions + output_transactions

    def get_internal_txs_of_address(self, addr: str, time_from: int, time_to: int,
                                    val_from: int, val_to: int, no_tx_list: int,
                                    internal=False) -> Any:
        """
        Get internal txs of specified address, with filtering by time and transferred capital.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            val_from: Minimum transferred currency of the transactions.
            val_to: Maximum transferred currency of transactions.
            no_tx_list: Maximum transactions to return.
            internal: Whether this method was called internally.

        Returns:
            List of internal transactions of an address.
        """
        raw_address = db_get_wrapper(self.db, b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        input_int_tx_hashes = []  # type: List[bytes]
        output_int_tx_hashes = []  # type: List[bytes]

        if address['inputIntTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-ii-'
            input_int_tx_hashes = db_iter_wrapper(self.db, prefix)
        if address['outputIntTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-io-'
            output_int_tx_hashes = db_iter_wrapper(self.db, prefix)
        found_txs = 0

        input_int_transactions = []
        for tx_data in input_int_tx_hashes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = tx_data.decode()
            tx_index, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = db_get_wrapper(self.db, b'internal-tx-' + tx_index.encode())
                internal_tx = coder.decode_internal_tx(raw_tx)
                input_int_transactions.append(internal_tx)
                found_txs += 1

        output_int_transactions = []
        for tx_data in output_int_tx_hashes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = tx_data.decode()
            tx_index, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = db_get_wrapper(self.db, b'internal-tx-' + tx_index.encode())
                internal_tx = coder.decode_internal_tx(raw_tx)
                output_int_transactions.append(internal_tx)
                found_txs += 1

        if internal:
            return (input_int_transactions, output_int_transactions)
        else:
            return input_int_transactions + output_int_transactions

    def get_token_txs_of_address(self, addr: str, time_from: int, time_to: int,
                                 no_tx_list: int,
                                 internal=False) -> Any:
        """
        Get token txs of specified address, with filtering by time and transferred capital.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            no_tx_list: Maximum transactions to return.
            internal: Whether this method was called internally.

        Returns:
            List of token transactions of an address.
        """
        raw_address = db_get_wrapper(self.db, b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        input_token_txs = []
        output_token_txs = []

        input_token_tx_indexes = []  # type: List[bytes]
        output_token_tx_indexes = []  # type: List[bytes]

        if address['inputTokenTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-ti-'
            input_token_tx_indexes = db_iter_wrapper(self.db, prefix)
        if address['outputTokenTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-to-'
            output_token_tx_indexes = db_iter_wrapper(self.db, prefix)
        found_txs = 0

        for token_tx_index in input_token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = db_get_wrapper(self.db, b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                input_token_txs.append(token_tx)
                found_txs += 1

        for token_tx_index in output_token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = db_get_wrapper(self.db, b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                output_token_txs.append(token_tx)
                found_txs += 1

        if internal:
            return (input_token_txs, output_token_txs)
        else:
            return input_token_txs + output_token_txs

    def get_address(self, addr: str, time_from: int, time_to: int,
                    val_from: int, val_to: int,
                    no_tx_list: int) -> Union[List[Dict[str, Any]], None]:
        """
        Get information of an address, with the possibility of filtering/limiting transactions.

        Args:
            addr: Ethereum address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            val_from: Minimum transferred currency of the transactions.
            val_to: Maximum transferred currency of transactions.
            no_tx_list: Maximum transactions to return.

        Returns:
            Address information along with its transactions.
        """
        raw_address = db_get_wrapper(self.db, b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        if address['code'] != '0x':
            raw_code = db_get_wrapper(self.db, b'address-contract-' + address['code'].encode())
            address['code'] = raw_code.decode()

        input_transactions, output_transactions = (
            self.get_transactions_of_address(addr, time_from, time_to, val_from, val_to,
                                             no_tx_list, True))

        address.pop('inputTxIndex', None)
        address.pop('outputTxIndex', None)

        address['inputTransactions'] = input_transactions
        address['outputTransactions'] = output_transactions

        input_int_transactions, output_int_transactions = (
            self.get_internal_txs_of_address(addr, time_from, time_to, val_from, val_to,
                                             no_tx_list, True))

        address.pop('inputIntTxIndex', None)
        address.pop('outputIntTxIndex', None)

        address['inputInternalTransactions'] = input_int_transactions
        address['outputInternalTransactions'] = output_int_transactions

        mined_hashes = []  # type: List[bytes]
        if address['minedIndex'] > 0:
            prefix = 'associated-data-' + addr + '-b-'
            mined_hashes = db_iter_wrapper(self.db, prefix)
        address.pop('minedIndex', None)
        address['mined'] = list(map(lambda x: x.decode(), mined_hashes))

        input_token_txs, output_token_txs = (
            self.get_token_txs_of_address(addr, time_from, time_to, no_tx_list, True))

        address.pop('inputTokenTxIndex', None)
        address.pop('outputTokenTxIndex', None)

        address['inputTokenTransactions'] = input_token_txs
        address['outputTokenTransactions'] = output_token_txs

        return address

    def get_balance(self, addr: str) -> Union[str, None]:
        """
        Get balance of an address.

        Args:
            addr: Requested address.

        Returns:
            Current balance of an address.
        """
        raw_address = db_get_wrapper(self.db, b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        return address['balance']

    def get_token(self, addr: str, time_from: int, time_to: int,
                  no_tx_list: int) -> Union[Dict[str, Any], None]:
        """
        Get informtion about a token based on its contract address.

        Args:
            addr: Token address.
            time_from: Beginning datetime to take transactions from.
            time_to: Ending datetime to take transactions from.
            no_tx_list: Maximum transactions to return.

        Returns:
            Information about a token.
        """
        raw_token = db_get_wrapper(self.db, b'token-' + addr.encode())
        if raw_token is None:
            return None
        token = coder.decode_token(raw_token)
        token['address'] = addr

        token_tx_indexes = []  # type: List[bytes]
        if token['txIndex'] > 0:
            prefix = 'associated-data-' + addr + '-tt-'
            token_tx_indexes = db_iter_wrapper(self.db, prefix)
        found_txs = 0
        token_txs = []
        for token_tx_index in token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = db_get_wrapper(self.db, b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                token_txs.append(token_tx)
                found_txs += 1

        token.pop('txIndex', None)
        token['tokenTransactions'] = token_txs

        return token
