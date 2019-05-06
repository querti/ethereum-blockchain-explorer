"""Gathers requested information from the database."""
# from multiprocessing import Lock
from typing import Any, List, Dict, Union
import logging
import itertools

import src.coder as coder

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
        block_index = self.db.get(b'hash-block-' + block_hash.encode())
        if block_index is None:
            LOG.info('Block of specified hash not found.')
            return None

        raw_block = self.db.get(b'block-' + block_index)
        block = coder.decode_block(raw_block)
        block['number'] = block_index.decode()
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return block

        for tx_hash in transaction_hashes:
            raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
            transaction = coder.decode_transaction(raw_tx)
            transaction['hash'] = tx_hash
            transactions.append(transaction)

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
        raw_block = self.db.get(b'block-' + block_index.encode())
        if raw_block is None:
            return None
        block = coder.decode_block(raw_block)
        return block['hash']

    def get_blocks_by_datetime(self, limit: int, block_start: int,
                               block_end: int) -> Union[List[Dict[str, Any]], None]:
        """
        Retrieves multiple blocks based on specified datetime range.

        Args:
            limit: Maximum blocks to gether.
            block_start: Beginning datetime.
            block_end: End datetime.

        Returns:
            List of block dictionaries.
        """
        block_indexes = []  # type: List[int]
        blocks = []  # type: List[Dict[str, Any]]
        counter = 0
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

        if block_indexes == []:
            return None

        # Since DB is working with string-numbers things might be kind of tricky
        block_indexes.sort()
        for block_index in range(block_indexes[0], block_indexes[-1] + 1):
            raw_block = self.db.get(b'block-' + str(block_index).encode())
            block = coder.decode_block(raw_block)
            block['number'] = block_index
            transaction_hashes = block['transactions'].split('+')
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
                transactions.append(transaction)

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
            raw_block = self.db.get(b'block-' + str(block_index).encode())
            block = coder.decode_block(raw_block)
            block['number'] = block_index
            transaction_hashes = block['transactions'].split('+')
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
                transactions.append(transaction)

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
        raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
        if raw_tx is None:
            LOG.info('Transaction of given hash not found.')
            return None
        transaction = coder.decode_transaction(raw_tx)
        transaction['hash'] = tx_hash

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
        block_index = self.db.get(b'hash-block-' + block_hash.encode())
        if block_index is None:
            LOG.info('Block of specified hash not found.')
            return None

        raw_block = self.db.get(b'block-' + block_index)
        block = coder.decode_block(raw_block)
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return []

        for tx_hash in transaction_hashes:
            raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
            transaction = coder.decode_transaction(raw_tx)
            transaction['hash'] = tx_hash
            transactions.append(transaction)

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
        raw_block = self.db.get(b'block-' + block_index.encode())
        block = coder.decode_block(raw_block)
        transaction_hashes = block['transactions'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            block['transactions'] = transactions
            return []

        for tx_hash in transaction_hashes:
            raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
            transaction = coder.decode_transaction(raw_tx)
            transaction['hash'] = tx_hash
            transactions.append(transaction)

        return transactions

    def get_transactions_of_address(self, addr: str, time_from: int, time_to: int,
                                    val_from: int,
                                    val_to: int) -> Union[List[Dict[str, Any]], None]:
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
        raw_address = self.db.get(b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)
        input_transactions = []  # type: List[Dict]
        output_transactions = []  # type: List[Dict]

        if address['inputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-i-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            input_tx_hashes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        if address['outputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-o-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            output_tx_hashes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        input_transactions = []
        for tx_data in input_tx_hashes:
            tx_decoded = tx_data.decode()
            tx_hash, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
                input_transactions.append(transaction)

        output_transactions = []
        for tx_data in output_tx_hashes:
            tx_decoded = tx_data.decode()
            tx_hash, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
                output_transactions.append(transaction)

        return input_transactions + output_transactions

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
        raw_address = self.db.get(b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)

        input_tx_hashes = []  # type: List[bytes]
        output_tx_hashes = []  # type: List[bytes]

        if address['inputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-i-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            input_tx_hashes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        if address['outputTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-o-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            output_tx_hashes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        found_txs = 0

        input_transactions = []
        for tx_data in input_tx_hashes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = tx_data.decode()
            tx_hash, value, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
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
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transaction['hash'] = tx_hash
                output_transactions.append(transaction)
                found_txs += 1

        del address['inputTxIndex']
        del address['outputTxIndex']

        address['inputTransactions'] = input_transactions
        address['outputTransactions'] = output_transactions

        mined_hashes = []  # type: List[bytes]
        if address['minedIndex'] > 0:
            prefix = 'associated-data-' + addr + '-b-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            mined_hashes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        del address['minedIndex']
        address['mined'] = list(map(lambda x: x.decode(), mined_hashes))

        input_token_txs = []
        output_token_txs = []

        input_token_tx_indexes = []  # type: List[bytes]
        output_token_tx_indexes = []  # type: List[bytes]

        if address['inputTokenTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-ti-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            input_token_tx_indexes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        if address['outputTokenTxIndex'] > 0:
            prefix = 'associated-data-' + addr + '-to-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            output_token_tx_indexes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        found_txs = 0

        for token_tx_index in input_token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = self.db.get(b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                input_token_txs.append(token_tx)
                found_txs += 1

        for token_tx_index in output_token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = self.db.get(b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                output_token_txs.append(token_tx)
                found_txs += 1

        del address['inputTokenTxIndex']
        del address['outputTokenTxIndex']

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
        raw_address = self.db.get(b'address-' + addr.encode())
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
        raw_token = self.db.get(b'token-' + addr.encode())
        if raw_token is None:
            return None
        token = coder.decode_token(raw_token)
        token['address'] = addr

        token_tx_indexes = []  # type: List[bytes]
        if token['txIndex'] > 0:
            prefix = 'associated-data-' + addr + '-tt-'
            it = self.db.iteritems()
            it.seek(prefix.encode())
            token_tx_indexes = list(dict(itertools.takewhile(
                lambda item: item[0].startswith(prefix.encode()), it)).values())

        found_txs = 0
        token_txs = []
        for token_tx_index in token_tx_indexes:
            if found_txs >= no_tx_list:
                break
            tx_decoded = token_tx_index.decode()
            tx_index, timestamp = tx_decoded.split('-')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)):
                raw_tx = self.db.get(b'token-tx-' + tx_index.encode())
                token_tx = coder.decode_token_tx(raw_tx)
                token_txs.append(token_tx)
                found_txs += 1

        del token['txIndex']
        token['tokenTransactions'] = token_txs

        return token
