"""Gathers requested information from the database."""
# from multiprocessing import Lock
from typing import Any, List, Dict, Union
import logging

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
        print('NOT IMPLEMENTED')
        return None
        it = self.block_timestamp_db.iterator(start=str(block_start).encode(),
                                              stop=str(block_end).encode())
        for timestamp, block_index in it:
            if counter >= limit and limit != 0:
                break
            if int(block_index.decode()) >= block_start and int(block_index.decode()) <= block_end:
                block_indexes.append(int(block_index.decode()))
                counter += 1
        if block_indexes == []:
            return None

        # Since DB is working with string-numbers things might be kind of tricky
        block_indexes.sort()
        for i in range(block_indexes[0], block_indexes[-1] + 1):
            raw_block = self.db.get(b'block-' + str(i).encode())
            block = coder.decode_block(raw_block)
            block['number'] = str(i)
            transaction_hashes = block['transactionIndexRange'].split('+')
            del block['transactionIndexRange']
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transactions.append(transaction)
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

        for i in range(int(index_start), int(index_end) + 1):
            raw_block = self.db.get(b'block-' + str(i).encode())
            block = coder.decode_block(raw_block)
            block['number'] = str(i)
            transaction_hashes = block['transactionIndexRange'].split('+')
            del block['transactionIndexRange']
            transactions = []  # type: List[Dict[str, Any]]

            if transaction_hashes == ['']:
                block['transactions'] = transactions
                blocks.append(block)
                continue

            for tx_hash in transaction_hashes:
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                transaction = coder.decode_transaction(raw_tx)
                transactions.append(transaction)
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
        transaction_hashes = block['transactionIndexRange'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            return []

        for tx_hash in transaction_hashes:
            raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
            transaction = coder.decode_transaction(raw_tx)
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
        if raw_block is None:
            LOG.info('Block of specified index not found')
            return None
        block = coder.decode_block(raw_block)
        transaction_hashes = block['transactionIndexRange'].split('+')
        transactions = []  # type: List[Dict[str, Any]]

        if transaction_hashes == ['']:
            return []

        for tx_hash in transaction_hashes:
            raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
            transaction = coder.decode_transaction(raw_tx)
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
        # TODO OPTIONAL ARGUMENT WHAT VALUE???
        raw_address = self.db.get(b'address-' + addr.encode())
        if raw_address is None:
            return None
        address = coder.decode_address(raw_address)
        input_transactions = []
        output_transactions = []

        for transaction in address['inputTransactions'].split('|'):
            if transaction == '':
                break
            tx_hash, timestamp, value = transaction.split('+')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                input_transactions.append(coder.decode_transaction(raw_tx))

        for transaction in address['outputTransactions'].split('|'):
            if transaction == '':
                break
            tx_hash, timestamp, value = transaction.split('+')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                output_transactions.append(coder.decode_transaction(raw_tx))
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
            no_tx_list: Maximum transactions to return

        Returns:
            Address information along with its transactions.
        """
        print('addr: {}, time_from: {}, time_to: {}, val_from: {}, val_to: {}, no_tx_list: {}'.format(addr, time_from, time_to, val_from, val_to, no_tx_list))
        raw_address = self.db.get(b'address-' + addr.encode())
        if raw_address is None:
            print('fuck')
            return None
        address = coder.decode_address(raw_address)

        input_transactions = []
        output_transactions = []
        i = 0
        size = len(address['inputTransactions'].split('|'))

        for transaction in address['inputTransactions'].split('|'):
            if transaction == '':
                break
            
            if i % 1000 == 0:
                print('prog: {}%'.format((i/size)*100))
            i += 1
            tx_hash, timestamp, value = transaction.split('+')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                #print('r:{}'.format(tx_hash.encode()))
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                input_transactions.append(raw_tx)

        for transaction in address['outputTransactions'].split('|'):
            if transaction == '':
                break
            tx_hash, timestamp, value = transaction.split('+')
            if (time_from <= int(timestamp) and time_to >= int(timestamp)
                    and val_from <= int(value) and val_to >= int(value)):
                raw_tx = self.db.get(b'transaction-' + tx_hash.encode())
                output_transactions.append(coder.decode_transaction(raw_tx))

        address['mined'] = address['mined'].split('|')
        if address['mined'] == ['']:
            address['mined'] = []

        del address['inputTransactions']
        del address['outputTransactions']

        all_transactions = input_transactions + output_transactions
        all_transactions = sorted(all_transactions, key=lambda k: int(k['timestamp']))

        address['inputTransactions'] = input_transactions
        address['outputTransactions'] = output_transactions
        #iteration = no_tx_list if no_tx_list < len(all_transactions) else len(all_transactions)
        # print('start1')
        # for i in range(iteration):
        #     if all_transactions[i] in input_transactions:
        #         address['inputTransactions'].append(all_transactions[i])
        #     if all_transactions[i] in output_transactions:
        #         address['outputTransactions'].append(all_transactions[i])
        # print('end1')
        input_token_txs = address['inputTokenTransactions'].split('|')
        address['inputTokenTransactions'] = []
        print('start2')
        for input_token_tx in input_token_txs:
            print('lel')
            if input_token_tx == '':
                break
            contract_addr, addr_from, value, tx_hash, timestamp = input_token_tx.split('+')
            address['inputTokenTransactions'].append({'contract_address': contract_addr,
                                                      'address_from': addr_from,
                                                      'value': value,
                                                      'transaction_hash': tx_hash,
                                                      'timestamp': timestamp})
        print('end2')
        output_token_txs = address['outputTokenTransactions'].split('|')
        address['outputTokenTransactions'] = []
        for output_token_tx in output_token_txs:
            if output_token_tx == '':
                break
            contract_addr, addr_to, value, tx_hash, timestamp = output_token_tx.split('+')
            address['outputTokenTransactions'].append({'contract_address': contract_addr,
                                                       'address_to': addr_to,
                                                       'value': value,
                                                       'transaction_hash': tx_hash,
                                                       'timestamp': timestamp})

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

    def get_token(self, addr: str) -> Union[Dict[str, Any], None]:
        """
        Get informtion about a token based on its contract address.

        Args:
            addr: Token address.

        Returns:
            Information about a token.
        """
        raw_token = self.db.get(b'token' + addr.encode())
        if raw_token is None:
            return None
        token = coder.decode_token(raw_token)

        return token
