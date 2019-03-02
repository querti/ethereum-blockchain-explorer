"""Updates the database with new entries."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict
import logging
from datetime import datetime

from src.common import setup_database
import src.coder as coder

LOG = logging.getLogger()

# TODO ADD MINER SUPPORT
class DatabaseUpdater:
    """Class that updates the database to the newest block."""

    def __init__(self, db: Any, blockchain: Any) -> None:
        """
        Initialization.

        Args:
                db: Database instance.
                blockchain: Instance of the blockchain wrapper.
        """
        self.db = db
        self.blockchain = blockchain
        self.blocks_db = db.prefixed_db(b'block-')
        self.block_hash_db = db.prefixed_db(b'hash-block-')
        self.block_timestamp_db = db.prefixed_db(b'timestamp-block-')
        self.transaction_db = db.prefixed_db(b'transaction-')
        self.tx_hash_db = db.prefixed_db(b'tx-hash-')
        self.address_db = db.prefixed_db(b'address-')
        # Get highest block index in DB
        it = self.blocks_db.iterator(include_value=False, reverse=True)
        # Blocks are not actually numerically ordered, but as number strings
        # Theoretically however, highest number should still be near the top
        test_nums = 10000
        counter = 0
        self._highest_block = 5000000
        # for block_index in it:
        #     if self._highest_block < int(block_index.decode()):
        #         self._highest_block = int(block_index.decode())
        #     counter += 1
        #     if counter >= test_nums:
        #         break
        # it.close()
        # Get highest transaction index in DB
        counter = 0
        it = self.transaction_db.iterator(include_value=False, reverse=True)
        self._highest_tx = 0
        # for tx_index in it:
        #     if self._highest_tx < int(tx_index.decode()):
        #         self._highest_tx = int(tx_index.decode())
        #     counter += 1
        #     if counter >= test_nums:
        #         break
        # it.close()

    def fill_database(self) -> None:
        """Adds new entries to the database (or creates a new one from scratch)."""
        # How often to notify the user about the progress.
        notify_rate = 500
        print('start: {}'.format(datetime.now()))
        while True:
            print('next {}'.format(self._highest_block))
            result = self.blockchain.gather_block(self._highest_block)
            if result is None:
                LOG.info('Database has been updated.')
                return
            imm_block, transactions, addresses = result
            block = dict(imm_block)
            first_tx, last_tx, address_txs = self.save_transactions(transactions)
            block['transactionIndexRange'] = str(first_tx) + '-' + str(last_tx)
            self._highest_block = self.save_block(block) + 1
            self.save_addresses(addresses, address_txs)
            if self._highest_block % notify_rate == 0:
                percentage = (self._highest_block / self.blockchain.get_height()) * 100
                LOG.info('Blockchain syncing: {:.2f}% done.'.format(percentage))
                print('end: {}'.format(datetime.now()))
                return

    def save_transactions(self, transactions: List[Dict]) -> Tuple[int, int, Dict]:
        """
        Saves new transactions the the database.

        Args:
            transactions: List of transactions gathered from the blockchain.

        Returns:
            Indexes for first and last saved transaction.
        """
        first = True
        first_tx = 0
        address_txs = {}
        for transaction in transactions:
            tx_value = coder.encode_transaction(transaction)
            self._highest_tx += 1
            if first:
                first_tx = self._highest_tx
                first = False
            self.transaction_db.put(str(self._highest_tx).encode(), tx_value)
            self.tx_hash_db.put(transaction['hash'].hex().encode(),
                                str(self._highest_tx).encode())

            address_txs[transaction['hash'].hex()] = {'transactionIndex': self._highest_tx,
                                                                 'timestamp': transaction['timestamp'],
                                                                 'value': transaction['value']}

        return (first_tx, self._highest_tx, address_txs)

    def save_block(self, block: Dict) -> int:
        """
        Updates the database with new block data.

        Args:
            block: Dictonary containing the block data.
        """
        block_value = coder.encode_block(block)
        self.blocks_db.put(str(block['number']).encode(), block_value)
        self.block_hash_db.put(block['hash'].hex().encode(), str(block['number']).encode())
        self.block_timestamp_db.put(str(block['timestamp']).encode(),
                                    str(block['number']).encode())
        return block['number']

    def save_addresses(self, addresses: Dict, address_txs: Dict) -> None:
        """
        Updates addresses in DB with new data.

        How are indexes and timestamps saved:
            index+timestamp|index+timestamp|...

        args:
            addresses: List of addresses that were affected.
            address_txs: Address data that was changed.
        """
        for address in addresses:
            existing_data = self.address_db.get(address.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_tx_str = existing_address['inputTransactionIndexes']
                output_tx_str = existing_address['outputTransactionIndexes']
            else:
                input_tx_str = ''
                output_tx_str = ''

            address_encode = {}
            address_encode['balance'] = addresses[address]['balance']
            address_encode['code'] = addresses[address]['code']
            for input_tx in addresses[address]['inputTransactionHashes']:
                input_tx_str += ('|' + str(address_txs[input_tx]['transactionIndex'])
                                 + '+' + str(address_txs[input_tx]['timestamp'])
                                 + '+' + str(address_txs[input_tx]['value']))
            if existing_data is None:
                input_tx_str = input_tx_str[1:]
            address_encode['inputTransactionIndexes'] = input_tx_str
            for output_tx in addresses[address]['outputTransactionHashes']:
                output_tx_str += ('|' + str(address_txs[output_tx]['transactionIndex'])
                                  + '+' + str(address_txs[output_tx]['timestamp'])
                                  + '+' + str(address_txs[input_tx]['value']))
            if existing_data is None:
                output_tx_str = output_tx_str[1:]
            address_encode['outputTransactionIndexes'] = output_tx_str

            address_value = coder.encode_address(address_encode)
            self.address_db.put(address.encode(), address_value)


@setup_database
def update_database(db_location: str,
                    db_lock: Any,
                    blockchain: Any,
                    db: Any = None) -> None:
    """
    Updates database with new entries.

    Args: db_location: Path to the leveldb database.
          db_lock: Instance of the database lock (to prevent multiple access).
          blockchain: Instance of the blockchain wrapper.
          db: Database instance.
    """
    db_updater = DatabaseUpdater(db, blockchain)
    db_updater.fill_database()
