"""Class that updates the database in bulk fashion."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict
import logging
from datetime import datetime
import subprocess
import csv

from src.common import setup_database
import src.coder as coder
from src.blockchain_wrapper import BlockchainWrapper

LOG = logging.getLogger()


class BulkDatabaseUpdater:
    """Class that updates the database in bulk fashion."""

    def __init__(self, db: Any,
                       interface: int,
                       confirmations: int,
                       bulk_size: int) -> None:
        """
        Initialization.

        Args:
                db: Database instance.
                interface: Path to the Geth blockchain node.
                confirmations: How many confirmations a block has to have.
                bulk_size: How many blocks to be included in bulk DB update.
        """
        self.blockchain = BlockchainWrapper(interface, confirmations)
        self.db = db
        self._blockchain = blockchain
        self._confirmations = confirmations
        self._bulk_size = bulk_size
        self.blocks_db = db.prefixed_db(b'block-')
        self.block_hash_db = db.prefixed_db(b'hash-block-')
        self.block_timestamp_db = db.prefixed_db(b'timestamp-block-')
        self.transaction_db = db.prefixed_db(b'transaction-')
        self.tx_hash_db = db.prefixed_db(b'tx-hash-')
        self.address_db = db.prefixed_db(b'address-')
        with open('./data/progress.txt', 'r') as f:
            self._highest_block, self._highest_tx = f.read().split('\n')
        
        if interface[-4:] == '.ipc':
            self._interface = 'file://' + interface
        else:
            self._interface = interface
        
    def fill_database(self) -> None:
        """Adds new entries to the database"""
        stop_iteration = False
        while True:
            last_block = self._blockchain.get_height() - self._confirmations
            if self._highest_block + self._bulk_size > last_block:
                latest_block = last_block
                stop_iteration = True
            else:
                latest_block = self._highest_block + self._bulk_size
            self.create_csv_files(self._highest_block, latest_block)
            blocks, transactions, addresses = self.gather_blocks()
            self.update_bulk_db(blocks, transactions, addresses)
            self._highest_block = latest_block
            with open('./data/progress.txt', 'w') as f:
                f.write('{}\n{}'.format(self._highest_block, self._highest_tx))
            if stop_iteration:
                break
            
            LOG.info('Bulk database update complete.')
    
    def create_csv_files(self, first_block: int, last_block: int) -> None:
        """
        Creates csv files holding the new information.
        
        Args:
            first_block: First block to be included.
            last_block: Last block to be included.
        """
        # Get blocks and their transactions
        block_tx_cmd = "ethereumetl export_blocks_and_transactions --start-block {} " \
                       "--end-block {} --provider-uri {} --blocks-output {} " \
                       "--transactions-output {}".format(first_block, last_block,
                       self._interface, './data/blocks.csv', './data/transactions.csv')
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        # Get transaction hashes
        tx_hash_cmd = "ethereumetl extract_csv_column --input {} --column hash " \
                      "--output {}".format('./data/transactions.csv', './data/tx_hashes.txt')
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        # Get receipts
        tx_receipts_cmd = "ethereumetl export_receipts_and_logs --transaction-hashes {} " \
                          " --provider-uri {} --receipts-output {}" \
                          "--logs-output {}".format('./data/tx_hashes.txt', self._interface,
                          './data/receipts.csv', '.data/logs.csv')
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        
    def gather_blocks(self) -> Tuple(Dict, Dict, Dict):
        """
        Create dictionary representation of processed blocks.

        Returns:
            Dictionary of new blocks.
        """
        blocks = {}
        miners = []
        with open('.data/blocks.csv') as csv_f:
            csv_blocks = csv.DictReader(csv_f, delimiter=',')
            line_count = 0
            for row in csv_blocks:
                block = {}
                if line_count == 0:
                    line_count += 1
                    continue
                line_count += 1
                block['number'] = row['number']  
                block['hash'] = row['hash']
                block['parentHash'] = row['parent_hash']
                block['nonce'] = row['nonce']
                block['logsBloom'] = row['logs_bloom']
                block['miner'] = row['miner']
                block['difficulty'] = row['difficulty']
                block['totalDifficulty'] = row['total_difficulty']
                block['extraData'] = row['extra_data']
                block['size'] = row['size']
                block['gasLimit'] = row['gas_limit']
                block['gasUsed'] = row['gas_used']
                block['timestamp'] = row['timestamp']
                block['sha3Uncles'] = row['sha3_uncles']

                blocks[block['hash']] = block
                miners.append((block['miner'], block['hash']))
        
        transactions, addresses = self.gather_transactions(blocks)
        for miner in miners:
            if miner[0] not in addresses and miner[0] is not None:
                addresses[miner[0]] = {'inputTransactionHashes': [],
                                                'outputTransactionHashes': [],
                                                'code': '0x'
                                                'mined': [miner[1]]}
            elif transaction['miner'] is not None:
                addresses[transaction['miner']]['mined'].append(miner[1])
        
        return (blocks, transactions, addresses)

    def gather_transactions(self, blocks: Dict) -> Tuple[Dict, Dict]:
        """
        Gathers transactions and adds their range to blocks, as well as to addresses.

        Args:
            blocks: Processed blocks.
        
        Returns: Gathered transactions and addresses.
        """
        transactions = {}
        addresses = {}
        current_block_hash = None
        first_block_tx = None
        current_highest_tx = None
        first_receipt_tx = self._highest_tx
        with open('.data/transactions.csv') as csv_f:
            csv_transactions = csv.DictReader(csv_f, delimiter=',')
            line_count = 0
            for row in csv_transactions:
                transaction = {}
                if line_count == 0:
                    line_count += 1
                    continue
                if current_block_hash is None:
                    current_block_hash = row['block_hash']
                    first_block_tx = self._highest_tx
                    current_highest_tx = self._highest_tx
                elif current_block_hash != row['block_hash']:
                    blocks[current_block_hash]['transactionIndexRange'] =
                        str(first_block_tx) + '-' + str(current_highest_tx)
                    current_block_hash = row['block_hash']
                    current_highest_tx += 1
                    self._highest_tx = current_highest_tx
                    first_block_tx = self._highest_tx
                elif current_block_hash == row['block_hash']:
                    current_highest_tx += 1

                transaction['blockHash'] = row['block_hash']
                transaction['blockNumber'] = row['block_number']
                transaction['from'] = row['from_address']
                transaction['to'] = row['to_address']
                transaction['gas'] = row['gas']
                transaction['gasPrice'] = row['gas_price']
                transaction['hash'] = row['hash']
                transaction['input'] = row['input']
                transaction['nonce'] = row['nonce']
                transaction['value'] = row['value']
                transaction['timestamp'] = blocks[current_block_hash]['timestamp']

                if transaction['from'] not in addresses and transaction['from'] is not None:
                    addresses[transaction['from']] = {'inputTransactionHashes': [transaction['hash']],
                                                    'outputTransactionHashes': [],
                                                    'code': '0x',
                                                    'mined': []}
                elif transaction['from'] is not None:
                    addresses[transaction['from']]['inputTransactionHashes'].append(transaction['hash'])

                if transaction['to'] not in addresses and transaction['to'] is not None:
                    addresses[transaction['to']] = {'inputTransactionHashes': [],
                                                    'outputTransactionHashes': [transaction['hash']],
                                                    'code': '0x',
                                                    'mined': []}
                elif transaction['to'] is not None:
                    addresses[transaction['to']]['outputTransactionHashes'].append(transaction['hash'])

                transactions[str(current_highest_tx)] = transaction
            
            # Last block transactions of the bulk
            blocks[current_block_hash]['transactionIndexRange'] =
                str(first_block_tx) + '-' + str(current_highest_tx)
            current_highest_tx += 1
            self._highest_tx = current_highest_tx
        
        transactions, addresses = self.gather_receipts(first_receipt_tx, transactions, addresses)
        return (transactions, addresses)

    def gather_receipts(self, start_tx: int, transactions: Dict, addresses: Dict) -> None:
        """
        Gathers receipts of the transactions.

        Args:
            start_tx: Start global index of transactions processed in this data bulk.
            transactions: Dictionary holding all currently proccessed transactions.
            addresses: Dictionary holding all currently processed addresses.
        """
        receipt_tx_index = start_txreceipts
        with open('.data/receipts.csv') as csv_f:
            csv_receipts = csv.DictReader(csv_f, delimiter=',')
            line_count = 0
            for row in csv_receipts:
                transactions[receipt_tx_index]['cumulativeGasUsed'] = row['cumulative_gas_used']
                transactions[receipt_tx_index]['gasUsed'] = row['gas_used']
                transactions[receipt_tx_index]['contractAddress'] = row['contract_address']
                receipt_tx_index += 1

                if (transaction['contractAddress'] not in addresses
                    and transaction['contractAddress'] is not None):
                    code = self._web3.eth.getCode(transaction['contractAddress'])
                    addresses[transaction['contractAddress']] = {'inputTransactionHashes': [],
                                                    'outputTransactionHashes': [],
                                                    'code': code,
                                                    'mined': []}
        
        logs_tx_index = start_tx
        with open('.data/logs.csv') as csv_f:
            csv_logs = csv.DictReader(csv_f, delimiter=',')
            line_count = 0
            for row in csv_logs:
                transactions[logs_tx_index]['logs'] = row['data']
                logs_tx_index += 1
        
        return (transactions, addresses)
    
    def update_bulk_db(self, blocks: Dict, transactions: Dict, addresses: Dict) -> None:
        """
        Updates the database with bulk data.

        Args:
            blocks: Dictionary containing blocks.
            transactions: Dictionary containing transactions.
            addresses: Dictionary containing addresses.
        """
        with self.db.write_batch() as wb
            for block_hash, block_dict in blocks:
                block_value = coder.encode_block(block_dict)
                self.db.put(b'block-' + str(block_dict['number']).encode(), block_value)
                self.db.put(b'hash-block-' + str(block_dict['hash']).encode(), str(block['number']).encode())
                self.db.put(b'timestamp-block-' + str(block_dict['timestamp']).encode(), str(block['number']).encode())
            
            for tx_index, tx_dict in transactions:
                tx_value = coder.encode_transaction(tx_dict)
                self.db.put(b'transaction-' + str(tx_index).encode(), tx_value)
                self.db.put(b'tx-hash-' + str(tx_dict['hash']).encode(), tx_index)

        addresses_encode = {}
        for addr_hash, addr_dict in addresses:
            existing_data = self.address_db.get(addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_tx_str = existing_address['inputTransactionIndexes']
                output_tx_str = existing_address['outputTransactionIndexes']
            else:
                input_tx_str = ''
                output_tx_str = ''

            address_encode = {}
            address_encode['balance'] = 0
            address_encode['code'] = addr_dict['code']
            for input_tx in addr_dict['inputTransactionHashes']:
                input_tx_str += ('|' + str(address_txs[input_tx]['transactionIndex'])
                                 + '+' + str(address_txs[input_tx]['timestamp'])
                                 + '+' + str(address_txs[input_tx]['value']))
            if existing_data is None:
                input_tx_str = input_tx_str[1:]
            address_encode['inputTransactionIndexes'] = input_tx_str
            for output_tx in addr_dict['outputTransactionHashes']:
                output_tx_str += ('|' + str(address_txs[output_tx]['transactionIndex'])
                                  + '+' + str(address_txs[output_tx]['timestamp'])
                                  + '+' + str(address_txs[input_tx]['value']))
            if existing_data is None:
                output_tx_str = output_tx_str[1:]
            address_encode['outputTransactionIndexes'] = output_tx_str
            for block_hash in addr_dict['mined']:
                mined_str += ('|' + str(block_hash))
            if existing_data is None:
                mined_str = mined_str[1:]
            address_encode['mined'] = mined_str

            addresses_encode[addr_hash] = address_encode

        with self.db.write_batch() as wb:
            for addr_hash, addr_dict in address_encode:
                address_value = coder.encode_address(addr_dict)
                self.db.put(b'address-' + str(addr_hash).encode(), address_value)

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
