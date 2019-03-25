"""Class that updates the database in bulk fashion."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict, Set
import logging
from datetime import datetime
import subprocess
import csv
import os
import sys
import json

import pandas
import numpy

from src.common import setup_database
import src.coders.bulkcoder as coder
from src.blockchain_wrapper import BlockchainWrapper
from src.requests.balances import BalanceGatherer
from src.requests.traces import TraceAddressGatherer

LOG = logging.getLogger()

csv.field_size_limit(100000000)

class BulkDatabaseUpdater:
    """Class that updates the database in bulk fashion."""

    def __init__(self, db: Any,
                       interface: int,
                       confirmations: int,
                       bulk_size: int,
                       process_traces: bool = False) -> None:
        """
        Initialization.

        WARNING: Examining traces will reveal more addresses, however the sync
                 will be significantly slower.

        Args:
                db: Database instance.
                interface: Path to the Geth blockchain node.
                confirmations: How many confirmations a block has to have.
                bulk_size: How many blocks to be included in bulk DB update.
                process_traces: Whether to get addresses from traces.
        """
        self._blockchain = BlockchainWrapper(interface, confirmations)
        self.db = db
        self._confirmations = confirmations
        self._bulk_size = bulk_size
        self.process_traces = process_traces
        self.address_db = db.prefixed_db(b'address-')
        self.token_db = db.prefixed_db(b'token-')
        with open('./data/progress.txt', 'r') as f:
            self._highest_block, self._highest_tx = f.read().split('\n')
            self._highest_block = int(self._highest_block)
            self._highest_tx = int(self._highest_tx)
        
        if interface[-4:] == '.ipc':
            self._interface = 'file://' + interface
        else:
            self._interface = interface
        
    def fill_database(self) -> None:
        """Adds new entries to the database"""
        stop_iteration = False
        numpy_array = None
        batch_index = 0
        while True:
            batch_index += 1
            # calculate batch range
            last_block = self._blockchain.get_height() - self._confirmations
            if self._highest_block + self._bulk_size > last_block:
                latest_block = last_block
                stop_iteration = True
            else:
                latest_block = self._highest_block + self._bulk_size
            
            if self._highest_block + self._bulk_size > 50000:
                break

            # Get data from Node
            self.create_csv_files(self._highest_block, latest_block)

            # Parse the data
            blocks, transactions, addresses = self.gather_blocks()
            tokens, token_txs = self.gather_tokens()
            if numpy_array is None:
                numpy_array = numpy.unique(numpy.array(list(addresses.keys())))
            else:
                numpy_array = numpy.union1d(numpy_array, numpy.array(list(addresses.keys())))
            # addr_set = addr_set.union(addresses.keys())
            if batch_index % 10 == 0:
                self._save_addresses(numpy_array)
                numpy_array = None
            if self.process_traces:
                addresses = self.add_trace_addresses(addresses, self._highest_block, latest_block)
            addresses = self.fill_addresses(addresses, transactions, tokens, token_txs)
            self.update_bulk_db(blocks, transactions, addresses, tokens)
            self._highest_block = latest_block
            with open('./data/progress.txt', 'w') as f:
                f.write('{}\n{}'.format(self._highest_block, self._highest_tx))

            if stop_iteration:
                break
            percentage = (self._highest_block / last_block) * 100
            LOG.info('Blockchain syncing: {:.2f}% done.'.format(percentage))
        
        if numpy_array is not None:
            self._save_addresses(numpy_array)
        print('done :)')
        # Update balances of all addresses
        self._update_address_balances(last_block)
            
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
        subprocess.call(block_tx_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        #output, error = process.communicate()
        # transactions have to be in a correct order, with 2 keys: block_num, tx_index
        df = pandas.read_csv('./data/transactions.csv')
        df = df.sort_values(['block_number', 'transaction_index'], ascending=[True, True])
        df.to_csv('./data/transactions.csv', index=False)

        # Get transaction hashes
        tx_hash_cmd = "ethereumetl extract_csv_column --input {} --column hash " \
                      "--output {}".format('./data/transactions.csv', './data/tx_hashes.txt')
        subprocess.call(tx_hash_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        #output, error = process.communicate()
        
        # Get receipts
        tx_receipts_cmd = "ethereumetl export_receipts_and_logs --transaction-hashes {} " \
                          " --provider-uri {} --receipts-output {} " \
                          "--logs-output {}".format('./data/tx_hashes.txt', self._interface,
                          './data/receipts.csv', './data/logs.csv')
        subprocess.call(tx_receipts_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        #output, error = process.communicate()
        self.create_token_csv(first_block, last_block)

    def create_token_csv(self, first_block: int, last_block: int) -> None:
        """
        Creates csv file containing information of Token addresses.

        Args:
            first_block: First block to be included.
            last_block: Last block to be included.
        """
        # get contract addresses
        contracts_addr_cmd = "ethereumetl extract_csv_column --input {} " \
                             " --column contract_address " \
                             "--output {}".format('./data/receipts.csv',
                                                  './data/contract_addresses.txt')
        subprocess.call(contracts_addr_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # get contracts
        # This is a bottleneck basically
        contracts_cmd = "ethereumetl export_contracts --contract-addresses {} " \
                        " --provider-uri {} " \
                        "--output {} ".format('./data/contract_addresses.txt', self._interface,
                        './data/contracts.csv')
        subprocess.call(contracts_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Extract token addresses
        if os.path.isfile('./data/contracts.csv'):
            data = []
            with open('./data/contracts.csv') as f:
                for row in csv.DictReader(f,  delimiter=','):
                    if row['is_erc20'] == 'True' or row['is_erc721'] == 'True':
                        data.append(row['address'])
            with open('./data/token_addresses.txt', 'w') as f:
                f.write('\n'.join(data))
       
        # get Tokens
        tokens_cmd = "ethereumetl export_tokens --token-addresses {} " \
                     " --provider-uri {} " \
                     "--output {} ".format('./data/token_addresses.txt', self._interface,
                     './data/tokens.csv')
        subprocess.call(tokens_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        token_tx_cmd = "ethereumetl extract_token_transfers --logs {} " \
                       "--output {}".format('./data/logs.csv', './data/token_transfers.csv')
        subprocess.call(token_tx_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    def gather_tokens(self) -> Dict:
        """
        Gathers information about newly created ERC-20 and ERC-721 tokens.

        Returns:
            Dictionary of format 'address: token_data', List of token transactions.
        """
        tokens = {}
        with open('./data/tokens.csv') as csv_f:
            csv_tokens = csv.DictReader(csv_f, delimiter=',')
            for row in csv_tokens:
                token = {}
                token['symbol'] = row['symbol']
                token['name'] = row['name']
                token['decimals'] = row['decimals']
                token['total_supply'] = row['total_supply']
                tokens[row['address']] = token
        
        with open('./data/contracts.csv') as csv_f:
            csv_contracts = csv.DictReader(csv_f, delimiter=',')
            for row in csv_contracts:
                if row['address'] in tokens:
                    if row['is_erc20'] == 'True':
                        tokens[row['address']]['type'] = 'ERC-20'
                    elif row['is_erc721']:
                        tokens[row['address']]['type'] = 'ERC-721'
        
        token_txs = []
        with open('./data/token_transfers.csv') as csv_f:
            csv_tokens_tx = csv.DictReader(csv_f, delimiter=',')
            for row in csv_tokens_tx:
                token_tx = {}
                token_tx['token_address'] = row['token_address']
                token_tx['from'] = row['from_address']
                token_tx['to'] = row['to_address']
                token_tx['value'] = row['value']

                token_txs.append(token_tx)
        
        return (tokens, token_txs)

    def gather_blocks(self) -> Tuple[Dict, Dict, Dict]:
        """
        Create dictionary representation of processed blocks.

        Returns:
            Dictionary of new blocks.
        """
        blocks = {}
        miners = []
        with open('./data/blocks.csv') as csv_f:
            csv_blocks = csv.DictReader(csv_f, delimiter=',')
            for row in csv_blocks:
                block = {}
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
                                                'code': '0x',
                                                'mined': [miner[1]],
                                                'inputTokenTransactions': [],
                                                'outputTokenTransactions': [],
                                                'ERC20Balances': [],
                                                'ERC721Tokens': []}
            elif miner[0] is not None:
                addresses[miner[0]]['mined'].append(miner[1])
        
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
        happened_blocks = []
        current_highest_tx = None
        self.tx_hash_order = []
        first_receipt_tx = self._highest_tx
        with open('./data/transactions.csv') as csv_f:
            csv_transactions = csv.DictReader(csv_f, delimiter=',')
            for row in csv_transactions:
                transaction = {}
                if current_block_hash is None:
                    current_block_hash = row['block_hash']
                    first_block_tx = self._highest_tx
                    current_highest_tx = self._highest_tx
                elif current_block_hash != row['block_hash']:
                    if current_block_hash in happened_blocks:
                        print('THIS IS VERY BAD')
                    else:
                        happened_blocks.append(current_block_hash)
                    blocks[current_block_hash]['transactionIndexRange'] = str(first_block_tx) + '-' + str(current_highest_tx)
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
                transaction['index'] = current_highest_tx

                if transaction['from'] not in addresses and transaction['from'] is not None:
                    addresses[transaction['from']] = {'inputTransactionHashes': [transaction['hash']],
                                                    'outputTransactionHashes': [],
                                                    'code': '0x',
                                                    'mined': [],
                                                    'inputTokenTransactions': [],
                                                    'outputTokenTransactions': [],
                                                    'ERC20Balances': [],
                                                    'ERC721Tokens': []}
                elif transaction['from'] is not None:
                    addresses[transaction['from']]['inputTransactionHashes'].append(transaction['hash'])

                if transaction['to'] not in addresses and transaction['to'] is not None:
                    addresses[transaction['to']] = {'inputTransactionHashes': [],
                                                    'outputTransactionHashes': [transaction['hash']],
                                                    'code': '0x',
                                                    'mined': [],
                                                    'inputTokenTransactions': [],
                                                    'outputTokenTransactions': [],
                                                    'ERC20Balances': [],
                                                    'ERC721Tokens': []}
                elif transaction['to'] is not None:
                    addresses[transaction['to']]['outputTransactionHashes'].append(transaction['hash'])
                transactions[transaction['hash']] = transaction
            
            # Last block transactions of the bulk
            blocks[current_block_hash]['transactionIndexRange'] = str(first_block_tx) + '-' + str(current_highest_tx)
            current_highest_tx += 1
            self._highest_tx = current_highest_tx
        transactions, addresses = self.gather_receipts(transactions, addresses)
        return (transactions, addresses)

    def gather_receipts(self, transactions: Dict, addresses: Dict) -> None:
        """
        Gathers receipts of the transactions.

        Args:
            transactions: Dictionary holding all currently proccessed transactions.
            addresses: Dictionary holding all currently processed addresses.
        """
        with open('./data/receipts.csv') as csv_f:
            csv_receipts = csv.DictReader(csv_f, delimiter=',')
            for row in csv_receipts:
                transactions[row['transaction_hash']]['cumulativeGasUsed'] = row['cumulative_gas_used']
                transactions[row['transaction_hash']]['gasUsed'] = row['gas_used']
                transactions[row['transaction_hash']]['contractAddress'] = row['contract_address']

                if (row['contract_address'] not in addresses
                    and row['contract_address'] is not None):
                    code = self._blockchain.get_code(row['contract_address'])
                    addresses[row['contract_address']] = {'inputTransactionHashes': [],
                                                    'outputTransactionHashes': [],
                                                    'code': code.hex(),
                                                    'mined': [],
                                                    'inputTokenTransactions': [],
                                                    'outputTokenTransactions': [],
                                                    'ERC20Balances': [],
                                                    'ERC721Tokens': []}
        with open('./data/logs.csv') as csv_f:
            csv_logs = csv.DictReader(csv_f, delimiter=',')
            for row in csv_logs:
                transactions[row['transaction_hash']]['logs'] = row['data']
        
        return (transactions, addresses)
    
    def add_trace_addresses(self, addresses: Dict, first_block: int, last_block: int) -> Dict:
        """
        Adds trace addresses to a list of addresses.

        Args:
            addresses: Already gathered addresses.
            first_block: Start of the block range.
            last_block: End block of the block range.
        
        Returns:
            Full address list.
        """
        address_gatherer = TraceAddressGatherer(self._interface)
        trace_addresses = address_gatherer._gather_addresses(first_block, last_block)

        for address in trace_addresses:
            if address not in addresses:
                addresses[address] = {'inputTransactionHashes': [],
                                      'outputTransactionHashes': [],
                                      'code': '0x',
                                      'mined': [],
                                      'inputTokenTransactions': [],
                                      'outputTokenTransactions': [],
                                      'ERC20Balances': [],
                                      'ERC721Tokens': []}
        
        return addresses

    def fill_addresses(self, addresses: Dict, transactions: Dict,
                       tokens: Dict, token_txs: Dict) -> Dict:
        """
        Fill addresses with transaction information.

        Args:
            addresses: Currently processed addresses.
            transactions: Currently processed transactions.
            tokens: Currently processed tokens.
            token_txs: Currently processed token transactions.
        
        Returns:
            Addresses with new information.
        """
        addresses = self.init_fill_addrs_token_data(addresses, token_txs)
        addresses_encode = {}
        for addr_hash, addr_dict in addresses.items():
            existing_data = self.address_db.get(addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_tx_str = existing_address['inputTransactionIndexes']
                output_tx_str = existing_address['outputTransactionIndexes']
                mined_str = existing_address['mined']
                input_token_txs_str = existing_address['inputTokenTransactions']
                output_token_txs_str = existing_address['outputTokenTransactions']
                erc20_balances_str = existing_address['ERC20Balances']
                erc721_tokens_str = existing_address['ERC721Tokens']
            else:
                input_tx_str = ''
                output_tx_str = ''
                mined_str = ''
                input_token_txs_str = ''
                output_token_txs_str = ''
                erc20_balances_str = ''
                erc721_tokens_str = ''

            address_encode = {}
            if addr_hash in tokens:
                if tokens[addr_hash]['type'] == 'ERC-20':
                    address_encode['tokenContract'] = 'ERC-20'
                elif tokens[addr_hash['type']] == 'ERC-721':
                    address_encode['tokenContract'] = 'ERC-721'
            else:
                address_encode['tokenContract'] = 'False'

            address_encode['balance'] = 'null'
            address_encode['code'] = addr_dict['code']
            for input_tx in addr_dict['inputTransactionHashes']:
                input_tx_str += ('|' + str(transactions[input_tx]['index'])
                                 + '+' + str(transactions[input_tx]['timestamp'])
                                 + '+' + str(transactions[input_tx]['value']))
            if input_tx_str != '' and input_tx_str[0] == '|':
                input_tx_str = input_tx_str[1:]
            address_encode['inputTransactionIndexes'] = input_tx_str
            for output_tx in addr_dict['outputTransactionHashes']:
                output_tx_str += ('|' + str(transactions[output_tx]['index'])
                                  + '+' + str(transactions[output_tx]['timestamp'])
                                  + '+' + str(transactions[input_tx]['value']))
            if output_tx_str != '' and output_tx_str[0] == '|':
                output_tx_str = output_tx_str[1:]
            address_encode['outputTransactionIndexes'] = output_tx_str
            for block_hash in addr_dict['mined']:
                mined_str += ('|' + str(block_hash))
            if mined_str != '' and mined_str[0] == '|':
                mined_str = mined_str[1:]
            address_encode['mined'] = mined_str

            addresses_encode[addr_hash] = address_encode
        # Also add token information to the addresses.
        updated_tokens = self.expand_tokens(tokens, token_txs)
        addresses_encode = self.fill_addrs_token_txs(addresses, addresses_encode, updated_tokens)
        return addresses_encode
    
    def init_fill_addrs_token_data(self, addresses: Dict, token_txs: List) -> Dict:
        """
        Fill address structures with initial token information.

        Args:
            addresses: Addresses containing workable data.
            token_txs: List of token transactions.
        
        Returns:
            Addresses enriched with token transactions data.
        """
        for token_tx in token_txs:
            if token_tx['from'] not in addresses and token_tx['from'] is not None:
                addresses[token_tx['from']] = {'inputTransactionHashes': [],
                                                'outputTransactionHashes': [],
                                                'code': '0x',
                                                'mined': [],
                                                'inputTokenTransactions': [],
                                                'outputTokenTransactions': [token_tx],
                                                'ERC20Balances': [],
                                                'ERC721Tokens': []}
            elif token_tx['from'] is not None:
                addresses[token_tx['from']]['outputTokenTransactions'].append(token_tx)
            
            if token_tx['to'] not in addresses and token_tx['to'] is not None:
                addresses[token_tx['to']] = {'inputTransactionHashes': [],
                                                'outputTransactionHashes': [],
                                                'code': '0x',
                                                'mined': [],
                                                'inputTokenTransactions': [token_tx],
                                                'outputTokenTransactions': [],
                                                'ERC20Balances': [],
                                                'ERC721Tokens': []}
            elif token_tx['to'] is not None:
                addresses[token_tx['to']]['inputTokenTransactions'].append(token_tx)
        
        return addresses
    
    def expand_tokens(self, tokens: Dict, token_txs: List) -> Dict:
        """
        Expand token list to make token information more available.

        Args:
            tokens: Tokens gathered so far (in this batch).
            token_txs: Token transactions to get other token info.

        Returns:
            Updated token list.
        """
        updated_tokens = tokens.copy()
        for token_tx in token_txs:
            if token_tx['token_address'] not in updated_tokens:
                data = self.token_db.get(token_tx['token_address'].encode())
                if data is not None:
                    updated_tokens[token_tx['token_address']] = coder.decode_token(data)
        
        return updated_tokens
                
    
    def fill_addrs_token_txs(self, addresses: Dict, addresses_encode: Dict, tokens: Dict) -> Dict:
        """
        Fills address information with token transactions and balance changes.

        Args:
            addresses: Currently processed addresses.
            addresses_encode: Addresses partially prepared for DB write.
            tokens: All relevant tokens.
        
        Returns:
            Updated addresses.
        """
        for addr_hash, addr_dict in addresses.items():
            existing_data = self.address_db.get(addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_token_txs_str = existing_address['inputTokenTransactions']
                output_token_txs_str = existing_address['outputTokenTransactions']
                erc20_balances_str = existing_address['ERC20Balances']
                erc721_tokens_str = existing_address['ERC721Tokens']
            else:
                input_token_txs_str = ''
                output_token_txs_str = ''
                erc20_balances_str = ''
                erc721_tokens_str = ''

            erc20_balances = coder.decode_erc20_balances(erc20_balances_str)
            erc721_items = coder.decode_erc721_records(erc721_tokens_str)

            for input_token_tx in addr_dict['inputTokenTransactions']:
                # If suitable token for the transaction wasn't found, it likely means that this 
                # token doesn't FULLY adhere to ERC-20/ERC-721 standard and will not be included.
                if input_token_tx['token_address'] not in tokens:
                    continue

                input_token_txs_str += ('|' + str(input_token_tx['token_address'])
                                        + '+' + str(input_token_tx['from'])
                                        + '+' + str(input_token_tx['value']))

                if tokens[input_token_tx['token_address']]['type'] == 'ERC-20':
                    erc20_balances[input_token_tx['token_address']] = (
                        erc20_balances.get(input_token_tx['token_address'], 0) - int(input_token_tx['value']))
                elif tokens[input_token_tx['token_address']]['type'] == 'ERC-721':
                    erc721_items[input_token_tx['token_address']].remove(input_token_tx['value'])
                
            if input_token_txs_str != '' and input_token_txs_str[0] == '|':
                input_token_txs_str = input_token_txs_str[1:]
            addresses_encode[addr_hash]['inputTokenTransactions'] = input_token_txs_str
            
            for output_token_tx in addr_dict['outputTokenTransactions']:
                if output_token_tx['token_address'] not in tokens:
                    continue
                if addr_hash == '0x1430064b130e4c5ba9cd032ce36818fc209313f9':
                    print('out: ' + str(output_token_tx))
                output_token_txs_str += ('|' + str(output_token_tx['token_address'])
                                        + '+' + str(output_token_tx['to'])
                                        + '+' + str(output_token_tx['value']))
                if tokens[output_token_tx['token_address']]['type'] == 'ERC-20':
                    erc20_balances[output_token_tx['token_address']] = (
                        erc20_balances.get(output_token_tx['token_address'], 0) + int(output_token_tx['value']))
                elif tokens[output_token_tx['token_address']]['type'] == 'ERC-721':
                    erc721_items[output_token_tx['token_address']].append(output_token_tx['value'])
    
            if output_token_txs_str != '' and output_token_txs_str[0] == '|':
                output_token_txs_str = output_token_txs_str[1:]
            addresses_encode[addr_hash]['outputTokenTransactions'] = output_token_txs_str

            addresses_encode[addr_hash]['ERC20Balances'] = coder.encode_erc20_balances(erc20_balances)
            addresses_encode[addr_hash]['ERC721Tokens'] = coder.encode_erc721_records(erc721_items)
        
        return addresses_encode
    
    def update_bulk_db(self, blocks: Dict, transactions: Dict,
                       addresses: Dict, tokens: Dict) -> None:
        """
        Updates the database with bulk data.

        Args:
            blocks: Dictionary containing blocks.
            transactions: Dictionary containing transactions.
            addresses: Dictionary containing addresses.
            tokens: Dictionary containing tokens.
        """
        with self.db.write_batch() as wb:
            for block_hash, block_dict in blocks.items():
                if 'transactionIndexRange' not in block_dict:
                    block_dict['transactionIndexRange'] = ''
                block_value = coder.encode_block(block_dict)
                self.db.put(b'block-' + str(block_dict['number']).encode(), block_value)
                self.db.put(b'hash-block-' + str(block_dict['hash']).encode(), str(block_dict['number']).encode())
                self.db.put(b'timestamp-block-' + str(block_dict['timestamp']).encode(), str(block_dict['number']).encode())

            for tx_hash, tx_dict in transactions.items():
                if 'logs' not in tx_dict:
                    tx_dict['logs'] = ''
                tx_value = coder.encode_transaction(tx_dict)
                self.db.put(b'transaction-' + str(tx_dict['index']).encode(), tx_value)
                self.db.put(b'tx-hash-' + tx_hash.encode(), str(tx_dict['index']).encode())
            
            for addr_hash, addr_dict in addresses.items():
                address_value = coder.encode_address(addr_dict)
                self.db.put(b'address-' + str(addr_hash).encode(), address_value)
            
            for addr_hash, token_dict in tokens.items():
                token_value = coder.encode_token(token_dict)
                self.db.put(b'token-' + str(addr_hash).encode(), token_value)
    
    def _save_addresses(self, addresses: Any) -> None:
        """
        Saves addresses of a batch to a temporary file.

        Args:
            addresses: Numpy array containing unique addresses.
        """
        if os.path.isfile('data/addresses.npy'):
            existing_addr = numpy.load('data/addresses.npy')
            new_addr = numpy.union1d(existing_addr, addresses)
            numpy.save('data/addresses.npy', new_addr)
        else:
            numpy.save('data/addresses.npy', addresses)

    def _update_address_balances(self, blockchain_height: int) -> None:
        """
        Load relevant addresses from tmp file, get their balances from Node and save them to DB.

        During this update new data will be added to blockchain and thus the DB will be out of date
        by the time it is completed. Small out of date-ness is acceptable but a longer one 
        will probabbly need to trigger a new batch update.

        Args:
            blockchain_height: Height at which sync was completed.
        """
        balance_gatherer = BalanceGatherer(self._interface)
        existing_addr = numpy.load('data/addresses.npy')
        size = existing_addr.size
        index = 0
        continue_iteration = True
        while continue_iteration:
            if index + self._bulk_size > size:
                sliced_arr = existing_addr[index:]
                continue_iteration = False
            else:
                sliced_arr = existing_addr[index:index + self._bulk_size]
                index = index + self._bulk_size
            
            addresses = sliced_arr.tolist()
            balances = balance_gatherer._gather_balances(addresses, blockchain_height)
            self._update_db_balances(balances)
        
        if os.path.exists('data/addresses.npy'):
            os.remove('data/addresses.npy')

    def _update_db_balances(self, addr_balances: Dict) -> None:
        """
        Updates balances of Ethereum addresses.

        Args:
            addr_balances: Dictionary containing 'address: balance' entries.
        """
        address_objects = {}
        for address in addr_balances:
            raw_addr = self.address_db.get(str(address).encode())
            address_objects[address] = coder.decode_address(raw_addr)
            address_objects[address]['balance'] = addr_balances[address]
        with self.db.write_batch() as wb:
            for address in address_objects:
                address_value = coder.encode_address(address_objects[address])
                self.db.put(b'address-' + str(address).encode(), address_value)

@setup_database
def update_database(db_location: str,
                    db_lock: Any,
                    interface: str,
                    confirmations: int,
                    bulk_size: int,
                    db: Any = None) -> None:
    """
    Updates database with new entries.

    Args: db_location: Path to the leveldb database.
          db_lock: Instance of the database lock (to prevent multiple access).
          blockchain: Instance of the blockchain wrapper.
          db: Database instance.
    """
    db_updater = BulkDatabaseUpdater(db, interface, confirmations, bulk_size)
    db_updater.fill_database()
