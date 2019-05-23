"""Class that updates the database."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict
import logging
import csv

import rocksdb

import src.coder as coder
from src.blockchain_wrapper import BlockchainWrapper
from src.updater.data_retriever import DataRetriever
from src.updater.balance_updater import BalanceUpdater
from src.decorator import db_get_wrapper

LOG = logging.getLogger()

csv.field_size_limit(100000000)


class DatabaseUpdater:
    """Class that updates the database."""

    def __init__(self, db: Any,
                 interface: str,
                 confirmations: int,
                 bulk_size: int,
                 db_lock: Any,
                 internal_transactions: bool = False,
                 datapath: str = 'data/',
                 gather_tokens_arg: bool = True,
                 max_workers: int = 5) -> None:
        """
        Initialization.

        Args:
            db: Database instance.
            interface: Path to the Geth blockchain node.
            confirmations: How many confirmations a block has to have.
            bulk_size: How many blocks to be included in bulk DB update.
            db_lock: Mutex that prevents simultanious DB write and read (to prevent read errors).
            internal_transactions: Whether to gather internal transactions.
            datapath: Path for temporary file created in DB creation.
            gather_tokens: Whether to also gather token information.
            max_workers: Maximum workers in Ethereum ETL.
        """
        self._blockchain = BlockchainWrapper(interface, confirmations)
        self.db = db
        self._confirmations = confirmations
        self._bulk_size = bulk_size
        self.internal_txs = internal_transactions
        self.datapath = datapath
        self.gather_tokens_arg = gather_tokens_arg
        self.max_workers = max_workers
        self.db_lock = db_lock
        with open(self.datapath + 'progress.txt', 'r') as f:
            data = f.read().split('\n')
            self._highest_block = int(data[0])
            self._highest_token_tx = int(data[1])
            self._highest_contract_code = int(data[2])
            self._highest_internal_tx = int(data[3])

        if interface[-4:] == '.ipc':
            self._interface = 'file://' + interface
        else:
            self._interface = interface

        self.retriever = DataRetriever(self._interface, self.datapath, self.gather_tokens_arg,
                                       self.internal_txs, self.max_workers)
        self.balance_updater = BalanceUpdater(self._bulk_size, self.datapath,
                                              self._interface, self.db, self.db_lock)

    def fill_database(self) -> bool:
        """
        Adds new entries to the database.

        Returns:
            True if the sync fell behind during balance gathering phase. False otherwise.
        """
        stop_iteration = False
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

            if self._highest_block == latest_block:
                LOG.info('Database up to date, no sync needed.')
                break

            # For debugging purposes only
            # if self._highest_block + self._bulk_size > 30000:
            #     break

            # Get data from Node
            self.retriever.create_csv_files(self._highest_block, latest_block)

            # Gather the block, transaction and address data
            blocks, transactions, addresses, address_code_asoc, int_tx_asoc = self.gather_blocks()
            # Gather token data (if selected)
            if self.gather_tokens_arg:
                tokens, token_txs = self.gather_tokens(transactions)
            else:
                tokens, token_txs = ({}, [])

            # Gather internal transactions (if selected)
            txs_write_dict = {}
            internal_txs = {}
            if self.internal_txs:
                addresses, transactions, txs_write_dict, internal_txs = (
                    self.gather_internal_txs(addresses, transactions, int_tx_asoc))

            # Every 5th batch, remove duplicate addresses from the address file
            if batch_index % 5 == 0:
                self.balance_updater._save_addresses(addresses, True)
            else:
                self.balance_updater._save_addresses(addresses, False)

            # Fill addresses with transaction data
            addresses, addresses_write_dict, tokens, token_txs = self.fill_addresses(addresses,
                                                                                     transactions,
                                                                                     tokens,
                                                                                     token_txs)
            # Write all data to DB
            self.update_bulk_db(blocks, transactions, addresses, tokens, addresses_write_dict,
                                token_txs, address_code_asoc, internal_txs, txs_write_dict)
            self._highest_block = latest_block
            # Save progress to file
            with open(self.datapath + 'progress.txt', 'w') as f:
                f.write('{}\n{}\n{}\n{}'.format(self._highest_block,
                                                self._highest_token_tx,
                                                self._highest_contract_code,
                                                self._highest_internal_tx))

            if stop_iteration:
                break
            percentage = (self._highest_block / last_block) * 100
            LOG.info('Blockchain syncing: {:.2f}% done.'.format(percentage))

        # Update balances of all addresses
        self.balance_updater._save_addresses({}, True)
        self.balance_updater._update_address_balances(last_block)

        # Did the DB fall behind during the sync?
        if self._blockchain.get_height() - self._confirmations - last_block < 3:
            return False
        else:
            return True

        LOG.info('Bulk database update complete.')

    def gather_blocks(self) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
        """
        Create dictionary representation of processed blocks.

        Returns:
            Gathered blocks, transactions, addresses, and associating dictionaries.
        """
        LOG.info('Gathering blocks from csv')
        blocks = {}
        miners = []
        with open(self.datapath + 'blocks.csv') as csv_f:
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
                block['transactions'] = ''
                blocks[block['hash']] = block
                miners.append((block['miner'], block['hash']))

        transactions, addresses, address_code_asoc, int_tx_asoc = self.gather_transactions(blocks)
        for miner in miners:
            if miner[0] not in addresses and miner[0] is not None:
                addresses[miner[0]] = {'code': '0x',
                                       'mined': [miner[1]],
                                       'newInputTxs': [],
                                       'newOutputTxs': [],
                                       'newInputTokens': [],
                                       'newOutputTokens': [],
                                       'newIntInputTxs': [],
                                       'newIntOutputTxs': []}
            elif miner[0] is not None:
                addresses[miner[0]]['mined'].append(miner[1])

        return (blocks, transactions, addresses, address_code_asoc, int_tx_asoc)

    def gather_transactions(self, blocks: Dict) -> Tuple[Dict, Dict, Dict, Dict]:
        """
        Gathers transactions and adds their hashes to blocks, as well as to addresses.

        Args:
            blocks: Processed blocks.

        Returns: Gathered transactions, addresses and associating dictionaries.
        """
        LOG.info('Gathering transactions from csv')
        transactions = {}
        addresses = {}  # type: Dict[str, Any]
        internal_tx_asoc = {}

        with open(self.datapath + 'transactions.csv') as csv_f:
            csv_transactions = csv.DictReader(csv_f, delimiter=',')
            for row in csv_transactions:
                transaction = {}
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
                transaction['internalTxIndex'] = 0
                transaction['timestamp'] = blocks[row['block_hash']]['timestamp']
                # transaction['index'] = str(current_highest_tx)
                internal_tx_asoc[row['block_number'] + '-' + row['transaction_index']] = (
                    row['hash'])

                if transaction['from'] not in addresses and transaction['from'] != '':
                    addresses[transaction['from']] = {'code': '0x',
                                                      'mined': [],
                                                      'newInputTxs': [],
                                                      'newOutputTxs': [(transaction['hash'],
                                                                        transaction['value'],
                                                                        transaction['timestamp'])],
                                                      'newInputTokens': [],
                                                      'newOutputTokens': [],
                                                      'newIntInputTxs': [],
                                                      'newIntOutputTxs': []}
                elif transaction['from'] != '':
                    addresses[transaction['from']]['newOutputTxs'].append(
                        (transaction['hash'], transaction['value'], transaction['timestamp']))

                if transaction['to'] not in addresses and transaction['to'] != '':
                    addresses[transaction['to']] = {'code': '0x',
                                                    'mined': [],
                                                    'newInputTxs': [(transaction['hash'],
                                                                     transaction['value'],
                                                                     transaction['timestamp'])],
                                                    'newOutputTxs': [],
                                                    'newInputTokens': [],
                                                    'newOutputTokens': [],
                                                    'newIntInputTxs': [],
                                                    'newIntOutputTxs': []}
                elif transaction['to'] != '':
                    addresses[transaction['to']]['newInputTxs'].append(
                        (transaction['hash'], transaction['value'], transaction['timestamp']))
                transactions[transaction['hash']] = transaction

                blocks[transaction['blockHash']]['transactions'] += transaction['hash'] + '+'

            # Remove last separator
            for block_hash in blocks:
                if (blocks[block_hash]['transactions'] != ''
                        and blocks[block_hash]['transactions'][-1] == '+'):
                    blocks[block_hash]['transactions'] = blocks[block_hash]['transactions'][:-1]

        transactions, addresses, address_code_asoc = self.gather_receipts(transactions, addresses)
        return (transactions, addresses, address_code_asoc, internal_tx_asoc)

    def gather_receipts(self, transactions: Dict, addresses: Dict) -> Tuple[Dict, Dict, Dict]:
        """
        Gathers receipts of the transactions.

        Args:
            transactions: Dictionary holding all currently proccessed transactions.
            addresses: Dictionary holding all currently processed addresses.

        Returns:
            Updated transaction and address data, address-contract associating dictionary
        """
        LOG.info('Gathering receipts from csv')
        with open(self.datapath + 'receipts.csv') as csv_f:
            csv_receipts = csv.DictReader(csv_f, delimiter=',')
            for row in csv_receipts:
                transactions[row['transaction_hash']]['cumulativeGasUsed'] = (
                    row['cumulative_gas_used'])
                transactions[row['transaction_hash']]['gasUsed'] = row['gas_used']
                transactions[row['transaction_hash']]['contractAddress'] = row['contract_address']

                if (row['contract_address'] not in addresses
                        and row['contract_address'] != ''):
                    addresses[row['contract_address']] = {'code': '0x',
                                                          'mined': [],
                                                          'newInputTxs': [],
                                                          'newOutputTxs': [],
                                                          'newInputTokens': [],
                                                          'newOutputTokens': [],
                                                          'newIntInputTxs': [],
                                                          'newIntOutputTxs': []}
        # Add log data
        with open(self.datapath + 'logs.csv') as csv_f:
            csv_logs = csv.DictReader(csv_f, delimiter=',')
            for row in csv_logs:
                transactions[row['transaction_hash']]['logs'] = row['data']

                transactions[row['transaction_hash']]['logs'] = ''
                transactions[row['transaction_hash']]['logs'] += row['data'] + '+'
                for topic in row['topics'].split(','):
                    transactions[row['transaction_hash']]['logs'] += topic + '-'

                if (transactions[row['transaction_hash']]['logs'] != ''
                        and transactions[row['transaction_hash']]['logs'][-1] == '-'):
                    transactions[row['transaction_hash']]['logs'] = (
                        transactions[row['transaction_hash']]['logs'][:-1])

                if (transactions[row['transaction_hash']]['logs'] != ''
                        and transactions[row['transaction_hash']]['logs'][-1] == '+'):
                    transactions[row['transaction_hash']]['logs'] = (
                        transactions[row['transaction_hash']]['logs'][:-1])

                transactions[row['transaction_hash']]['logs'] += topic + '|'

        # Add contract codes
        address_code_asoc = {}
        with open(self.datapath + 'contracts.csv') as csv_f:
            csv_contracts = csv.DictReader(csv_f, delimiter=',')
            for row in csv_contracts:
                self._highest_contract_code += 1
                address_code_asoc['address-contract-' + str(self._highest_contract_code)] = (
                    row['bytecode'])
                addresses[row['address']]['code'] = str(self._highest_contract_code)
                if row['is_erc20'] == 'True':
                    addresses[row['address']]['tokenContract'] = 'ERC-20'
                if row['is_erc721'] == 'True':
                    addresses[row['address']]['tokenContract'] = 'ERC-721'

        return (transactions, addresses, address_code_asoc)

    def gather_tokens(self, transactions: Dict) -> Tuple[Dict, List]:
        """
        Gathers information about newly created ERC-20 and ERC-721 tokens.

        Args:
            transactions: Gathered transactions for giving token transactions some context.

        Returns:
            Dictionary of format 'address: token_data', List of token transactions.
        """
        tokens = {}
        with open(self.datapath + 'tokens.csv') as csv_f:
            csv_tokens = csv.DictReader(csv_f, delimiter=',')
            for row in csv_tokens:
                token = {}
                token['symbol'] = row['symbol']
                token['name'] = row['name']
                token['decimals'] = row['decimals']
                token['totalSupply'] = row['total_supply']
                token['txIndex'] = 0
                token['transactions'] = []
                tokens[row['address']] = token

        token_txs = []
        with open(self.datapath + 'token_transfers.csv') as csv_f:
            csv_tokens_tx = csv.DictReader(csv_f, delimiter=',')
            for row in csv_tokens_tx:
                token_tx = {}
                token_tx['tokenAddress'] = row['token_address']
                token_tx['addressFrom'] = row['from_address']
                token_tx['addressTo'] = row['to_address']
                token_tx['value'] = row['value']
                token_tx['transactionHash'] = row['transaction_hash']
                token_tx['timestamp'] = transactions[token_tx['transactionHash']]['timestamp']

                token_txs.append(token_tx)

        return (tokens, token_txs)

    def gather_internal_txs(self, addresses: Dict, transactions: Dict, int_tx_asoc: Dict) -> Dict:
        """
        Gathers internal transactions.

        Args:
            addresses: Already gathered addresses.
            transactions: Gathered normal transactions.
            int_tx_asoc: Dictionary containing asociative info of txs and internal txs.

        Returns:
            Updated transactions, addresses, and associating dictionary.
        """
        internal_txs = {}
        txs_write_dict = {}
        with open(self.datapath + 'traces.csv') as csv_f:
            csv_int_tx = csv.DictReader(csv_f, delimiter=',')
            for csv_tx in csv_int_tx:
                int_tx = {}
                int_tx['from'] = csv_tx['from_address']
                int_tx['to'] = csv_tx['to_address']
                int_tx['value'] = csv_tx['value']
                int_tx['input'] = csv_tx['input']
                int_tx['output'] = csv_tx['output']
                int_tx['traceType'] = csv_tx['trace_type']
                int_tx['callType'] = csv_tx['call_type']
                int_tx['rewardType'] = csv_tx['reward_type']
                int_tx['gas'] = csv_tx['gas']
                int_tx['gasUsed'] = csv_tx['gas_used']
                tx_hash = int_tx_asoc[csv_tx['block_number'] + '-' + csv_tx['transaction_index']]
                int_tx['transactionHash'] = tx_hash
                int_tx['timestamp'] = transactions[tx_hash]['timestamp']
                int_tx['error'] = csv_tx['error']

                self._highest_internal_tx += 1
                internal_txs[self._highest_internal_tx] = int_tx
                transactions[tx_hash]['internalTxIndex'] += 1
                str_index = str(transactions[tx_hash]['internalTxIndex'])
                txs_write_dict[tx_hash + '-tit-' + str_index] = (
                    self._highest_internal_tx)

                if int_tx['from'] not in addresses and int_tx['from'] != '':
                    addresses[int_tx['from']] = {'code': '0x',
                                                 'mined': [],
                                                 'newInputTxs': [],
                                                 'newOutputTxs': [],
                                                 'newInputTokens': [],
                                                 'newOutputTokens': [],
                                                 'newIntInputTxs': [],
                                                 'newIntOutputTxs': [(self._highest_internal_tx,
                                                                      int_tx['value'],
                                                                      int_tx['timestamp'])]}
                elif int_tx['from'] != '':
                    addresses[int_tx['from']]['newIntOutputTxs'].append(
                        (self._highest_internal_tx, int_tx['value'], int_tx['timestamp']))

                if int_tx['to'] not in addresses and int_tx['to'] != '':
                    addresses[int_tx['to']] = {'code': '0x',
                                               'mined': [],
                                               'newInputTxs': [],
                                               'newOutputTxs': [],
                                               'newInputTokens': [],
                                               'newOutputTokens': [],
                                               'newIntInputTxs': [(self._highest_internal_tx,
                                                                   int_tx['value'],
                                                                   int_tx['timestamp'])],
                                               'newIntOutputTxs': []}
                elif int_tx['to'] != '':
                    addresses[int_tx['to']]['newIntInputTxs'].append(
                        (self._highest_internal_tx, int_tx['value'], int_tx['timestamp']))

                internal_txs[self._highest_internal_tx] = int_tx
        return (addresses, transactions, txs_write_dict, internal_txs)

    def fill_addresses(self, addresses: Dict, transactions: Dict,
                       tokens: Dict, token_txs: List) -> Tuple[Dict, Dict]:
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
        LOG.info('Filling addresses.')
        updated_tokens, filtered_token_txs = self.expand_tokens(tokens, token_txs)
        addresses, updated_tokens = self.fill_addresses_tokens(addresses,
                                                               updated_tokens,
                                                               filtered_token_txs)
        addresses_encode = {}
        addresses_write_dict = {}
        for addr_hash, addr_dict in addresses.items():
            existing_data = db_get_wrapper(self.db, b'address-' + addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                last_input_tx_index = int(existing_address['inputTxIndex'])
                last_output_tx_index = int(existing_address['outputTxIndex'])
                last_input_token_tx_index = int(existing_address['inputTokenTxIndex'])
                last_output_token_tx_index = int(existing_address['outputTokenTxIndex'])
                last_input_int_tx_index = int(existing_address['inputIntTxIndex'])
                last_output_int_tx_index = int(existing_address['outputIntTxIndex'])
                last_mined_block_index = int(existing_address['minedIndex'])
            else:
                last_input_tx_index = 0
                last_output_tx_index = 0
                last_input_token_tx_index = 0
                last_output_token_tx_index = 0
                last_input_int_tx_index = 0
                last_output_int_tx_index = 0
                last_mined_block_index = 0

            address_encode = {}
            if existing_data is not None:
                address_encode['tokenContract'] = existing_address['tokenContract']
                if addr_hash in updated_tokens:
                    updated_tokens[addr_hash]['type'] = existing_address['tokenContract']
            else:
                if 'tokenContract' in addr_dict:
                    address_encode['tokenContract'] = addr_dict['tokenContract']
                    if addr_hash in updated_tokens:
                        updated_tokens[addr_hash]['type'] = addr_dict['tokenContract']
                else:
                    address_encode['tokenContract'] = 'False'

            address_encode['balance'] = 'null'
            if existing_data is not None:
                address_encode['code'] = existing_address['code']
            else:
                address_encode['code'] = addr_dict['code']

            for input_tx in addr_dict['newInputTxs']:
                last_input_tx_index += 1
                addresses_write_dict[addr_hash + '-i-' + str(last_input_tx_index)] = (
                    str(input_tx[0]) + '-' + str(input_tx[1]) + '-' + str(input_tx[2]))
            for output_tx in addr_dict['newOutputTxs']:
                last_output_tx_index += 1
                addresses_write_dict[addr_hash + '-o-' + str(last_output_tx_index)] = (
                    str(output_tx[0]) + '-' + str(output_tx[1]) + '-' + str(output_tx[2]))
            for mined_hash in addr_dict['mined']:
                last_mined_block_index += 1
                addresses_write_dict[addr_hash + '-b-' + str(last_mined_block_index)] = mined_hash

            address_encode['inputTxIndex'] = last_input_tx_index
            address_encode['outputTxIndex'] = last_output_tx_index
            address_encode['inputTokenTxIndex'] = last_input_token_tx_index
            address_encode['outputTokenTxIndex'] = last_output_token_tx_index
            address_encode['inputIntTxIndex'] = last_input_int_tx_index
            address_encode['outputIntTxIndex'] = last_output_int_tx_index
            address_encode['minedIndex'] = last_mined_block_index

            addresses_encode[addr_hash] = address_encode
        # Also add token information to the addresses.
        addresses_encode, updated_tokens, addresses_write_dict = self.fill_addrs_token_txs(
            addresses, addresses_encode, updated_tokens, addresses_write_dict)
        # Also add internal transactions to addresses
        addresses_encode, addresses_write_dict = self.fill_addrs_int_txs(
            addresses, addresses_encode, addresses_write_dict)

        return (addresses_encode, addresses_write_dict, updated_tokens, filtered_token_txs)

    def fill_addrs_int_txs(self, addresses: Dict, addresses_encode: Dict,
                           addresses_write_dict: Dict) -> Tuple[Dict, Dict, Dict]:
        """
        Fills address information with internal transactions.

        Args:
            addresses: Currently processed addresses.
            addresses_encode: Addresses partially prepared for DB write.
            addresses_write_dict: Dictionary containing info connecting addresses with their txs.

        Returns:
            Updated addresses.
        """
        for addr_hash, addr_dict in addresses.items():
            last_input_int_tx_index = addresses_encode[addr_hash]['inputIntTxIndex']
            last_output_int_tx_index = addresses_encode[addr_hash]['outputIntTxIndex']

            for input_tx in addr_dict['newIntInputTxs']:
                last_input_int_tx_index += 1
                addresses_write_dict[addr_hash + '-ii-' + str(last_input_int_tx_index)] = (
                    str(input_tx[0]) + '-' + str(input_tx[1]) + '-' + str(input_tx[2]))

            for output_tx in addr_dict['newIntOutputTxs']:
                last_output_int_tx_index += 1
                addresses_write_dict[addr_hash + '-io-' + str(last_output_int_tx_index)] = (
                    str(output_tx[0]) + '-' + str(output_tx[1]) + '-' + str(output_tx[2]))

            addresses_encode[addr_hash]['inputIntTxIndex'] = last_input_int_tx_index
            addresses_encode[addr_hash]['outputIntTxIndex'] = last_output_int_tx_index

        return (addresses_encode, addresses_write_dict)

    def fill_addresses_tokens(self, addresses: Dict, tokens: Dict,
                              token_txs: Dict) -> Tuple[Dict, Dict]:
        """
        Fill addresses and tokens with token transactions.

        Args:
            addresses: Addresses containing workable data.
            tokens: Tokens whose transactions were found.
            token_txs: List of token transactions.

        Returns:
            Addresses enriched with token transactions data.
        """
        for token_tx_index, token_tx in token_txs.items():
            if token_tx['addressFrom'] not in addresses and token_tx['addressFrom'] != '':
                addresses[token_tx['addressFrom']] = {'code': '0x',
                                                      'mined': [],
                                                      'newInputTxs': [],
                                                      'newOutputTxs': [],
                                                      'newInputTokens': [],
                                                      'newOutputTokens': [(token_tx_index,
                                                                           token_tx['timestamp'])],
                                                      'newIntInputTxs': [],
                                                      'newIntOutputTxs': []}
            elif token_tx['addressFrom'] != '':
                addresses[token_tx['addressFrom']]['newOutputTokens'].append(
                    (token_tx_index, token_tx['timestamp']))

            if token_tx['addressTo'] not in addresses and token_tx['addressTo'] != '':
                addresses[token_tx['addressTo']] = {'code': '0x',
                                                    'mined': [],
                                                    'newInputTxs': [],
                                                    'newOutputTxs': [],
                                                    'newInputTokens': [(token_tx_index,
                                                                        token_tx['timestamp'])],
                                                    'newOutputTokens': [],
                                                    'newIntInputTxs': [],
                                                    'newIntOutputTxs': []}
            elif token_tx['addressTo'] != '':
                addresses[token_tx['addressTo']]['newInputTokens'].append(
                    (token_tx_index, token_tx['timestamp']))

            tokens[token_tx['tokenAddress']]['transactions'].append(
                (token_tx_index, token_tx['timestamp']))

        return (addresses, tokens)

    def expand_tokens(self, tokens: Dict, token_txs: List) -> Tuple[Dict, Dict]:
        """
        Find all relevant tokens from DB and reject not-found transactions.

        Args:
            tokens: Tokens gathered so far (in this batch).
            token_txs: Token transactions to get other token info.

        Returns:
            Updated token list.
        """
        full_tokens = {}
        filtered_txs = {}
        for token_tx in token_txs:
            data = db_get_wrapper(self.db, b'token-' + token_tx['tokenAddress'].encode())
            if data is not None:
                db_token = coder.decode_token(data)
                db_token['transactions'] = []
                full_tokens[token_tx['tokenAddress']] = db_token
                self._highest_token_tx += 1
                filtered_txs[self._highest_token_tx] = token_tx
            elif token_tx['tokenAddress'] in tokens:
                full_tokens[token_tx['tokenAddress']] = tokens[token_tx['tokenAddress']]
                self._highest_token_tx += 1
                filtered_txs[self._highest_token_tx] = token_tx

        for token in tokens:
            if token not in full_tokens:
                full_tokens[token] = tokens[token]

        return (full_tokens, filtered_txs)

    def fill_addrs_token_txs(self, addresses: Dict, addresses_encode: Dict,
                             tokens: Dict, addresses_write_dict: Dict) -> Tuple[Dict, Dict, Dict]:
        """
        Fills address information with token transactions.

        Args:
            addresses: Currently processed addresses.
            addresses_encode: Addresses partially prepared for DB write.
            tokens: All relevant tokens.
            addresses_write_dict: Dictionary containing info connecting addresses with their txs.

        Returns:
            Updated addresses, tokens, and associating dictionary.
        """
        for addr_hash, addr_dict in addresses.items():
            last_input_token_tx_index = addresses_encode[addr_hash]['inputTokenTxIndex']
            last_output_token_tx_index = addresses_encode[addr_hash]['outputTokenTxIndex']

            for input_token_tx in addr_dict['newInputTokens']:
                last_input_token_tx_index += 1
                addresses_write_dict[addr_hash + '-ti-' + str(last_input_token_tx_index)] = (
                    str(input_token_tx[0]) + '-' + str(input_token_tx[1]))
            for output_token_tx in addr_dict['newOutputTokens']:
                last_output_token_tx_index += 1
                addresses_write_dict[addr_hash + '-to-' + str(last_output_token_tx_index)] = (
                    str(output_token_tx[0]) + '-' + str(output_token_tx[1]))
            addresses_encode[addr_hash]['inputTokenTxIndex'] = last_input_token_tx_index
            addresses_encode[addr_hash]['outputTokenTxIndex'] = last_output_token_tx_index

        for token_addr, token_dict in tokens.items():
            token_tx_index = token_dict['txIndex']
            for token_tx in token_dict['transactions']:
                token_tx_index += 1
                addresses_write_dict[token_addr + '-tt-' + str(token_tx_index)] = (
                    str(token_tx[0]) + '-' + str(token_tx[1]))

            token_dict['txIndex'] = token_tx_index

        return (addresses_encode, tokens, addresses_write_dict)

    def update_bulk_db(self, blocks: Dict, transactions: Dict, addresses: Dict,
                       tokens: Dict, addresses_write_dict: Dict, token_txs: Dict,
                       address_code_asoc: Dict, internal_txs: Dict, txs_write_dict: Dict) -> None:
        """
        Updates the database with bulk data.

        Args:
            blocks: Dictionary containing blocks.
            transactions: Dictionary containing transactions.
            addresses: Dictionary containing addresses.
            tokens: Dictionary containing tokens.
            addresses_write_dict: Data connecting addresses to their blocks/txs.
            token_txs: Dictionary containing token transactions.
            address_code_asoc: Contract codes of addresses (saved separately due to lot of data).
            internal_txs: Internal transactions ti be written to DB.
            txs_write_dict: Associations between internal txs and txs.
        """
        self.db_lock.acquire()
        print('lock started')
        LOG.info('Writing to database.')
        wb = rocksdb.WriteBatch()
        for block_hash, block_dict in blocks.items():
            if 'transactionIndexRange' not in block_dict:
                block_dict['transactionIndexRange'] = ''
            block_value = coder.encode_block(block_dict)
            wb.put(b'block-' + str(block_dict['number']).encode(), block_value)
            wb.put(b'hash-block-' + str(block_dict['hash']).encode(),
                   str(block_dict['number']).encode())
            wb.put(b'timestamp-block-' + str(block_dict['timestamp']).encode(),
                   str(block_dict['number']).encode())

        for tx_hash, tx_dict in transactions.items():
            if 'logs' not in tx_dict:
                tx_dict['logs'] = ''
            tx_value = coder.encode_transaction(tx_dict)
            wb.put(b'transaction-' + tx_hash.encode(), tx_value)

        for addr_hash, addr_dict in addresses.items():
            address_value = coder.encode_address(addr_dict)
            wb.put(b'address-' + str(addr_hash).encode(), address_value)

        for addr_hash, token_dict in tokens.items():
            token_value = coder.encode_token(token_dict)
            wb.put(b'token-' + str(addr_hash).encode(), token_value)

        for token_tx_index, token_tx_dict in token_txs.items():
            token_tx_value = coder.encode_token_tx(token_tx_dict)
            wb.put(b'token-tx-' + str(token_tx_index).encode(), token_tx_value)

        for addr_key, addr_data in addresses_write_dict.items():
            wb.put(b'associated-data-' + str(addr_key).encode(), str(addr_data).encode())

        for code_key, code_data in address_code_asoc.items():
            wb.put(code_key.encode(), code_data.encode())

        for tx_key, tx_data in txs_write_dict.items():
            wb.put(b'associated-data-' + tx_key.encode(), str(tx_data).encode())

        for internal_tx_index, internal_tx_dict in internal_txs.items():
            internal_tx_value = coder.encode_internal_tx(internal_tx_dict)
            wb.put(b'internal-tx-' + str(internal_tx_index).encode(), internal_tx_value)

        self.db.write(wb)
        self.db_lock.release()
        print('lock ended')


def update_database(db_location: str,
                    interface: str,
                    confirmations: int,
                    bulk_size: int,
                    process_traces: bool,
                    datapath: str,
                    gather_tokens: bool,
                    max_workers: int,
                    db_lock: Any,
                    db: Any = None) -> None:
    """
    Updates database with new entries.

    Args:
        db_location: Where the DB is located.
        interface: Path to the Geth blockchain node.
        confirmations: How many confirmations a block has to have.
        bulk_size: How many blocks to be included in bulk DB update.
        process_traces: Whether to get addresses from traces.
        datapath: Path for temporary file created in DB creation.
        gather_tokens: Whether to also gather token information.
        max_workers: Maximum workers in Ethereum ETL.
        db_lock: Mutex that prevents simultanious DB write and read (to prevent read errors).
        db: Database instance.
    """
    db_updater = DatabaseUpdater(db, interface, confirmations, bulk_size, db_lock, process_traces,
                                 datapath, gather_tokens, max_workers)
    # sync occurs multiple times as present will change before sync is completed.
    while True:
        fell_behind = db_updater.fill_database()
        LOG.info('Database update has been completed.')
        # fell_behind = False
        # If during sync the updater didn't fall too far behind, consider sync finished
        if not fell_behind:
            break
        LOG.info('Fell behind the blockchain during address processing, starting another sync.')
