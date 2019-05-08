"""Class that updates the database."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict
import logging
import csv

import rocksdb

import src.coder as coder
from src.blockchain_wrapper import BlockchainWrapper
from src.requests.traces import TraceAddressGatherer
from src.updater.data_retriever import DataRetriever
from src.updater.balance_updater import BalanceUpdater

LOG = logging.getLogger()

csv.field_size_limit(100000000)


class DatabaseUpdater:
    """Class that updates the database."""

    def __init__(self, db: Any,
                 interface: str,
                 confirmations: int,
                 bulk_size: int,
                 process_traces: bool = False,
                 datapath: str = 'data/',
                 gather_tokens_arg: bool = True) -> None:
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
            datapath: Path for temporary file created in DB creation.
            gather_tokens: Whether to also gather token information.
        """
        self._blockchain = BlockchainWrapper(interface, confirmations)
        self.db = db
        self._confirmations = confirmations
        self._bulk_size = bulk_size
        self.process_traces = process_traces
        self.datapath = datapath
        self.gather_tokens_arg = gather_tokens_arg
        with open(self.datapath + 'progress.txt', 'r') as f:
            data = f.read().split('\n')
            self._highest_block = int(data[0])
            self._highest_token_tx = int(data[1])
            self._highest_contract_code = int(data[2])

        if interface[-4:] == '.ipc':
            self._interface = 'file://' + interface
        else:
            self._interface = interface

        self.retriever = DataRetriever(self._interface, self.datapath, self.gather_tokens_arg)
        self.balance_updater = BalanceUpdater(self._bulk_size, self.datapath,
                                              self._interface, self.db)

    def fill_database(self) -> bool:
        """Adds new entries to the database."""
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
            if self._highest_block + self._bulk_size > 30000:
                break
            # Get data from Node
            self.retriever.create_csv_files(self._highest_block, latest_block)

            # Parse the data
            blocks, transactions, addresses, address_code_asoc = self.gather_blocks()
            if self.gather_tokens_arg:
                tokens, token_txs = self.gather_tokens(transactions)
            else:
                tokens, token_txs = ({}, [])
            if self.process_traces:
                addresses = self.add_trace_addresses(addresses, self._highest_block, latest_block)
            # every 5th batch, save addresses to file and remove duplicates
            if batch_index % 5 == 0:
                self.balance_updater._save_addresses(addresses, True)
            else:
                self.balance_updater._save_addresses(addresses, False)
            addresses, addresses_write_dict, tokens, token_txs = self.fill_addresses(addresses,
                                                                                     transactions,
                                                                                     tokens,
                                                                                     token_txs)
            self.update_bulk_db(blocks, transactions, addresses, tokens,
                                addresses_write_dict, token_txs, address_code_asoc)
            self._highest_block = latest_block
            with open(self.datapath + 'progress.txt', 'w') as f:
                f.write('{}\n{}\n{}'.format(self._highest_block,
                                            self._highest_token_tx,
                                            self._highest_contract_code))

            if stop_iteration:
                break
            percentage = (self._highest_block / last_block) * 100
            LOG.info('Blockchain syncing: {:.2f}% done.'.format(percentage))

        # Update balances of all addresses
        self.balance_updater._save_addresses({}, True)
        self.balance_updater._update_address_balances(last_block)

        # Did the DB fall behind during the sync?
        if self._blockchain.get_height() - self._confirmations - last_block > 10:
            return False
        else:
            return True

        LOG.info('Bulk database update complete.')

    def gather_blocks(self) -> Tuple[Dict, Dict, Dict]:
        """
        Create dictionary representation of processed blocks.

        Returns:
            Dictionary of new blocks.
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

        transactions, addresses, address_code_asoc = self.gather_transactions(blocks)
        for miner in miners:
            if miner[0] not in addresses and miner[0] is not None:
                addresses[miner[0]] = {'code': '0x',
                                       'mined': [miner[1]],
                                       'newInputTxs': [],
                                       'newOutputTxs': [],
                                       'newInputTokens': [],
                                       'newOutputTokens': []}
            elif miner[0] is not None:
                addresses[miner[0]]['mined'].append(miner[1])

        return (blocks, transactions, addresses, address_code_asoc)

    def gather_transactions(self, blocks: Dict) -> Tuple[Dict, Dict]:
        """
        Gathers transactions and adds their hashes to blocks, as well as to addresses.

        Args:
            blocks: Processed blocks.

        Returns: Gathered transactions and addresses.
        """
        LOG.info('Gathering transactions from csv')
        transactions = {}
        addresses = {}  # type: Dict[str, Any]

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
                transaction['timestamp'] = blocks[row['block_hash']]['timestamp']
                # transaction['index'] = str(current_highest_tx)

                if transaction['from'] not in addresses and transaction['from'] is not None:
                    addresses[transaction['from']] = {'code': '0x',
                                                      'mined': [],
                                                      'newInputTxs': [],
                                                      'newOutputTxs': [(transaction['hash'],
                                                                        transaction['value'],
                                                                        transaction['timestamp'])],
                                                      'newInputTokens': [],
                                                      'newOutputTokens': []}
                elif transaction['from'] is not None:
                    addresses[transaction['from']]['newOutputTxs'].append(
                        (transaction['hash'], transaction['value'], transaction['timestamp']))

                if transaction['to'] not in addresses and transaction['to'] is not None:
                    addresses[transaction['to']] = {'code': '0x',
                                                    'mined': [],
                                                    'newInputTxs': [(transaction['hash'],
                                                                     transaction['value'],
                                                                     transaction['timestamp'])],
                                                    'newOutputTxs': [],
                                                    'newInputTokens': [],
                                                    'newOutputTokens': []}
                elif transaction['to'] is not None:
                    addresses[transaction['to']]['newInputTxs'].append(
                        (transaction['hash'], transaction['value'], transaction['timestamp']))
                transactions[transaction['hash']] = transaction

                blocks[transaction['blockHash']]['transactions'] += transaction['hash'] + '+'

            for block_hash in blocks:
                if (blocks[block_hash]['transactions'] != ''
                        and blocks[block_hash]['transactions'][-1] == '+'):
                    blocks[block_hash]['transactions'] = blocks[block_hash]['transactions'][:-1]

        transactions, addresses, address_code_asoc = self.gather_receipts(transactions, addresses)
        return (transactions, addresses, address_code_asoc)

    def gather_receipts(self, transactions: Dict, addresses: Dict) -> Tuple[Dict, Dict]:
        """
        Gathers receipts of the transactions.

        Args:
            transactions: Dictionary holding all currently proccessed transactions.
            addresses: Dictionary holding all currently processed addresses.
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
                    code = self._blockchain.get_code(row['contract_address'])
                    addresses[row['contract_address']] = {'code': '0x',
                                                          'mined': [],
                                                          'newInputTxs': [],
                                                          'newOutputTxs': [],
                                                          'newInputTokens': [],
                                                          'newOutputTokens': []}

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
                addresses[address] = {'code': '0x',
                                      'mined': [],
                                      'newInputTxs': [],
                                      'newOutputTxs': [],
                                      'newInputTokens': [],
                                      'newOutputTokens': []}

        return addresses

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
            existing_data = self.db.get(b'address-' + addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                last_input_tx_index = int(existing_address['inputTxIndex'])
                last_output_tx_index = int(existing_address['outputTxIndex'])
                last_input_token_tx_index = int(existing_address['inputTokenTxIndex'])
                last_output_token_tx_index = int(existing_address['outputTokenTxIndex'])
                last_mined_block_index = int(existing_address['minedIndex'])
            else:
                last_input_tx_index = 0
                last_output_tx_index = 0
                last_input_token_tx_index = 0
                last_output_token_tx_index = 0
                last_mined_block_index = 0

            address_encode = {}
            if 'tokenContract' in addr_dict:
                address_encode['tokenContract'] = addr_dict['tokenContract']
                if addr_hash in tokens:
                    tokens[addr_hash]['type'] = addr_dict['tokenContract']
            else:
                address_encode['tokenContract'] = 'False'

            address_encode['balance'] = 'null'
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
            address_encode['minedIndex'] = last_mined_block_index

            addresses_encode[addr_hash] = address_encode
        # Also add token information to the addresses.
        addresses_encode, updated_tokens, addresses_write_dict = self.fill_addrs_token_txs(
            addresses, addresses_encode, updated_tokens, addresses_write_dict)
        return (addresses_encode, addresses_write_dict, updated_tokens, filtered_token_txs)

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
            if token_tx['addressFrom'] not in addresses and token_tx['addressFrom'] is not None:
                addresses[token_tx['addressFrom']] = {'code': '0x',
                                                      'mined': [],
                                                      'newInputTxs': [],
                                                      'newOutputTxs': [],
                                                      'newInputTokens': [],
                                                      'newOutputTokens': [(token_tx_index,
                                                                           token_tx['timestamp'])]}
            elif token_tx['addressFrom'] is not None:
                addresses[token_tx['addressFrom']]['newOutputTokens'].append(
                    (token_tx_index, token_tx['timestamp']))

            if token_tx['addressTo'] not in addresses and token_tx['addressTo'] is not None:
                addresses[token_tx['addressTo']] = {'code': '0x',
                                                    'mined': [],
                                                    'newInputTxs': [],
                                                    'newOutputTxs': [],
                                                    'newInputTokens': [(token_tx_index,
                                                                        token_tx['timestamp'])],
                                                    'newOutputTokens': []}
            elif token_tx['addressTo'] is not None:
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
            data = self.db.get(b'token-' + token_tx['tokenAddress'].encode())
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
            Updated addresses.
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
                       address_code_asoc: Dict) -> None:
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
        """
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

        self.db.write(wb)


def update_database(db_location: str,
                    interface: str,
                    confirmations: int,
                    bulk_size: int,
                    process_traces: bool,
                    datapath: str,
                    gather_tokens: bool,
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
        db: Database instance.
    """
    db_updater = DatabaseUpdater(db, interface, confirmations,
                                 bulk_size, process_traces, datapath, gather_tokens)
    # sync occurs multiple times as present will change before sync is completed.
    while True:
        fell_behind = db_updater.fill_database()
        LOG.info('Database update has been completed.')
        fell_behind = False
        # If during sync the updater didn't fall too far behind, consider sync finished
        if not fell_behind:
            break
        LOG.info('Fell behind the blockchain during address processing, starting another sync.')
