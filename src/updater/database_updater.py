"""Class that updates the database."""
# from multiprocessing import Lock
from typing import Any, Tuple, List, Dict
import logging
import csv
import time

import rocksdb

import src.coder as coder
from src.blockchain_wrapper import BlockchainWrapper
from src.requests.traces import TraceAddressGatherer
from src.updater.data_retriever import DataRetriever
from src.updater.balance_updater import BalanceUpdater
from src.requests.blocks_transactions import BlocksTransactionsGatherer
from src.requests.receipts import ReceiptsGatherer
from src.requests.tokens import TokensGatherer

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
            self._highest_tx = int(data[1])

        if interface[-4:] == '.ipc':
            self._interface = 'file://' + interface
        else:
            self._interface = interface

        self.retriever = DataRetriever(self._interface, self.datapath, self.gather_tokens_arg)
        self.balance_updater = BalanceUpdater(self._bulk_size, self.datapath,
                                              self._interface, self.db)
        self.blocks_gatherer = BlocksTransactionsGatherer(self._interface)
        self.receipts_gatherer = ReceiptsGatherer(self._interface)
        self.tokens_gatherer = TokensGatherer(self._interface)

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
            # TODO: remove later
            if self._highest_block + self._bulk_size > 50000:
               break
            # Get data from Node
            # self.retriever.create_csv_files(self._highest_block, latest_block)

            # Parse the data
            blocks, transactions, addresses, contract_addresses, logs = self.gather_blocks(self._highest_block, latest_block)
            if self.gather_tokens_arg:
                tokens, token_txs = self.gather_tokens(transactions, contract_addresses, logs)
            else:
                tokens, token_txs = ({}, [])
            if self.process_traces:
                addresses = self.add_trace_addresses(addresses, self._highest_block, latest_block)
            # every 10th batch, save addresses to file and remove duplicates
            if batch_index % 5 == 0:
                self.balance_updater._save_addresses(addresses, True)
            else:
                self.balance_updater._save_addresses(addresses, False)
            addresses = self.fill_addresses(addresses, transactions, tokens, token_txs)
            self.update_bulk_db(blocks, transactions, addresses, tokens)
            self._highest_block = latest_block
            with open(self.datapath + 'progress.txt', 'w') as f:
                f.write('{}\n{}'.format(self._highest_block, self._highest_tx))

            if stop_iteration:
                break
            percentage = (self._highest_block / last_block) * 100
            LOG.info('Blockchain syncing: {:.2f}% done.'.format(percentage))
            time.sleep(2)


        # Update balances of all addresses
        self.balance_updater._save_addresses({}, True)
        self.balance_updater._update_address_balances(last_block)

        # Did the DB fall behind during the sync?
        if self._blockchain.get_height() - self._confirmations - last_block > 10:
            return False
        else:
            return True

        LOG.info('Bulk database update complete.')

    def gather_blocks(self, first_block: int, last_block: int) -> Tuple[Dict, Dict, Dict]:
        """
        Create dictionary representation of processed blocks.

        Returns:
            Dictionary of new blocks.
        """
        LOG.info('Gathering blocks from csv')
        blocks = {}
        miners = []
        contract_addresses = []
        logs = []
        raw_blocks, raw_transactions = self.blocks_gatherer.gather_blocks_and_transactions(first_block, last_block)
        for raw_block_hash, raw_block in raw_blocks.items():
            block = {}
            block['difficulty'] = str(int(raw_block['difficulty'], 16))
            block['extraData'] = raw_block['extraData']
            block['gasLimit'] = str(int(raw_block['gasLimit'], 16))
            block['gasUsed'] = str(int(raw_block['gasUsed'], 16))
            block['hash'] = raw_block['hash']
            block['logsBloom'] = raw_block['logsBloom']
            block['miner'] = raw_block['miner']
            block['nonce'] = str(int(raw_block['nonce'], 16))
            block['number'] = str(int(raw_block['number'], 16))
            block['parentHash'] = raw_block['parentHash']
            block['sha3Uncles'] = raw_block['sha3Uncles']
            block['size'] = str(int(raw_block['size'], 16))
            block['timestamp'] = str(int(raw_block['timestamp'], 16))
            block['totalDifficulty'] = str(int(raw_block['totalDifficulty'], 16))
            block['transactions'] = ''
            blocks[raw_block_hash] = block
            miners.append((block['miner'], raw_block_hash))

        transactions, addresses, block_contract_addresses, block_logs = self.gather_transactions(raw_transactions, blocks)
        contract_addresses += block_contract_addresses
        logs += block_logs
        for miner in miners:
            if miner[0] not in addresses and miner[0] is not None:
                addresses[miner[0]] = {'inputTransactionHashes': [],
                                       'outputTransactionHashes': [],
                                       'code': '0x',
                                       'mined': [miner[1]],
                                       'inputTokenTransactions': [],
                                       'outputTokenTransactions': []}
            elif miner[0] is not None:
                addresses[miner[0]]['mined'].append(miner[1])

        return (blocks, transactions, addresses, contract_addresses, logs)

    def gather_transactions(self, raw_transactions: Dict, blocks: Dict) -> Tuple[Dict, Dict]:
        """
        Gathers transactions and adds their hashes to blocks, as well as to addresses.

        Args:
            blocks: Processed blocks.

        Returns: Gathered transactions and addresses.
        """
        LOG.info('Gathering transactions from csv')
        transactions = {}
        addresses = {}  # type: Dict[str, Any]
        for tx_hash, raw_tx in raw_transactions.items():
            transaction = {}
            transaction['blockHash'] = raw_tx['blockHash']
            transaction['blockNumber'] = str(int(raw_tx['blockNumber'], 16))
            transaction['from'] = raw_tx['from']
            transaction['gas'] = str(int(raw_tx['gas'], 16))
            transaction['gasPrice'] = str(int(raw_tx['gasPrice'], 16))
            transaction['hash'] = raw_tx['hash']
            transaction['input'] = raw_tx['input']
            transaction['nonce'] = raw_tx['nonce']
            transaction['to'] = raw_tx['to']
            transaction['value'] = str(int(raw_tx['value'], 16))
            transaction['timestamp'] = blocks[transaction['blockHash']]['timestamp']

            if transaction['from'] not in addresses and transaction['from'] is not None:
                addresses[transaction['from']] = {'inputTransactionHashes':
                                                    [transaction['hash']],
                                                    'outputTransactionHashes': [],
                                                    'code': '0x',
                                                    'mined': [],
                                                    'inputTokenTransactions': [],
                                                    'outputTokenTransactions': []}
            elif transaction['from'] is not None:
                addresses[transaction['from']]['inputTransactionHashes'].append(
                    transaction['hash'])

            if transaction['to'] not in addresses and transaction['to'] is not None:
                addresses[transaction['to']] = {'inputTransactionHashes': [],
                                                'outputTransactionHashes':
                                                [transaction['hash']],
                                                'code': '0x',
                                                'mined': [],
                                                'inputTokenTransactions': [],
                                                'outputTokenTransactions': []}
            elif transaction['to'] is not None:
                addresses[transaction['to']]['outputTransactionHashes'].append(
                    transaction['hash'])
            transactions[transaction['hash']] = transaction

            blocks[transaction['blockHash']]['transactions'] += transaction['hash'] + '+'

        for block_hash in blocks:
            if (blocks[block_hash]['transactions'] != ''
                    and blocks[block_hash]['transactions'][-1] == '+'):
                blocks[block_hash]['transactions'] = blocks[block_hash]['transactions'][:-1]

        transactions, addresses, contract_addresses, logs = self.gather_receipts(transactions, addresses)
        return (transactions, addresses, contract_addresses, logs)

    def gather_receipts(self, transactions: Dict, addresses: Dict) -> Tuple[Dict, Dict]:
        """
        Gathers receipts of the transactions.

        Args:
            transactions: Dictionary holding all currently proccessed transactions.
            addresses: Dictionary holding all currently processed addresses.
        """
        LOG.info('Gathering receipts from csv')
        raw_receipts = self.receipts_gatherer.gather_receipts(list(transactions))
        contract_addresses = []
        logs = []
        for tx_hash, raw_receipt in raw_receipts.items():
            transactions[tx_hash]['cumulativeGasUsed'] = str(int(raw_receipt['cumulativeGasUsed'], 16))
            transactions[tx_hash]['gasUsed'] = str(int(raw_receipt['gasUsed'], 16))
            
            if raw_receipt['contractAddress'] is not None:
                contract_addresses.append(raw_receipt['contractAddress'])
                transactions[tx_hash]['contractAddress'] = raw_receipt['contractAddress']
            else:
                transactions[tx_hash]['contractAddress'] = None

            if (transactions[tx_hash]['contractAddress'] not in addresses
                        and transactions[tx_hash]['contractAddress'] is not None):
                    code = self._blockchain.get_code(transactions[tx_hash]['contractAddress'])
                    addresses[transactions[tx_hash]['contractAddress']] = {'inputTransactionHashes': [],
                                                          'outputTransactionHashes': [],
                                                          'code': code.hex(),
                                                          'mined': [],
                                                          'inputTokenTransactions': [],
                                                          'outputTokenTransactions': []}
            transactions[tx_hash]['logs'] = ''
            for log in raw_receipt['logs']:
                logs.append(log)
                transactions[tx_hash]['logs'] += log['data'] + '+'
                for topic in log['topics']:
                    transactions[tx_hash]['logs'] += topic + '-'
                
                if (transactions[tx_hash]['logs'] != ''
                        and transactions[tx_hash]['logs'][-1] == '-'):
                    transactions[tx_hash]['logs'] = transactions[tx_hash]['logs'][:-1]
                
                if (transactions[tx_hash]['logs'] != ''
                            and transactions[tx_hash]['logs'][-1] == '+'):
                    transactions[tx_hash]['logs'] = transactions[tx_hash]['logs'][:-1]
                
                transactions[tx_hash]['logs'] += topic + '|'
            
            if (transactions[tx_hash]['logs'] != ''
                    and transactions[tx_hash]['logs'][-1] == '|'):
                transactions[tx_hash]['logs'] = transactions[tx_hash]['logs'][:-1]

        return (transactions, addresses, contract_addresses, logs)

    def gather_tokens(self, transactions: Dict, contract_addresses: List[str], logs: List) -> Tuple[Dict, List]:
        """
        Gathers information about newly created ERC-20 and ERC-721 tokens.

        Args:
            transactions: Gathered transactions for giving token transactions some context.

        Returns:
            Dictionary of format 'address: token_data', List of token transactions.
        """
        tokens = {}
        tokens = self.tokens_gatherer.get_tokens(contract_addresses)
        for token_addr, raw_token in tokens.items():
            tokens[token_addr] = raw_token

        token_txs = []
        token_txs = self.tokens_gatherer.get_token_transfers(logs)
        for token_tx in token_txs:
            token_tx['timestamp'] = transactions[token_tx['transaction_hash']]['timestamp']

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
                addresses[address] = {'inputTransactionHashes': [],
                                      'outputTransactionHashes': [],
                                      'code': '0x',
                                      'mined': [],
                                      'inputTokenTransactions': [],
                                      'outputTokenTransactions': []}

        return addresses

    def fill_addresses(self, addresses: Dict, transactions: Dict,
                       tokens: Dict, token_txs: List) -> Dict:
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
        addresses = self.init_fill_addrs_token_data(addresses, token_txs)
        addresses_encode = {}
        it = 0
        items = []
        for addr_hash, addr_dict in addresses.items():
            progress = int(it/len(addresses)*100)
            if progress % 20 == 0 and progress not in items:
                LOG.info('Address filling progress: {}%'.format(progress))
                items.append(progress)
            it += 1
            existing_data = self.db.get(b'address-' + addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_tx_str = existing_address['inputTransactions']
                output_tx_str = existing_address['outputTransactions']
                mined_str = existing_address['mined']
            else:
                input_tx_str = ''
                output_tx_str = ''
                mined_str = ''

            address_encode = {}
            if addr_hash in tokens:
                if tokens[addr_hash]['type'] == 'ERC-20':
                    address_encode['tokenContract'] = 'ERC-20'
                elif tokens[addr_hash]['type'] == 'ERC-721':
                    address_encode['tokenContract'] = 'ERC-721'
            else:
                address_encode['tokenContract'] = 'False'

            address_encode['balance'] = 'null'
            address_encode['code'] = addr_dict['code']
            for input_tx in addr_dict['inputTransactionHashes']:
                input_tx_str += ('|' + str(input_tx)
                                 + '+' + str(transactions[input_tx]['timestamp'])
                                 + '+' + str(transactions[input_tx]['value']))
            if input_tx_str != '' and input_tx_str[0] == '|':
                input_tx_str = input_tx_str[1:]
            address_encode['inputTransactions'] = input_tx_str
            for output_tx in addr_dict['outputTransactionHashes']:
                output_tx_str += ('|' + str(output_tx)
                                  + '+' + str(transactions[output_tx]['timestamp'])
                                  + '+' + str(transactions[input_tx]['value']))
            if output_tx_str != '' and output_tx_str[0] == '|':
                output_tx_str = output_tx_str[1:]
            address_encode['outputTransactions'] = output_tx_str
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
                                               'outputTokenTransactions': [token_tx]}
            elif token_tx['from'] is not None:
                addresses[token_tx['from']]['outputTokenTransactions'].append(token_tx)

            if token_tx['to'] not in addresses and token_tx['to'] is not None:
                addresses[token_tx['to']] = {'inputTransactionHashes': [],
                                             'outputTransactionHashes': [],
                                             'code': '0x',
                                             'mined': [],
                                             'inputTokenTransactions': [token_tx],
                                             'outputTokenTransactions': []}
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
                data = self.db.get(b'token-' + token_tx['token_address'].encode())
                if data is not None:
                    updated_tokens[token_tx['token_address']] = coder.decode_token(data)

        return updated_tokens

    def fill_addrs_token_txs(self, addresses: Dict, addresses_encode: Dict, tokens: Dict) -> Dict:
        """
        Fills address information with token transactions.

        Args:
            addresses: Currently processed addresses.
            addresses_encode: Addresses partially prepared for DB write.
            tokens: All relevant tokens.

        Returns:
            Updated addresses.
        """
        for addr_hash, addr_dict in addresses.items():
            existing_data = self.db.get(b'address-' + addr_hash.encode())
            # Address not yet in records
            if existing_data is not None:
                existing_address = coder.decode_address(existing_data)
                input_token_txs_str = existing_address['inputTokenTransactions']
                output_token_txs_str = existing_address['outputTokenTransactions']
            else:
                input_token_txs_str = ''
                output_token_txs_str = ''

            for input_token_tx in addr_dict['inputTokenTransactions']:
                # If suitable token for the transaction wasn't found, it likely means that this
                # token doesn't FULLY adhere to ERC-20/ERC-721 standard and will not be included.
                if input_token_tx['token_address'] not in tokens:
                    continue

                input_token_txs_str += ('|' + str(input_token_tx['token_address'])
                                        + '+' + str(input_token_tx['from'])
                                        + '+' + str(input_token_tx['value'])
                                        + '+' + str(input_token_tx['transaction_hash'])
                                        + '+' + str(input_token_tx['timestamp']))

            if input_token_txs_str != '' and input_token_txs_str[0] == '|':
                input_token_txs_str = input_token_txs_str[1:]
            addresses_encode[addr_hash]['inputTokenTransactions'] = input_token_txs_str

            for output_token_tx in addr_dict['outputTokenTransactions']:
                if output_token_tx['token_address'] not in tokens:
                    continue
                output_token_txs_str += ('|' + str(output_token_tx['token_address'])
                                         + '+' + str(output_token_tx['to'])
                                         + '+' + str(output_token_tx['value'])
                                         + '+' + str(output_token_tx['transaction_hash'])
                                         + '+' + str(output_token_tx['timestamp']))

            if output_token_txs_str != '' and output_token_txs_str[0] == '|':
                output_token_txs_str = output_token_txs_str[1:]
            addresses_encode[addr_hash]['outputTokenTransactions'] = output_token_txs_str

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
        LOG.info('Writing to database.')
        print("b: {}, t: {}, a: {}, to: {}".format(len(blocks), len(transactions), len(addresses), len(tokens)))
        wb = rocksdb.WriteBatch()
        for block_hash, block_dict in blocks.items():
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
        # TODO: for testing purposes
        fell_behind = False
        # If during sync the updater didn't fall too far behind, consider sync finished
        if not fell_behind:
            break
        LOG.info('Fell behind the blockchain during address processing, starting another sync.')
