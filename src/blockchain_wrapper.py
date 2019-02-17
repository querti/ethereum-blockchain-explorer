"""Class used to gather information directly from Ethereum full node."""
from typing import Dict, List, Tuple, Union, Any
import logging

from web3 import Web3

LOG = logging.getLogger(__name__)


class BlockchainWrapper:
    """Class used to gather information directly from Ethereum full node."""

    def __init__(self, interface: str,
                 finality_threshold: int = 12) -> None:
        """
        Initialization.

        Args:
            interface: String representing RPC, WS, or IPC interface.
            finality_threshold: How many confirmations a block has to have.
        """
        self._finality_threshold = finality_threshold
        self.web3 = None
        self._interface = interface
        if self._interface == '' or 'ipc' in self._interface:
            self._web3 = Web3(Web3.IPCProvider(self._interface))
        elif 'http' in self._interface:
            self._web3 = Web3(Web3.HTTPProvider(self._interface))
        elif 'ws' in self._interface:
            self._web3 = Web3(Web3.WebsocketProvider(self._interface))
        else:
            self._web3 = Web3()

    def gather_block(self, block_index: int) -> Union[None, Tuple[Dict, List, Dict]]:
        """
        Gathers a full information about a block specified by its index.

        Args:
            block_index: Index of the desired block.

        Returns:
            Information about a block, its transactions, and affected addresses.
        """
        self._block_index = block_index
        blockchain_height = self._web3.eth.blockNumber
        if blockchain_height - self._finality_threshold < block_index:
            LOG.info('Not enough confirmations to include the block.')
            return None

        block = self._web3.eth.getBlock(block_index, full_transactions=False)
        transaction_hashes = block['transactions']

        result = self.gather_full_transactions_addresses(transaction_hashes,
                                                         block['timestamp'])
        if result is None:
            return None

        full_transactions, addresses = result
        return (block, full_transactions, addresses)

    def gather_full_transactions_addresses(self,
                                           hashes: List[str],
                                           timestamp: str) -> Union[None, Tuple[List[Dict], Dict]]:
        """
        Gathers full transactions, receipts, and asociated addresses.

        Args:
            hashes: List of transaction hashes.
            timestamp: Timestamp of the block.

        Returns:
            List of full transactions, and addresses with new information.
        """
        full_transactions = []
        addresses = {}  # type: Dict[str, Any]

        for transaction_hash in hashes:
            transaction = self._web3.getTransaction(transaction_hash)
            receipt = self._web3.getTransactionReceipt(transaction_hash)
            if receipt is None:
                LOG.warning('Receipt not yet generated')
                return None
            transaction['contractAddress'] = receipt['contractAddress']
            transaction['cumulativeGasUsed'] = receipt['cumulativeGasUsed']
            transaction['gasUsed'] = receipt['gasUsed']
            transaction['logs'] = receipt['logs']
            transaction['transactionHash'] = receipt['transactionHash']
            transaction['transactionIndex'] = receipt['transactionIndex']
            transaction['transactionBlockIndex'] = self._block_index
            transaction['timestamp'] = timestamp
            # WILL THIS WORK
            transaction['status'] = receipt['status']
            full_transactions.append(transaction)

            if transaction['from'] not in addresses:
                addresses['from'] = {'inputTransactionHashes': [transaction['transactionHash']],
                                     'outputTransactionHashes': [],
                                     'code': '0x'}
            else:
                addresses['from']['inputTransactionHashes'].append(transaction['transactionHash'])

            if transaction['to'] not in addresses:
                addresses['to'] = {'inputTransactionHashes': [],
                                   'outputTransactionHashes': [transaction['transactionHash']],
                                   'code': '0x'}
            else:
                addresses['from']['outputTransactionHashes'].append(transaction['transactionHash'])

            if transaction['contractAddress'] not in addresses:
                code = self._web3.eth.getCode(transaction['contractAddress'])
                addresses['contractAddress'] = {'inputTransactionHashes': [],
                                                'outputTransactionHashes': [],
                                                'code': code}

        addresses = self.gather_address_balances(addresses)
        return (full_transactions, addresses)

    def gather_address_balances(self, addresses: Dict) -> Dict:
        """
        Gets current balance for all addresses where some change occured.

        Args:
            Dictionary holding all addresses differences.

        Returns:
            Dictionary with added balances.
        """
        for address in addresses:
            balance = self._web3.eth.getBalance(address, self._block_index)
            addresses['address']['balance'] = balance

        return addresses
