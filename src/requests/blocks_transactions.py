"""Gather blocks and transactions using asynchronous API."""

import json
from typing import List, Any, Dict, Tuple

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri


class BlocksTransactionsGatherer:
    """Gather blocks and transactions using asynchronous API."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              batch=True))

    def _generate_web3_requests(self, start_block: int, end_block: int) -> List[Any]:
        """
        Prepare all block gather calls.

        Args:
            start_block: First block from the batch to be gathered.
            end_block: Last block from the batch to be gathered.

        Returns:
            A list of JSON RPC requests.
        """
        block_requests = []

        for i in range(start_block, end_block+1):
            request = {'jsonrpc': '2.0',
                       'method': 'eth_getBlockByNumber',
                       'params': [hex(i), True],
                       'id': i}
            block_requests.append(request)

        return block_requests

    def gather_blocks_and_transactions(self, start_block: int, end_block: int) -> Tuple[Dict[Any, str], Dict[Any, str]]:
        """
        Gathers blocks and transactions.

        Args:
            start_block: First block from the batch to be gathered.
            end_block: Last block from the batch to be gathered.

        Returns:
            Dictionaries containing blocks and their transactions.
        """
        requests = self._generate_web3_requests(start_block, end_block)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        blocks = {}
        transactions = {}

        for response in response:

            block = response.get('result')
            block_txs = []
            blocks[block['hash']] = block

            for transaction in block['transactions']:
                transactions[transaction['hash']] = transaction
                block_txs.append(transaction['hash'])
            
            blocks[block['hash']]['transactions'] = "+".join(block_txs)

        return (blocks, transactions)
