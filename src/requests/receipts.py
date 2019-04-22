"""Gather transaction receipts using asynchronous API."""

import json
from typing import List, Any, Dict

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri


class ReceiptsGatherer:
    """Gather transaction receipts using asynchronous API."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              batch=True))

    def _generate_web3_requests(self, tx_hashes: List[str]) -> List[Any]:
        """
        Prepare all receipt gather calls.

        Args:
            tx_hashes: List of transaction hashes.

        Returns:
            A list of JSON RPC requests.
        """
        receipt_requests = []
        
        for tx_hash in tx_hashes:
            request = {'jsonrpc': '2.0',
                       'method': 'eth_getTransactionReceipt',
                       'params': [tx_hash],
                       'id': tx_hash}
            receipt_requests.append(request)

        return receipt_requests

    def gather_receipts(self, tx_hashes: List[str]) -> Dict[Any, str]:
        """
        Gathers transaction receipts.

        Args:
            tx_hashes: List of transaction hashes.

        Returns:
            Dictionaries containing transaction receipts.
        """
        requests = self._generate_web3_requests(addresses, height)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        receipts = {}

        for response in response:
            receipt = response.get('result')
            block_txs = []
            receipts[receipt['transactionHash']] = receipt

        return receipts
