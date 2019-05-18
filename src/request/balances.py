"""Gather address balances using asynchronous API."""

import json
from typing import List, Any, Dict

from src.request.thread_local_proxy import ThreadLocalProxy
from src.request.auto import get_provider_from_uri


class BalanceGatherer:
    """Gather address balances using asynchronous API."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              timeout=18000,
                                                                              batch=True))

    def _generate_web3_requests(self, addresses: List[str], height: int) -> List[Any]:
        """
        Prepare all eth_getBalance calls.

        Args:
            addresses: List of addresses whose balance is to be gathered.
            height: Block index at which the balance is gathered.

        Returns:
            A list of JSON RPC requests.
        """
        balance_requests = []
        hex_height = hex(height)
        hex_height = 'latest'

        for address in addresses:
            request = {'jsonrpc': '2.0',
                       'method': 'eth_getBalance',
                       'params': [address, hex_height],
                       'id': address}
            balance_requests.append(request)

        return balance_requests

    def _gather_balances(self, addresses: List[str], height: int) -> Dict[Any, str]:
        """
        Gathers balances of specified addresses.

        Args:
            addresses: List of addresses whose balance is to be gathered.
            height: Block index at which the balance is gathered.

        Returns:
            Dictionary containing addresses and their balance.
        """
        requests = self._generate_web3_requests(addresses, height)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        addr_dict = {}
        for item in response:
            if isinstance(item, str) or item.get('result') is None:
                continue
            addr_dict[item.get('id')] = str(int(item.get('result'), 16))

        return addr_dict
