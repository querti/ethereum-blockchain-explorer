"""Gather unique trace addresses using asynchronous API."""

import json
from typing import List, Any, Dict, Set

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri

class TraceAddressGatherer:
    """Gather addresses from traces using asynchronous API."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              batch=True))
        
    def _generate_web3_requests(self, block_start: int, block_end: int) -> List[Any]:
        """
        Prepare all debug_traceBlockByNumber calls.

        Args:
            block_start: Start of the block range.
            block_end: End block of the block range.
        
        Returns:
            A list of JSON RPC requests.
        """
        trace_requests = []
        for i in range(block_start, block_end +1):
            request = {'jsonrpc': '2.0',
                       'method': 'debug_traceBlockByNumber',
                       'params': [hex(i), {'tracer': 'callTracer'}],
                       'id': i}
            trace_requests.append(request)
        
        return trace_requests
    
    def _gather_addresses(self, block_start: int, block_end: int) -> Set
        """
        Gathers addresses that occured in traces of block range transactions.

        Args:
            block_start: Start of the block range.
            block_end: End block of the block range.
        
        Returns:
            Set of addresses.
        """
        requests = self._generate_web3_requests(addresses, height)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        # TODO finish this
        addresses = set()
        for block in response:
            result = block['result']
            if item.get('result') is None or item['result'] == []:
                continue
            for item in 
            addresses.add(item['result']['to'])
        
        return addresses