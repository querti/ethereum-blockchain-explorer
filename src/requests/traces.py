"""Gather unique trace addresses using asynchronous API."""

import json
from typing import List, Any, Set

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri


class TraceAddressGatherer:
    """Gather addresses from traces using asynchronous API."""

    def __init__(self, interface: str, batch_size: int = 100) -> None:
        """
        Initialization.

        Batch size should generally be at a few hundred at most as it might give timeout otherwise.

        Args:
            interface: Ethereum blockchain interface address.
            batch_size: How big the batches should be.
        """
        self.batch_size = batch_size
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
        for i in range(block_start, block_end + 1):
            request = {'jsonrpc': '2.0',
                       'method': 'debug_traceBlockByNumber',
                       'params': [hex(i), {'tracer': 'callTracer'}],
                       'id': i}
            trace_requests.append(request)

        return trace_requests

    def _gather_addresses(self, block_start: int, block_end: int) -> Set:
        """
        Gathers addresses that occured in traces of block range transactions.

        Args:
            block_start: Start of the block range.
            block_end: End block of the block range.

        Returns:
            Set of addresses.
        """
        addresses = set()
        batches = (block_end - block_start) // self.batch_size + 1

        for i in range(batches):
            start = block_start + i * self.batch_size
            end = block_start + (i + 1) * self.batch_size
            if end > block_end:
                end = block_end
            requests = self._generate_web3_requests(start, end)
            response = self._batch_gatherer.make_request(json.dumps(requests))

            for block in response:
                if 'result' not in block:
                    continue
                data = block['result']
                for item in data:
                    addresses.add(item['result']['to'])

        return addresses
