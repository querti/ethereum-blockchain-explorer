"""Gather tokens and their transactions using asynchronous API."""

import json
from typing import List, Any, Dict

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri
from src.requests.eth_contract_service import EthContractService


class TokensGatherer:
    """Gather tokens and their transactions using asynchronous API."""

    def __init__(self, interface: str, contract_addresses: List[str]) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
            contract_addresses: List of contract addresses in this batch.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              batch=True))
        self._contract_addresses = contract_addresses
        self._contract_service = EthContractService()

    def _generate_web3_contract_requests(self) -> List[Any]:
        """
        Prepare all contract gather calls.


        Returns:
            A list of JSON RPC contract requests.
        """
        contract_requests = []
        
        for contract_address in self._contract_addresses:
            request = {'jsonrpc': '2.0',
                       'method': 'eth_getCode',
                       'params': [contract_address, 'latest'],
                       'id': contract_address}
            contract_requests.append(request)

        return contract_requests

    def gather_contracts(self) -> Dict[Any, str]:
        """
        Gathers address contracts.

        Returns:
            Dictionaries containing address contracts.
        """
        requests = self._generate_web3_contract_requests(addresses, height)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        contracts = {}
        erc_contracts = {}

        for response in response:
            contract = response.get('result')
            function_sighashes = self._contract_service.get_function_sighashes(contract)
            if (self._contract_service.is_erc20_contract(function_sighashes)
                or self._contract_service.is_erc721_contract(function_sighashes)):
                erc_contracts.append(response.get('id'))
            contracts[response.get('id')] = response.get('result')

        return (contracts, erc_contracts)
