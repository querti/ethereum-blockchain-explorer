"""Gather tokens and their transactions using asynchronous API."""

import json
from typing import List, Any, Dict

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput

from src.requests.thread_local_proxy import ThreadLocalProxy
from src.requests.auto import get_provider_from_uri
from src.requests.eth_contract_service import EthContractService
from src.requests.erc20_abi import ERC20_ABI

TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


class TokensGatherer:
    """Gather tokens and their transactions using asynchronous API."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: Ethereum blockchain interface address.
            contract_addresses: List of contract addresses in this batch.
            receipts: Gathered receipts.
        """
        self._interface = interface
        self._batch_gatherer = ThreadLocalProxy(lambda: get_provider_from_uri(self._interface,
                                                                              batch=True))
        self._web3 = ThreadLocalProxy(lambda: Web3(get_provider_from_uri(self._interface)))
        self._contract_service = EthContractService()
        self._function_call_result_transformer = clean_user_provided_content

    def _generate_web3_contract_requests(self, contract_addresses: List[str]) -> List[Any]:
        """
        Prepare all contract gather calls.


        Returns:
            A list of JSON RPC contract requests.
        """
        contract_requests = []
        
        for contract_address in contract_addresses:
            request = {'jsonrpc': '2.0',
                       'method': 'eth_getCode',
                       'params': [contract_address, 'latest'],
                       'id': contract_address}
            contract_requests.append(request)

        return contract_requests

    def gather_contracts(self, contract_addresses: List[str]) -> Dict[Any, str]:
        """
        Gathers address contracts.

        Returns:
            Dictionaries containing address contracts.
        """
        requests = self._generate_web3_contract_requests(contract_addresses)
        response = self._batch_gatherer.make_request(json.dumps(requests))

        contracts = {}
        erc_addresses = []
        addr_types = {}

        for response in response:
            contract = response.get('result')
            function_sighashes = self._contract_service.get_function_sighashes(contract)
            if self._contract_service.is_erc20_contract(function_sighashes):
                erc_addresses.append(response.get('id'))
                addr_types[response.get('id')] = 'ERC-20'
            if self._contract_service.is_erc721_contract(function_sighashes):
                erc_addresses.append(response.get('id'))
                addr_types[response.get('id')] = 'ERC-721'

            contracts[response.get('id')] = response.get('result')

        return (contracts, erc_addresses, addr_types)

    def get_tokens(self, contract_addresses: List[str]) -> Dict[str, Any]:
        """
        Get token information.

        Args:
            erc_addresses: List of contract addresses that are ERC-20 or ERC-721.
        
        Returns:
            Information about newly created tokens.
        """
        contracts, erc_addresses, addr_types = self.gather_contracts(contract_addresses)
        tokens = {}

        for erc_address in erc_addresses:
            checksum_address = self._web3.toChecksumAddress(erc_address)
            contract = self._web3.eth.contract(address=checksum_address, abi=ERC20_ABI)

            token = {}
            token['symbol'] = self._call_contract_function(contract.functions.symbol())
            if token['symbol'] is None:
                token['symbol'] = ''
            token['name'] = self._call_contract_function(contract.functions.name())
            if token['name'] is None:
                token['name'] = ''
            token['decimals'] = str(self._call_contract_function(contract.functions.decimals()))
            if token['decimals'] is None:
                token['decimals'] = ''
            token['total_supply'] = str(self._call_contract_function(contract.functions.totalSupply()))
            token['type'] = addr_types[erc_address]
        
            tokens[erc_address] = token
        
        return tokens
    
    def _call_contract_function(self, func):
        # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
        # or was self-destructed
        # OverflowError exception happens if the return type of the function doesn't match the expected type
        result = call_contract_function(
            func=func,
            ignore_errors=(BadFunctionCallOutput, OverflowError, ValueError),
            default_value=None)

        if self._function_call_result_transformer is not None:
            return self._function_call_result_transformer(result)
        else:
            return result

    def get_token_transfers(self, logs: List) -> List:
        """
        Get token transfers from logs.

        Returns:
            A list of token transfers.
        """
        token_transfers = []
        for log in logs:
            transfer = self.extract_transfer_from_log(log)
            if transfer is not None:
                token_transfers.append(transfer)

        return token_transfers

    def extract_transfer_from_log(self, receipt_log):

        topics = receipt_log['topics']
        if topics is None or len(topics) < 1:
            return None

        if topics[0] == TRANSFER_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log['data'])
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 4:
                logger.warning("The number of topics and data parts is not equal to 4 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            token_transfer = {}
            token_transfer['token_address'] = to_normalized_address(receipt_log['address'])
            token_transfer['from'] = word_to_address(topics_with_data[1])
            token_transfer['to'] = word_to_address(topics_with_data[2])
            token_transfer['value'] = hex_to_dec(topics_with_data[3])
            token_transfer['transaction_hash'] = receipt_log['transactionHash']
            return token_transfer

        return None


def call_contract_function(func, ignore_errors, default_value=None):
    try:
        result = func.call()
        return result
    except Exception as ex:
        if type(ex) in ignore_errors:
            return default_value
        else:
            raise ex


ASCII_0 = 0

def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content

def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: '0x' + word, words))
        return words_with_0x
    return []

def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)

def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()

def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        print("Not a hex string %s" % hex_string)
        return hex_string

def chunk_string(string, length):
    return (string[0 + i:length + i] for i in range(0, len(string), length))