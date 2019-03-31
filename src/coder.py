"""Coder for preparing data for DB writes and parsing data from DB reads."""
from typing import Dict, List


def encode_transaction(transaction: Dict) -> bytes:
    """
    Creates bytes representation of transaction data.

    Args:
        transaction: Dictionary representing a transaction.

    Returns:
        Bytes to be saved as transaction value in DB.
    """
    tx_str = ''
    tx_str += transaction['blockHash'] + '\0'
    tx_str += transaction['blockNumber'] + '\0'
    if transaction['from'] is None:
        tx_str += '/0'
    else:
        tx_str += transaction['from'] + '\0'
    if transaction['to'] is None:
        tx_str += '/0'
    else:
        tx_str += transaction['to'] + '\0'
    tx_str += transaction['gas'] + '\0'
    tx_str += transaction['gasPrice'] + '\0'
    # tx_str += transaction['hash'] + '\0'
    tx_str += transaction['input'] + '\0'
    tx_str += transaction['nonce'] + '\0'
    tx_str += transaction['blockNumber'] + '\0'
    tx_str += transaction['value'] + '\0'
    tx_str += transaction['cumulativeGasUsed'] + '\0'
    tx_str += transaction['gasUsed'] + '\0'
    tx_str += transaction['logs'] + '\0'
    tx_str += transaction.get('contractAddress', '') + '\0'
    tx_str += transaction['timestamp']

    return tx_str.encode()


def decode_transaction(raw_transaction: bytes) -> Dict:
    """
    Decodes bytes representation of a transaction into transaction dictionary.

    Args:
        raw_transaction: Bytes representing a transaction.

    Returns:
        Transaction in dictionary form.
    """
    tx_items = raw_transaction.decode().split('\0')
    transaction = {}

    transaction['blockHash'] = tx_items[0]
    transaction['blockNumber'] = tx_items[1]
    transaction['from'] = tx_items[2]
    transaction['to'] = tx_items[3]
    transaction['gas'] = tx_items[4]
    transaction['gasPrice'] = tx_items[5]
    # transaction['transactionHash'] = tx_items[6]
    transaction['input'] = tx_items[6]
    transaction['nonce'] = tx_items[7]
    transaction['transactionBlockIndex'] = tx_items[8]
    transaction['value'] = tx_items[9]
    transaction['cumulativeGasUsed'] = tx_items[10]
    transaction['gasUsed'] = tx_items[11]
    transaction['logs'] = tx_items[12]
    transaction['contract_address'] = tx_items[13]
    transaction['timestamp'] = tx_items[14]

    return transaction


def encode_block(block: Dict) -> bytes:
    """
    Creates bytes representation of block data.

    args:
        block: Dictionary containing the block data.

    returns:
        Bytes to be saved as block value in DB.
    """
    block_str = ''
    block_str += block['hash'] + '\0'
    block_str += block['parentHash'] + '\0'
    block_str += block['nonce'] + '\0'
    block_str += block['logsBloom'] + '\0'
    block_str += block['miner'] + '\0'
    block_str += block['difficulty'] + '\0'
    block_str += block['totalDifficulty'] + '\0'
    block_str += block['extraData'] + '\0'
    block_str += block['size'] + '\0'
    block_str += block['gasLimit'] + '\0'
    block_str += block['gasUsed'] + '\0'
    block_str += block['timestamp'] + '\0'
    # block_str += block['transactionIndexRange'] + '\0'
    block_str += block['sha3Uncles'] + '\0'
    block_str += block['transactions']
    # REWARD?????
    # block_str += block['hasREWARD???h'] + '\0'

    return block_str.encode()


def decode_block(raw_block: bytes) -> Dict:
    """
    Decodes bytes representation of a block into block dictionary.

    Args:
        raw_block: Bytes representing a block.

    Returns:
        Block in dictionary form.
    """
    block_items = raw_block.decode().split('\0')
    block = {}

    block['hash'] = block_items[0]
    block['parentHash'] = block_items[1]
    block['nonce'] = block_items[2]
    block['logsBloom'] = block_items[3]
    block['miner'] = block_items[4]
    block['difficulty'] = block_items[5]
    block['totalDifficulty'] = block_items[6]
    block['extraData'] = block_items[7]
    block['size'] = block_items[8]
    block['gasLimit'] = block_items[9]
    block['gasUsed'] = block_items[10]
    block['timestamp'] = block_items[11]
    # block['transactionIndexRange'] = block_items[12]
    block['sha3Uncles'] = block_items[12]
    block['transactions'] = block_items[13]
    # REWARD????
    # block['hasREWARD???h'] + '\0'

    return block


def encode_address(address: Dict) -> bytes:
    """
    Creates bytes representation of address data.

    args:
        address: Dictionary containing the address data.

    returns:
        Bytes to be saved as address value in DB.
    """
    address_str = ''
    address_str += address['balance'] + '\0'
    address_str += address['code'] + '\0'
    address_str += address['inputTransactions'] + '\0'
    address_str += address['outputTransactions'] + '\0'
    address_str += address['mined'] + '\0'
    address_str += address['tokenContract'] + '\0'
    address_str += address['inputTokenTransactions'] + '\0'
    address_str += address['outputTokenTransactions'] + '\0'

    return address_str.encode()


def decode_address(raw_address: bytes) -> Dict:
    """
    Decodes bytes representation of an address into address dictionary.

    Args:
        raw_address: Bytes representing an address.

    Returns:
        Address in dictionary form.
    """
    address_items = raw_address.decode().split('\0')
    address = {}

    address['balance'] = address_items[0]
    address['code'] = address_items[1]
    address['inputTransactions'] = address_items[2]
    address['outputTransactions'] = address_items[3]
    address['mined'] = address_items[4]
    address['tokenContract'] = address_items[5]
    address['inputTokenTransactions'] = address_items[6]
    address['outputTokenTransactions'] = address_items[7]

    return address


def encode_token(token: Dict) -> bytes:
    """
    Creates bytes representation of token data.

    args:
        token: Dictionary containing the token data.

    returns:
        Bytes to be saved as token value in DB.
    """
    token_str = ''
    token_str += token['symbol'] + '\0'
    token_str += token['name'] + '\0'
    token_str += token['decimals'] + '\0'
    token_str += token['total_supply'] + '\0'
    token_str += token['type'] + '\0'

    return token_str.encode()


def decode_token(raw_token: bytes) -> Dict:
    """
    Decodes bytes representation of an token into token dictionary.

    Args:
        raw_token: Bytes representing a token.

    Returns:
        Token in dictionary form.
    """
    token_items = raw_token.decode().split('\0')
    token = {}

    token['symbol'] = token_items[0]
    token['name'] = token_items[1]
    token['decimals'] = token_items[2]
    token['total_supply'] = token_items[3]
    token['type'] = token_items[4]

    return token


def encode_erc20_balances(erc20_balances: Dict) -> str:
    """
    Encodes dictionary containing ERC-20 balances into a string.

    Args:
        erc20_balances: Dictionary of balances.

    Returns:
        String representing address balances.
    """
    erc20_balances_str = ''
    if not erc20_balances:
        return ''
    for addr, balance in erc20_balances.items():
        erc20_balances_str += '|' + addr + '+' + str(balance)
    return erc20_balances_str[1:]


def decode_erc20_balances(erc20_balances_str: str) -> Dict:
    """
    Decodes string of ERC-20 balances of an address into a dictionary.

    Args:
        erc20_balances_str: String representing current erc-20 balances.

    Returns:
        Dictionary containing all balances.
    """
    balances = {}
    if erc20_balances_str == '':
        return {}
    for token in erc20_balances_str.split('|'):
        token_address, balance = token.split('+')
        balances['token_address'] = int(balance)

    return balances


def encode_erc721_records(erc721_records: Dict[str, List]) -> str:
    """
    Encodes dictionary containing ERC-721 records into a string.

    Args:
        erc721_records: Dictionary of owned items.

    Returns:
        String representing address's owned items.
    """
    erc721_records_str = ''
    if not erc721_records:
        return ''
    for addr, items in erc721_records.items():
        erc721_records_str += '|' + addr
        for item in items:
            erc721_records_str += '+' + item

    return erc721_records_str[1:]


def decode_erc721_records(erc721_records_str: str) -> Dict[str, List]:
    """
    Decodes string of ERC-721 items of an address into a dictionary.

    Args:
        erc721_records_str: String representing current erc-721 items.

    Returns:
        Dictionary containing all address's items.
    """
    items = {}
    if erc721_records_str == '':
        return {}
    for token in erc721_records_str.split('|'):
        records = token.split('+')
        items[records[0]] = records[1:]

    return items
