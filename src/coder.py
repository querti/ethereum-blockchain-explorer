"""Coder for preparing data for DB writes and parsing data from DB reads."""
from typing import Dict, Union


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
    tx_str += transaction['value'] + '\0'
    tx_str += transaction['cumulativeGasUsed'] + '\0'
    tx_str += transaction['gasUsed'] + '\0'
    tx_str += transaction['logs'] + '\0'
    tx_str += transaction.get('contractAddress', '') + '\0'
    tx_str += transaction['timestamp'] + '\0'
    tx_str += str(transaction['internalTxIndex']) + '\0'

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
    transaction['value'] = tx_items[8]
    transaction['cumulativeGasUsed'] = tx_items[9]
    transaction['gasUsed'] = tx_items[10]
    transaction['logs'] = tx_items[11]
    transaction['contractAddress'] = tx_items[12]
    transaction['timestamp'] = tx_items[13]
    transaction['internalTxIndex'] = int(tx_items[14])  # type: ignore

    logs = []
    if (transaction['logs'] != '' and transaction['logs'][-1] == '|'):
        transaction['logs'] = transaction['logs'][:-1]

    for log in transaction['logs'].split('|'):
        fields = log.split('+')
        full_log = {}
        full_log['data'] = fields[0]
        if len(fields) == 2:
            topics = fields[1].split('-')
            full_log['topics'] = topics  # type: ignore
        else:
            full_log['topics'] = []  # type: ignore
        logs.append(full_log)

    transaction['logs'] = logs  # type: ignore

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
    address_str += str(address['inputTxIndex']) + '\0'
    address_str += str(address['outputTxIndex']) + '\0'
    address_str += str(address['minedIndex']) + '\0'
    address_str += address['tokenContract'] + '\0'
    address_str += str(address['inputTokenTxIndex']) + '\0'
    address_str += str(address['outputTokenTxIndex']) + '\0'
    address_str += str(address['inputIntTxIndex']) + '\0'
    address_str += str(address['outputIntTxIndex']) + '\0'

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
    address = {}  # type: Dict[str, Union[str, int]]

    address['balance'] = address_items[0]
    address['code'] = address_items[1]
    address['inputTxIndex'] = int(address_items[2])
    address['outputTxIndex'] = int(address_items[3])
    address['minedIndex'] = int(address_items[4])
    address['tokenContract'] = address_items[5]
    address['inputTokenTxIndex'] = int(address_items[6])
    address['outputTokenTxIndex'] = int(address_items[7])
    address['inputIntTxIndex'] = int(address_items[8])
    address['outputIntTxIndex'] = int(address_items[9])

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
    token_str += token['totalSupply'] + '\0'
    token_str += token['type'] + '\0'
    token_str += str(token['txIndex']) + '\0'

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
    token = {}  # type: Dict[str, Union[str, int]]

    token['symbol'] = token_items[0]
    token['name'] = token_items[1]
    token['decimals'] = token_items[2]
    token['totalSupply'] = token_items[3]
    token['type'] = token_items[4]
    token['txIndex'] = int(token_items[5])

    return token


def encode_token_tx(token_tx: Dict) -> bytes:
    """
    Creates bytes representation of token transaction data.

    args:
        token_tx: Dictionary containing the token transaction data.

    returns:
        Bytes to be saved as token value in DB.
    """
    token_tx_str = ''
    token_tx_str += token_tx['tokenAddress'] + '\0'
    token_tx_str += token_tx['addressFrom'] + '\0'
    token_tx_str += token_tx['addressTo'] + '\0'
    token_tx_str += token_tx['value'] + '\0'
    token_tx_str += token_tx['transactionHash'] + '\0'
    token_tx_str += token_tx['timestamp'] + '\0'

    return token_tx_str.encode()


def decode_token_tx(raw_token_tx: bytes) -> Dict:
    """
    Decodes bytes representation of an token transaction into a dictionary.

    Args:
        raw_token_tx: Bytes representing a token transaction.

    Returns:
        Token transaction in dictionary form.
    """
    token_tx_items = raw_token_tx.decode().split('\0')
    token_tx = {}

    token_tx['tokenAddress'] = token_tx_items[0]
    token_tx['addressFrom'] = token_tx_items[1]
    token_tx['addressTo'] = token_tx_items[2]
    token_tx['value'] = token_tx_items[3]
    token_tx['transactionHash'] = token_tx_items[4]
    token_tx['timestamp'] = token_tx_items[5]

    return token_tx


def encode_internal_tx(internal_tx: Dict) -> bytes:
    """
    Creates bytes representation of internal transaction data.

    args:
        internal_tx: Dictionary containing the internal transaction data.

    returns:
        Bytes to be saved as internal value in DB.
    """
    internal_tx_str = ''
    internal_tx_str += internal_tx['from'] + '\0'
    internal_tx_str += internal_tx['to'] + '\0'
    internal_tx_str += internal_tx['value'] + '\0'
    internal_tx_str += internal_tx['input'] + '\0'
    internal_tx_str += internal_tx['output'] + '\0'
    internal_tx_str += internal_tx['traceType'] + '\0'
    internal_tx_str += internal_tx['callType'] + '\0'
    internal_tx_str += internal_tx['rewardType'] + '\0'
    internal_tx_str += internal_tx['gas'] + '\0'
    internal_tx_str += internal_tx['gasUsed'] + '\0'
    internal_tx_str += internal_tx['transactionHash'] + '\0'
    internal_tx_str += internal_tx['timestamp'] + '\0'
    internal_tx_str += internal_tx['error'] + '\0'

    return internal_tx_str.encode()


def decode_internal_tx(raw_internal_tx: bytes) -> Dict:
    """
    Decodes bytes representation of an internal transaction into a dictionary.

    Args:
        raw_internal_tx: Bytes representing an internal transaction.

    Returns:
        Internal transaction in dictionary form.
    """
    internal_tx_items = raw_internal_tx.decode().split('\0')
    internal_tx = {}

    internal_tx['from'] = internal_tx_items[0]
    internal_tx['to'] = internal_tx_items[1]
    internal_tx['value'] = internal_tx_items[2]
    internal_tx['input'] = internal_tx_items[3]
    internal_tx['output'] = internal_tx_items[4]
    internal_tx['traceType'] = internal_tx_items[5]
    internal_tx['callType'] = internal_tx_items[6]
    internal_tx['rewardType'] = internal_tx_items[7]
    internal_tx['gas'] = internal_tx_items[8]
    internal_tx['gasUsed'] = internal_tx_items[9]
    internal_tx['transactionHash'] = internal_tx_items[10]
    internal_tx['timestamp'] = internal_tx_items[11]
    internal_tx['error'] = internal_tx_items[12]

    return internal_tx
