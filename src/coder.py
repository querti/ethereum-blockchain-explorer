"""Set of functions to help encode and decode database entries."""
from typing import Dict


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
    tx_str += transaction['from'] + '\0'
    tx_str += transaction['to'] + '\0'
    tx_str += transaction['gas'] + '\0'
    tx_str += transaction['gasPrice'] + '\0'
    tx_str += transaction['transactionHash'] + '\0'
    tx_str += transaction['input'] + '\0'
    tx_str += transaction['nonce'] + '\0'
    tx_str += transaction['transactionBlockIndex'] + '\0'
    tx_str += transaction['value'] + '\0'
    tx_str += transaction['cumulativeGasUsed'] + '\0'
    tx_str += transaction['gasUsed'] + '\0'
    tx_str += transaction['logsBloom'] + '\0'
    tx_str += transaction['status'] + '\0'
    if transaction['contract'] is None:
        tx_str += '0\0'
    else:
        tx_str += '1\0'
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
    transaction['transactionHash'] = tx_items[6]
    transaction['input'] = tx_items[7]
    transaction['nonce'] = tx_items[8]
    transaction['transactionBlockIndex'] = tx_items[9]
    transaction['value'] = tx_items[10]
    transaction['cumulativeGasUsed'] = tx_items[11]
    transaction['gasUsed'] = tx_items[12]
    transaction['logsBloom'] = tx_items[13]
    transaction['status'] = tx_items[14]
    transaction['contract'] = tx_items[15]
    transaction['timestamp'] = tx_items[16]

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
    block_str += block['transactionIndexRange'] + '\0'
    block_str += block['sha3Uncles'] + '\0'
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
    block['transactionIndexRange'] = block_items[12]
    block['sha3Uncles'] = block_items[13]
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
    address_str += address['inputTransactionIndexes'] + '\0'
    address_str += address['outputTransactionIndexes'] + '\0'

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
    address['inputTransactionIndexes'] = address_items[2]
    address['outputTransactionIndexes'] = address_items[3]

    return address
