"""Rest API validity tester."""

import unittest
import json
import rocksdb

from src.database_gatherer import DatabaseGatherer

DB_PATH = '/home/lgallovi/Documents/database'


class ValidityTester(unittest.TestCase):
    """
    Test validity of data provided by the database.

    Requires a database synced with TESTNET data.Fields are compared to those provided
    by https://ropsten.etherscan.io.
    """

    def test_get_block(self):
        """Test validity of block data."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/block_data.txt', 'r') as f:
            compare_data = json.loads(f.read())

        block_hash = '0x31a2bdcaed45ff75bd2d4803731b4200719f6fe1c07e3661f33e3a9a2c996a6e'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_block_by_hash(block_hash)

        self.assertEqual(compare_data, gathered_data)

    def test_get_block_index(self):
        """Test validity of getting block index."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)

        block_index = '25308'
        block_hash = '0x31a2bdcaed45ff75bd2d4803731b4200719f6fe1c07e3661f33e3a9a2c996a6e'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_block_hash_by_index(block_index)

        self.assertEqual(block_hash, gathered_data)

    def test_get_blocks_timerange(self):
        """Test validity of block data from a selected time range."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/block_data_timerange.txt', 'r') as f:
            compare_data = json.loads(f.read())

        time_start = 1479653257
        time_end = 1479653542
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_blocks_by_datetime(100000, time_start, time_end)

        self.assertEqual(compare_data, gathered_data)

    def test_get_blocks_indexes(self):
        """Test validity of block data from a selected index range."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/block_data_indexrange.txt', 'r') as f:
            compare_data = json.loads(f.read())

        index_start = 20
        index_end = 30
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_blocks_by_indexes(index_start, index_end)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transaction(self):
        """Test validity of transaction data."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/tx_data.txt', 'r') as f:
            compare_data = json.loads(f.read())

        tx_hash = '0x57f281bf8792353cdab545fe439410f0f6478e272e1dcc5748d87299d32373e7'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transaction_by_hash(tx_hash)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_block_by_hash(self):
        """Test validity of transactions of a block selected by hash."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/block_hash_transactions.txt', 'r') as f:
            compare_data = json.loads(f.read())

        block_hash = '0x57f8464fa5d5f8fc4f4926963e2df38afc4f4c378e933f46c7b3f727bc0c5dcb'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_block_by_hash(block_hash)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_block_by_index(self):
        """Test validity of transactions of a block selected by index."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/block_index_transactions.txt', 'r') as f:
            compare_data = json.loads(f.read())

        block_index = '25829'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_block_by_index(block_index)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_address(self):
        """Test validity of transactions of an address."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/address_transactions.txt', 'r') as f:
            compare_data = json.loads(f.read())

        address = '0x004b7f28a01a9f9142b2fc818b22325c4c049166'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_address(address, 0, 99999999999,
                                                             0, 9999999999999999999999999999,
                                                             99999999999999999999999999)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_address_filter_time(self):
        """Test validity of transactions of an address. Filter by time."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/address_transactions_filter_time.txt', 'r') as f:
            compare_data = json.loads(f.read())

        address = '0x004b7f28a01a9f9142b2fc818b22325c4c049166'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_address(address, 1481121885, 1481124969,
                                                             0, 9999999999999999999999999999,
                                                             99999999999999999999999999)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_address_filter_value(self):
        """Test validity of transactions of an address. Filter by value."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/address_transactions_filter_value.txt', 'r') as f:
            compare_data = json.loads(f.read())

        address = '0x004b7f28a01a9f9142b2fc818b22325c4c049166'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_address(address, 0, 99999999999,
                                                             10000000, 10000000000000000000,
                                                             99999999999999999999999999)

        self.assertEqual(compare_data, gathered_data)

    def test_get_transactions_of_address_limit(self):
        """Test validity of transactions of an address. Limit returned amount."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/address_transactions_filter_limit.txt', 'r') as f:
            compare_data = json.loads(f.read())

        address = '0x004b7f28a01a9f9142b2fc818b22325c4c049166'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_transactions_of_address(address, 0, 99999999999,
                                                             0, 9999999999999999999999999999, 5)

        self.assertEqual(compare_data, gathered_data)

    def test_get_token(self):
        """Test validity returning a token and its transactions."""
        db = rocksdb.DB(DB_PATH, rocksdb.Options(create_if_missing=True, max_open_files=10000),
                        read_only=True)
        with open('tests/resources/token_data.txt', 'r') as f:
            compare_data = json.loads(f.read())

        address = '0x9724a061eaff6e34f4127272bf84cab690f98339'
        gatherer = DatabaseGatherer(db)
        gathered_data = gatherer.get_token(address, 0, 999999999999, 9999999999999999999999999999)

        self.assertEqual(compare_data, gathered_data)
