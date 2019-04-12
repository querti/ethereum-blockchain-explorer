"""Class for filling addresses with current balance information."""
from typing import Any, Dict
import subprocess
import logging
import os

import rocksdb

from src.requests.balances import BalanceGatherer
import src.coder as coder

LOG = logging.getLogger()


class BalanceUpdater:
    """Class for filling addresses with current balance information."""

    def __init__(self, bulk_size: int, datapath: str, interface: str, db: Any) -> None:
        """
        Initialization.

        Args:
            bulk_size: How many blocks to be included in bulk DB update.
            datapath: Path for temporary file created in DB creation.
            interface: Path to the Geth blockchain node.
            db: Database instance.
        """
        self._bulk_size = bulk_size
        self.datapath = datapath
        self._interface = interface
        self.db = db
        # self.address_db = db.prefixed_db(b'address-')

    def _save_addresses(self, addresses: Dict, sort: bool) -> None:
        """
        Add new addresses to a file, while removing duplicates.

        Args:
            addresses: Addresses gathered in this batch.
            sort: Whether to sort and filter uniue addresses.
        """
        LOG.info('Saving addresses')
        addr_str = '\n' + '\n'.join(addresses.keys())
        with open(self.datapath + 'addresses.txt', 'a+') as f:
            f.write(addr_str)

        if sort:
            LOG.info('Removing duplicate addresses.')
            sort_cmd = 'sort -u {} -o {}'.format(self.datapath + 'addresses.txt',
                                                 self.datapath + 'addresses.txt')
            subprocess.call(sort_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _update_address_balances(self, blockchain_height: int) -> None:
        """
        Load relevant addresses from tmp file, get their balances from Node and save them to DB.

        During this update new data will be added to blockchain and thus the DB will be out of date
        by the time it is completed. Small out of date-ness is acceptable but a longer one
        will probabbly need to trigger a new batch update.

        Args:
            blockchain_height: Height at which sync was completed.
        """
        balance_gatherer = BalanceGatherer(self._interface)
        continue_iteration = True

        addr_count = 0
        with open(self.datapath + 'addresses.txt') as f:
            for i, l in enumerate(f):
                addr_count += 1

        with open(self.datapath + 'addresses.txt', 'r') as f:
            while continue_iteration:
                LOG.info('Updating balances: {0:.2f}%'.format((it/(addr_count/self._bulk_size))*100))
                it += 1
                addresses = []
                for i in range(self._bulk_size):
                    line = f.readline()
                    if line == '':
                        continue_iteration = False
                        break
                    addresses.append(line[:-1])
                balances = balance_gatherer._gather_balances(addresses, blockchain_height)
                self._update_db_balances(balances)

        if os.path.exists(self.datapath + 'addresses.txt'):
            os.remove(self.datapath + 'addresses.txt')

    def _update_db_balances(self, addr_balances: Dict) -> None:
        """
        Updates balances of Ethereum addresses in the LevelDB database in batches.

        Args:
            addr_balances: Dictionary containing 'address: balance' entries.
        """
        address_objects = {}
        for address in addr_balances:
            raw_addr = self.db.get(b'address-' + str(address).encode())
            address_objects[address] = coder.decode_address(raw_addr)
            address_objects[address]['balance'] = addr_balances[address]

        #wb = rocksdb.WriteBatch()
        for address in address_objects:
            address_value = coder.encode_address(address_objects[address])
            self.db.put(b'address-' + str(address).encode(), address_value)

        #self.db.write(wb)
