"""Class for filling addresses with current balance information."""
from typing import Any, Dict
import subprocess
import logging
import os

import rocksdb

from src.request.balances import BalanceGatherer
import src.coder as coder
from src.decorator import db_get_wrapper

LOG = logging.getLogger()


class BalanceUpdater:
    """Class for filling addresses with current balance information."""

    def __init__(self, bulk_size: int, datapath: str,
                 interface: str, db: Any, db_lock: Any) -> None:
        """
        Initialization.

        Args:
            bulk_size: How many blocks to be included in bulk DB update.
            datapath: Path for temporary file created in DB creation.
            interface: Path to the Geth blockchain node.
            db: Database instance.
            db_lock: Mutex that prevents simultanious DB write and read (to prevent read errors).
        """
        self._bulk_size = bulk_size
        self.datapath = datapath
        self._interface = interface
        self.db = db
        self.db_lock = db_lock
        # self.address_db = db.prefixed_db(b'address-')

    def _save_addresses(self, addresses: Dict, sort: bool) -> None:
        """
        Add new addresses to a file, while removing duplicates.

        Args:
            addresses: Addresses gathered in this batch.
            sort: Whether to sort and filter uniue addresses.
        """
        if addresses != {}:
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
        will need to trigger a new batch update.

        Args:
            blockchain_height: Height at which sync was completed.
        """
        balance_gatherer = BalanceGatherer(self._interface)
        continue_iteration = True

        if not os.path.exists(self.datapath + 'addresses.txt'):
            return

        addr_count = 0
        with open(self.datapath + 'addresses.txt') as f:
            for i, l in enumerate(f):
                addr_count += 1

        it = 0
        with open(self.datapath + 'addresses.txt') as f:
            while continue_iteration:
                progress = (it / (addr_count / self._bulk_size)) * 100
                if progress > 100.00:
                    progress = 100.00
                LOG.info('Updating balances: {0:.2f}%'.format(progress))
                addresses = []
                for i in range(self._bulk_size):
                    line = f.readline()
                    if it > (addr_count / self._bulk_size):
                        continue_iteration = False
                        break
                    if line != '' and line != '\n':
                        addresses.append(line[:-1])
                it += 1
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
            raw_addr = db_get_wrapper(self.db, b'address-' + str(address).encode())
            if raw_addr is None:
                continue
            address_objects[address] = coder.decode_address(raw_addr)
            address_objects[address]['balance'] = addr_balances[address]

        self.db_lock.acquire()
        wb = rocksdb.WriteBatch()
        for address in address_objects:
            address_value = coder.encode_address(address_objects[address])
            wb.put(b'address-' + str(address).encode(), address_value)

        self.db.write(wb)
        self.db_lock.release()
