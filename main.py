#!/usr/bin/env python3
"""Ethereum blockchain explorer."""

from time import sleep
from multiprocessing import Process, Lock
import argparse

import connexion
import plyvel
from typing import Any

from src.database_updater import update_database
from src.blockchain_wrapper import BlockchainWrapper

# TODO: Add support for history changing??? (stale fork and such)


def blockchain_daemon(db_location: str, db_lock: Any, blockchain: Any, refresh: int) -> None:
    """
    Updates the leveldb database while REST API is already running.

    Args:
        db_location: Path to the leveldb database.
        db_lock: Instance of the database lock (to prevent multiple access).
        blockchain: Instance of the blockchain wrapper.
        refresh: How many seconds to sleep until database is refreshed.
    """
    while True:
        sleep(refresh)
        update_database(db_location, db_lock)


def add_args(parser: Any) -> None:
    """
    Adds arguments to the parser.

    Args:
        parser: Argument parser.
    """
    parser.add_argument('--interface', default='',
                        help='Geth API interface address.')
    parser.add_argument('--dbpath', default='db/',
                        help='Path where the database will be saved.')
    parser.add_argument('--confirmations', type=int, default=12,
                        help='Minimum number of comfirmations until block can included.')
    parser.add_argument('--refresh', type=int, default=20,
                        help='How many seconds to wait until the next database refresh.')


def main():
    """Main function."""
    print('You have started the blockchain explorer.')

    parser = argparse.ArgumentParser()
    add_args(parser)
    args = parser.parse_args()

    db = plyvel.DB(args.dbpath, create_if_missing=True)
    db.put(b'jeden', b'dva;tri;styri')
    db.close()
    db_lock = Lock()
    blockchain = BlockchainWrapper(args.interface, args.comfirmations)
    blockchain_daemon_p = Process(target=blockchain_daemon, args=(args.dbpath,
                                                                  db_lock,
                                                                  blockchain,
                                                                  args.refresh))
    blockchain_daemon_p.start()
    app = connexion.App(__name__, specification_dir='cfg/')
    app.app.config['DB_LOCATION'] = args.dbpath
    app.app.config['DB_LOCK'] = db_lock
    app.add_api('swagger.yaml')
    app.run()


if __name__ == "__main__":
    main()
