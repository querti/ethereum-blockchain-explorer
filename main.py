#!/usr/bin/env python3
"""Ethereum blockchain explorer."""

from time import sleep
from multiprocessing import Process, Lock
import argparse
import cProfile
import os
import sys

import connexion
import plyvel
from typing import Any
import logging

import src.database_updater as database_updater
import src.bulk_database_updater as bulk_database_updater
from src.blockchain_wrapper import BlockchainWrapper
from src.database_gatherer import DatabaseGatherer

# TODO: CHANGE THINGS TO TEST BULK
# TODO: Add traces
# TODO: Add token transactions
# TODO: Save addresses to pytables??
# TODO: Implement token gathering.

logging.basicConfig(format=('%(asctime)s - %(levelname)s - %(message)s'))
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

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
        database_updater.update_database(db_location, db_lock, blockchain)


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
    parser.add_argument('--bulk_size', type=int, default=10000,
                        help='How many blocks should be processed at once.')
    parser.add_argument('--parse_traces', type=bool, default=False,
                        help='Whether the internal transactions should be examined as well.' \
                              'Warning: May take a long time.')
    parser.add_argument('--datapath', type=str, default='data/',
                        help='Path, where temporary update data should be saved.' \
                             'Warning: It will reach several GBs during the initial sync.')
    parser.add_argument('--gather_tokens', type=bool, default=False,
                        help='If the blockchain explorer should also gather token data.')

def init_data_dir(datapath: str):
    """
    Initializes files holding program state values.

    Args:
        datapath: Path to where the data will be stored.
    """
    if not os.path.exists(datapath) or not os.path.isdir(datapath):
        os.mkdir(datapath)
    
    if not os.path.exists(datapath + '/progress.txt'):
        with open(datapath + '/progress.txt', 'w+') as f:
            f.write('0\n0')


def main():
    """Main function."""
    LOG.info('You have started the blockchain explorer.')
    parser = argparse.ArgumentParser()
    add_args(parser)
    args = parser.parse_args()

    db = plyvel.DB(args.dbpath, create_if_missing=True)
    db.close()
    db_lock = Lock()
    datapath = args.datapath
    if datapath[-1] != '/':
        datapath = datapath + '/'
    init_data_dir(datapath)
    blockchain = BlockchainWrapper(args.interface, args.confirmations)
    # Before API interface is started, database is created/updated.
    print(args.gather_tokens)
    bulk_database_updater.update_database(args.dbpath, db_lock, args.interface,
                                          args.confirmations, args.bulk_size,
                                          args.parse_traces, datapath, args.gather_tokens)
    sys.exit(0)
    # blockchain_daemon_p = Process(target=blockchain_daemon, args=(args.dbpath,
    #                                                               db_lock,
    #                                                               blockchain,
    #                                                               args.refresh))
    # blockchain_daemon_p.start()
    app = connexion.App(__name__, specification_dir='cfg/')
    app.app.config['DB_LOCATION'] = args.dbpath
    app.app.config['DB_LOCK'] = db_lock
    app.add_api('swagger.yaml')
    app.run()


if __name__ == "__main__":
    # cProfile.run('main()')
    main()
