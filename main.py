#!/usr/bin/env python3
"""Ethereum blockchain explorer."""

from time import sleep
from multiprocessing import Process
import argparse
import os
import sys

import connexion
import rocksdb
from typing import Any
import logging

import src.updater.database_updater as database_updater

logging.basicConfig(stream=sys.stdout, format=('%(asctime)s - %(levelname)s - %(message)s'))
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def blockchain_daemon(db_location: str, interface: str, confirmations: int,
                      bulk_size: int, parse_traces: bool,
                      datapath: str, gather_tokens: bool, refresh: int, db) -> None:
    """
    Updates the leveldb database while REST API is already running.

    Args:
        db_location: Where the DB is located.
        interface: Path to the Geth blockchain node.
        confirmations: How many confirmations a block has to have.
        bulk_size: How many blocks to be included in bulk DB update.
        process_traces: Whether to get addresses from traces.
        datapath: Path for temporary file created in DB creation.
        gather_tokens: Whether to also gather token information.
        refresh: How often should the DB be updated.
    """
    while True:
        sleep(refresh)
        database_updater.update_database(db_location, interface,
                                         confirmations, bulk_size,
                                         parse_traces, datapath, gather_tokens, db)


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
    parser.add_argument('--bulk_size', type=int, default=5000,
                        help='How many blocks should be processed at once.')
    parser.add_argument('--parse_traces', type=bool, default=False,
                        help='Whether the internal transactions should be examined as well.'
                             'Warning: May take a long time.')
    parser.add_argument('--datapath', type=str, default='data/',
                        help='Path, where temporary update data should be saved.'
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
    datapath = args.datapath

    db = rocksdb.DB(args.dbpath, rocksdb.Options(create_if_missing=True, max_open_files=5000))
    if datapath[-1] != '/':
        datapath = datapath + '/'
    init_data_dir(datapath)
    # Before API interface is started, database is created/updated.
    database_updater.update_database(args.dbpath, args.interface,
                                     args.confirmations, args.bulk_size,
                                     args.parse_traces, datapath, args.gather_tokens, db)
    read_db = rocksdb.DB(args.dbpath,
                         rocksdb.Options(create_if_missing=True, max_open_files=5000),
                         read_only=True)

    # blockchain_daemon_p = Process(target=blockchain_daemon, args=(args.dbpath,
    #                                                               args.interface,
    #                                                               args.confirmations,
    #                                                               args.bulk_size,
    #                                                               args.parse_traces,
    #                                                               datapath,
    #                                                               args.gather_tokens,
    #                                                               args.refresh, db))
    # blockchain_daemon_p.daemon = True
    # blockchain_daemon_p.start()
    app = connexion.App(__name__, specification_dir='cfg/')
    app.app.config['DB_LOCATION'] = args.dbpath
    app.app.config['DB'] = read_db
    app.add_api('swagger.yaml')
    app.run()


if __name__ == "__main__":
    main()
