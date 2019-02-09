#!/usr/bin/env python3
"""Ethereum blockchain explorer."""

from time import sleep
from multiprocessing import Process

import connexion
import plyvel

from src.db_updater import update_database

def blockchain_daemon(db_location: str) -> None:
    """
    Updates the leveldb database while REST API is already running.

    Args:
        db_location: Path to the leveldb database.
    """
    while True:
        sleep(20)
        update_database(db_location)

def main():
    """Main function."""
    print('You have started the blockchain explorer.')

    db = plyvel.DB('db/', create_if_missing=True)
    db.put(b'jeden', b'dva;tri;styri')
    db.close()
    blockchain_daemon_process = Process(target=blockchain_daemon, args=('db/',))
    blockchain_daemon_process.start()
    app = connexion.App(__name__, specification_dir='cfg/')
    app.app.config['DB_LOCATION'] = 'db/'
    app.add_api('swagger.yaml')
    app.run()


if __name__ == "__main__":
    main()
