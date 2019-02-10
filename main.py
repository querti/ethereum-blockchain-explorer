#!/usr/bin/env python3
"""Ethereum blockchain explorer."""

from time import sleep
from multiprocessing import Process, Lock

import connexion
import plyvel
from typing import Any

from src.db_updater import update_database


def blockchain_daemon(db_location: str, db_lock: Any) -> None:
    """
    Updates the leveldb database while REST API is already running.

    Args:
        db_location: Path to the leveldb database.
    """
    while True:
        sleep(20)
        update_database(db_location, db_lock)


def main():
    """Main function."""
    print('You have started the blockchain explorer.')

    db = plyvel.DB('db/', create_if_missing=True)
    db.put(b'jeden', b'dva;tri;styri')
    db.close()
    db_lock = Lock()
    blockchain_daemon_p = Process(target=blockchain_daemon, args=('db/',
                                                                  db_lock))
    blockchain_daemon_p.start()
    app = connexion.App(__name__, specification_dir='cfg/')
    app.app.config['DB_LOCATION'] = 'db/'
    app.app.config['DB_LOCK'] = db_lock
    app.add_api('swagger.yaml')
    app.run()


if __name__ == "__main__":
    main()
