"""Will probably update database or something."""
# from multiprocessing import Lock
from typing import Any

from src.common import setup_database


@setup_database
def update_database(db_location: str, db_lock: Any, db=None) -> None:
    """
    Updates database with new entries.

    Args: db_location: Path to the leveldb database.
          db_lock: Instance of the database lock (to prevent multiple access).
          db: Database instance.
    """
    db.put(b'dva', b'sest;osem;sedem')
