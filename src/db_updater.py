from src.common import setup_database

@setup_database
def update_database(db_location: str, db=None) -> None:
    """
    Updates database with new entries.

    Args: db_location: path to the leveldb database.
          db: database instance.
    """
    db.put(b'dva', b'sest;osem;sedem')