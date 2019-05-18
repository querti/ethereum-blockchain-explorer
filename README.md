# Ethereum blockchain explorer

VUT FIT master's thesis - Blockchain Abstraction Library

## Overview

This is a program which simplifies the data gathering process from the Ethereum blockchain.
Before the program can be used, all data must be first gathered from the blockchain.
The gathered information can then be queried by using the REST API.

## Synchronization process

An interface to a Geth node is required for the purpose of gathering the data.
Parity node not supported.

Depending on what data is to be tracked, the process can take days or even weeks.
The created database will have the size of about 1,5x the size of the Geth node.
The application supports all three Geth interfaces: IPC, HTTP, WS

## Endpoints

A list of endpoints provided by the application:

```
/block/[:blockHash]
/block-index/[:height]
/blocks?limit=&blockStart=&blockEnd=
/block-indexes?limit=&indexStart=&indexEnd=

/tx/[:txhash]
/txs/?block=[:hash]
/txs-index/?block-index=[:blockIndex]
/txs-addr/?address=[:addr][?time_from=&time_to=&val_from=&val_to=&no_tx_list=]
/addrs/[:addrs]/txs[?time_from=&time_to=&val_from=&val_to=&no_tx_list=]
/int-txs-addr/?address=[:addr][?time_from=&time_to=&val_from=&val_to=&no_tx_list=]
/addrs/[:addrs]/int-txs[?time_from=&time_to=&val_from=&val_to=&no_tx_list=]
/token-txs-addr/?address=[:addr][?time_from=&time_to=&no_tx_list=]
/addrs/[:addrs]/token-txs[?time_from=&time_to=&no_tx_list=]

/addr/[:addr][?time_from=&time_to=&val_from=&val_to=&no_tx_list=]
/addr/[:addr]/balance
/addrs/[:addrs][?time_from=&time_to=&val_from=&val_to=&no_tx_list=]

/token/[:addr][?time_from=&time_to=&no_tx_list=]
```

A more detailed description can be found on `http://localhost:5000/api/ui/` (once the API is running).

## Usage

The application is written in Python. Following parameters can be specified:
```
usage: main.py [-h] --interface INTERFACE [--dbpath DBPATH]
               [--confirmations CONFIRMATIONS] [--refresh REFRESH]
               [--bulk_size BULK_SIZE] [--internal_txs] [--datapath DATAPATH]
               [--gather_tokens] [--max_workers MAX_WORKERS]

optional arguments:
  -h, --help            show this help message and exit
  --interface INTERFACE
                        Geth API interface address.
  --dbpath DBPATH       Path where the database will be saved.
  --confirmations CONFIRMATIONS
                        Minimum number of comfirmations until block can be
                        included.
  --refresh REFRESH     How many seconds to wait until the next database
                        refresh.
  --bulk_size BULK_SIZE
                        How many blocks should be processed at once.
  --internal_txs        Whether to also gather internal transactions.
  --datapath DATAPATH   Path, where temporary update data should be saved.
                        Warning: It will reach several GBs during the initial
                        sync.
  --gather_tokens       If the blockchain explorer should also gather token
                        data.
  --max_workers MAX_WORKERS
                        Maximum number of workers in Ethereum ETL.
```

If the chosen bulk size if too large, the application may encounter out-of-memory errors.
The program can also be run by using the `run.sh` file, where the desired parameters can be specified.
It is a wrapper script which allows the application to recover from these errors and progressively reduce the bulk size if this type of error is encountered.

## Dependencies

Python dependencies are listed in the `requirements.txt` file.

Other dependencies:

Go Ethereum - https://geth.ethereum.org/

RocksDB - https://rocksdb.org/

The application can be also run in a Docker container using the docker-compose.
In the `docker-compose.yml` file, the user needs to specify the desired paths to database and data folders.
If an IPC port will be used, the path to the Ethereum node must be specified as well.
The application invocation in the `docker-run.sh` should be edited with the desired parameters.

## Capabilities

The following data can be tracked by the application:
* Blocks
* Transactions
* Internal transactions
* Tokens
* Token transactions
* Addresses

Nearly all data provided by the Ethereum node are saved to the database.

The following data type associations are present:

* Blocks -> Transactions
* Transactions -> Internal Transactions
* Addresses -> Transactions
* Addresses -> Internal Transactions
* Addresses -> Token Transactions
* Addresses -> Mined Blocks
* Tokens -> Token Transactions

The user can opt out of tracking the token data and the internal transactions (see program parameters).
A Geth node in 'full' synchronization mode is required if the internal transactions are wished to be tracked.

The application uses RocksDB, which is a key-value database.
The data fetching speeds should be better than relational databases, but SQL queries cannot be used.