"""Retrieves raw data from the blockchain."""
import subprocess
import os
import csv
import logging

LOG = logging.getLogger()


class DataRetriever:
    """Retrieves raw data from the blockchain."""

    def __init__(self, interface: str, datapath: str, gather_tokens: bool) -> None:
        """
        Initialization.

        Args:
            interface: Path to the Geth blockchain node.
            datapath: Path for temporary file created in DB creation.
            gather_tokens: Whether to also gather tokens
        """
        self._interface = interface
        self.datapath = datapath
        self.gather_tokens = gather_tokens

    def create_csv_files(self, first_block: int, last_block: int) -> None:
        """
        Creates csv files holding the new information.

        Args:
            first_block: First block to be included.
            last_block: Last block to be included.
        """
        LOG.info('Getting blocks from blockchain')
        # Get blocks and their transactions
        block_tx_cmd = "ethereumetl export_blocks_and_transactions --start-block {} " \
                       "--end-block {} --provider-uri {} --blocks-output {} " \
                       "--transactions-output {}".format(first_block, last_block,
                                                         self._interface,
                                                         self.datapath + 'blocks.csv',
                                                         self.datapath + 'transactions.csv')
        print(block_tx_cmd)
        subprocess.call(block_tx_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        LOG.info('Getting transactions from blockchain')
        # Get transaction hashes
        tx_hash_cmd = "ethereumetl extract_csv_column --input {} --column hash " \
                      "--output {}".format(self.datapath + 'transactions.csv',
                                           self.datapath + 'tx_hashes.txt')
        subprocess.call(tx_hash_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        LOG.info('Getting receipts from blockchain')
        # Get receipts
        tx_receipts_cmd = "ethereumetl export_receipts_and_logs --transaction-hashes {} " \
                          " --provider-uri {} --receipts-output {} " \
                          "--logs-output {}".format(self.datapath + '/tx_hashes.txt',
                                                    self._interface,
                                                    self.datapath + 'receipts.csv',
                                                    self.datapath + 'logs.csv')
        subprocess.call(tx_receipts_cmd.split(),
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if self.gather_tokens:
            self.create_token_csv(first_block, last_block)

    def create_token_csv(self, first_block: int, last_block: int) -> None:
        """
        Creates csv file containing information of Token addresses.

        Args:
            first_block: First block to be included.
            last_block: Last block to be included.
        """
        LOG.info('Getting contract addresses from blockchain')
        # get contract addresses
        contracts_addr_cmd = "ethereumetl extract_csv_column --input {} " \
                             " --column contract_address " \
                             "--output {}".format(self.datapath + 'receipts.csv',
                                                  self.datapath + 'contract_addresses.txt')
        subprocess.call(contracts_addr_cmd.split(),
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        LOG.info('Getting contracts from blockchain')
        # get contracts
        # This is a bottleneck basically
        contracts_cmd = "ethereumetl export_contracts --contract-addresses {} " \
                        " --provider-uri {} " \
                        "--output {} ".format(self.datapath + 'contract_addresses.txt',
                                              self._interface,
                                              self.datapath + 'contracts.csv')
        subprocess.call(contracts_cmd.split(),
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Extract token addresses
        if os.path.isfile(self.datapath + 'contracts.csv'):
            data = []
            with open(self.datapath + 'contracts.csv') as f:
                for row in csv.DictReader(f, delimiter=','):
                    if row['is_erc20'] == 'True' or row['is_erc721'] == 'True':
                        data.append(row['address'])
            with open(self.datapath + 'token_addresses.txt', 'w') as f:
                f.write('\n'.join(data))
        
        LOG.info('Getting tokens from blockchain')
        # get Tokens
        tokens_cmd = "ethereumetl export_tokens --token-addresses {} " \
                     " --provider-uri {} " \
                     "--output {} ".format(self.datapath + 'token_addresses.txt', self._interface,
                                           self.datapath + 'tokens.csv')
        subprocess.call(tokens_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        LOG.info('Getting token transfers from blockchain')
        token_tx_cmd = "ethereumetl extract_token_transfers --logs {} " \
                       "--output {}".format(self.datapath + 'logs.csv',
                                            self.datapath + 'token_transfers.csv')
        subprocess.call(token_tx_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
