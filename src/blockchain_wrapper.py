"""Class used to gather information directly from Ethereum full node."""
import logging

from web3 import Web3

LOG = logging.getLogger()


class BlockchainWrapper:
    """Class used to gather information directly from Ethereum full node."""

    def __init__(self, interface: str,
                 finality_threshold: int = 12) -> None:
        """
        Initialization.

        Args:
            interface: String representing RPC, WS, or IPC interface.
            finality_threshold: How many confirmations a block has to have.
        """
        self._finality_threshold = finality_threshold
        self.web3 = None
        self._interface = interface
        if self._interface == '' or '.ipc' in self._interface:
            self._web3 = Web3(Web3.IPCProvider(self._interface))
        elif 'http' in self._interface:
            self._web3 = Web3(Web3.HTTPProvider(self._interface))
        elif 'ws' in self._interface:
            self._web3 = Web3(Web3.WebsocketProvider(self._interface))
        else:
            self._web3 = Web3()

    def get_height(self) -> int:
        """
        Get current height of the blockchain.

        Returns:
            Current blockchain height.
        """
        return self._web3.eth.blockNumber
