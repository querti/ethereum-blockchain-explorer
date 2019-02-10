"""Class used to gather information directly from Ethereum full node."""

from web3 import Web3


class BlockchainWrapper:
    """Class used to gather information directly from Ethereum full node."""

    def __init__(self, interface: str) -> None:
        """
        Initialization.

        Args:
            interface: String representing RPC, WS, or IPC interface.
        """
        self.web3 = None
        self._interface = interface
        if self._interface == '' or 'ipc' in self._interface:
            self._web3 = Web3(Web3.IPCProvider(self._interface))
        elif 'http' in self._interface:
            self._web3 = Web3(Web3.HTTPProvider(self._interface))
        elif 'ws' in self._interface:
            self._web3 = Web3(Web3.WebsocketProvider(self._interface))
        else:
            self._web3 = Web3()
