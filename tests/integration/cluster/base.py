import socket
from abc import ABC, abstractmethod


class AppManager(ABC):
    @abstractmethod
    def deploy(self, params: dict = None) -> dict:
        """Deploy the application environment."""

    @abstractmethod
    def cleanup(self, setup_data: dict) -> None:
        """Clean up the application environment."""


def get_free_port():
    """Ask the OS for a free ephemeral port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
