from abc import ABC, abstractmethod

from requests import models


class PrimarySource(ABC):
    """
    Primary source interface.
    """
    @abstractmethod
    def get_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_data_key_name(self) -> str:
        pass


class XBRLPrimarySource(ABC):

    @abstractmethod
    def get_data(self, xbrl_url: str) -> models.Response:
        pass
