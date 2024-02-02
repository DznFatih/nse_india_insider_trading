from abc import ABC, abstractmethod


class SourceHTTPMetadata(ABC):

    """ Interface for Source metadata classes."""
    @abstractmethod
    def get_base_url(self) -> str:
        pass

    @abstractmethod
    def get_header(self) -> dict:
        pass

    @abstractmethod
    def get_cookie_url(self) -> str:
        pass
