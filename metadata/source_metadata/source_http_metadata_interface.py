from abc import ABC, abstractmethod


class SourceHTTPMetadata(ABC):

    @abstractmethod
    def get_base_url(self) -> str:
        pass

    @abstractmethod
    def get_header(self) -> dict:
        pass

    @abstractmethod
    def get_cookie_url(self) -> str:
        pass

    @abstractmethod
    def get_source_system(self) -> str:
        pass
