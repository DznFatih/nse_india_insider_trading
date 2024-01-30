from abc import abstractmethod, ABC


class XBRLFileDownloaderABC(ABC):

    @abstractmethod
    def create_xbrl_folder(self) -> None:
        pass

    @abstractmethod
    def download_xbrl_file_to_local_machine(self, xbrl_url: str) -> None:
        pass

    @abstractmethod
    def get_xbrl_data(self) -> str:
        pass
