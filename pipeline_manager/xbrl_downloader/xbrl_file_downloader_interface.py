from abc import abstractmethod, ABC
from lib.lib import Path


class XBRLFileDownloaderInterface(ABC):

    """
    XBRLFileDownloaderInterface interface
    """

    @abstractmethod
    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        pass

    @abstractmethod
    def get_xbrl_data(self) -> str:
        pass

    @abstractmethod
    def get_xbrl_document_page_visit_attempt_count(self) -> int:
        pass

    @abstractmethod
    def get_xbrl_document_download_success_count(self) -> int:
        pass

    @abstractmethod
    def get_xbrl_document_download_error_count(self) -> int:
        pass