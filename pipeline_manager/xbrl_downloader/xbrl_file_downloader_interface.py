from abc import abstractmethod, ABC
from lib.lib import Path


class XBRLFileDownloaderInterface(ABC):

    @abstractmethod
    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        pass

    @abstractmethod
    def get_xbrl_data(self) -> str:
        pass
