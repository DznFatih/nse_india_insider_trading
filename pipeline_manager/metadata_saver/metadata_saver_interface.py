from abc import ABC, abstractmethod
from lib.lib import Path


class MetadataSaverInterface(ABC):

    @abstractmethod
    def save_metadata_to_file(self, folder_path: Path, data: dict) -> None:
        pass
