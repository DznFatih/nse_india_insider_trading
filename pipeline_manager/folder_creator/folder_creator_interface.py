from abc import ABC, abstractmethod
from pathlib import Path


class FolderCreator(ABC):

    @abstractmethod
    def create_xbrl_folder(self) -> None:
        pass

    @abstractmethod
    def get_folder_path(self) -> Path:
        pass
