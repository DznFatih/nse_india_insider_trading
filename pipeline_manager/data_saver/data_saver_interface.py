from abc import ABC, abstractmethod
from pathlib import Path


class DataSaver(ABC):

    @abstractmethod
    def save_data(self, folder_path_to_save_data: Path) -> None:
        pass
