from abc import ABC, abstractmethod
from lib.lib import Path


class EntityProcessor(ABC):
    """
    Entity processor interface
    """
    @abstractmethod
    def get_cleaned_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_orphan_cleaned_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_cleaned_row_count(self) -> int:
        pass

    @abstractmethod
    def process_data(self, raw_data: dict, xbrl_folder_path: Path) -> None:
        pass
