from abc import ABC, abstractmethod
from lib.lib import Path, datetime


class EntityProcessor(ABC):
    """
    Entity processor interface
    """
    @abstractmethod
    def get_cleaned_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_changed_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_cleaned_row_count(self) -> int:
        pass

    @abstractmethod
    def process_data(self, raw_data: dict, xbrl_folder_path: Path) -> None:
        pass

    @abstractmethod
    def get_xbrl_document_download_end_time(self) -> str:
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

    @abstractmethod
    def get_xbrl_document_download_start_time(self) -> str:
        pass
