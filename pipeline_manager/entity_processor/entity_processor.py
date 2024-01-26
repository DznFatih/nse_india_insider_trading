from abc import ABC, abstractmethod


class EntityProcessor(ABC):

    @abstractmethod
    def get_cleaned_data(self) -> list[tuple]:
        pass

    @abstractmethod
    def get_cleaned_row_count(self) -> int:
        pass

    @abstractmethod
    def process_data(self, raw_data: dict) -> None:
        pass
