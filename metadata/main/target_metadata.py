from abc import ABC, abstractmethod


class TargetMetadata(ABC):

    @property
    @abstractmethod
    def get_schema_name(self) -> str:
        pass

    @property
    @abstractmethod
    def get_table_name(self) -> str:
        pass

    @property
    @abstractmethod
    def get_table_column_name_list(self) -> list[str]:
        pass
