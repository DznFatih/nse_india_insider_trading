from abc import ABC, abstractmethod

from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from primary_source.primary_source import PrimarySource


class EntityParameter(ABC):

    def primary_source(self) -> list[PrimarySource]:
        return self.get_primary_source()

    @abstractmethod
    def get_primary_source(self) -> list[PrimarySource]:
        pass

    def data_processor(self) -> EntityProcessor:
        return self.get_data_processor()

    @abstractmethod
    def get_data_processor(self) -> EntityProcessor:
        pass
