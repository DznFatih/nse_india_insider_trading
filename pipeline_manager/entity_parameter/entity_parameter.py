from abc import ABC, abstractmethod

from pipeline_manager.data_saver.data_saver import DataSaver
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator import FolderCreator
from pipeline_manager.primary_source.primary_source import PrimarySource


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

    def folder_creator(self) -> FolderCreator:
        return self.get_folder_creator()

    @abstractmethod
    def get_folder_creator(self) -> FolderCreator:
        pass

    def data_saver(self) -> list[DataSaver]:
        return self.get_data_saver()

    @abstractmethod
    def get_data_saver(self) -> list[DataSaver]:
        pass
