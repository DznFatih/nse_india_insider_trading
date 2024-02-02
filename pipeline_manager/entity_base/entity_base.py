from pipeline_manager.data_saver.data_saver_interface import DataSaver
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator_interface import FolderCreator
from pipeline_manager.primary_source.primary_source_interface import PrimarySource


class EntityBase:

    def __init__(self, primary_source_list: list[PrimarySource], data_processor: EntityProcessor,
                 data_saver: list[DataSaver], folder_creator: FolderCreator) -> None:
        self.__primary_source_list: list[PrimarySource] = primary_source_list
        self.__data_processor: EntityProcessor = data_processor
        self.__data_saver: list[DataSaver] = data_saver
        self.__run_result: dict = dict()
        self.__folder_creator: FolderCreator = folder_creator

    def initiate_pipeline(self) -> None:
        data: dict = self.__get_primary_source_data()
        self.__folder_creator.create_xbrl_folder()
        self.__data_processor.process_data(raw_data=data, xbrl_folder_path=self.__folder_creator.get_folder_path())
        self.__save_data_to_file()

    def __save_data_to_file(self) -> None:
        for item in self.__data_saver:
            item.save_data(folder_path_to_save_data=self.__folder_creator.get_folder_path())

    def __get_primary_source_data(self) -> dict:
        data: dict = {}
        for item in self.__primary_source_list:
            data[item.get_data_key_name()] = item.get_data()

        return data
