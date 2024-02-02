from pipeline_manager.data_saver.data_saver_interface import DataSaver
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator_interface import FolderCreator
from pipeline_manager.primary_source.primary_source_interface import PrimarySource


class EntityBase:

    def __init__(self, primary_source_list: list[PrimarySource], data_processor: EntityProcessor,
                 data_saver: list[DataSaver], folder_creator: FolderCreator) -> None:
        """
        This class controls all workflow. It downloads data from primary source, creates a folder to
        save currently running workflow data, sends its data to data processor and asks DataSaver to save data
        to specified folder.
        :param primary_source_list: PrimarySource list to loop through
        :param data_processor: Data processor
        :param data_saver: Data saver
        :param folder_creator: Folder creator
        """
        self.__primary_source_list: list[PrimarySource] = primary_source_list
        self.__data_processor: EntityProcessor = data_processor
        self.__data_saver: list[DataSaver] = data_saver
        self.__run_result: dict = dict()
        self.__folder_creator: FolderCreator = folder_creator

    def initiate_pipeline(self) -> None:
        """
        Runs the workflow. It only communicates with interfaces, so we can pass any objects to be processed
        which inherits interfaces
        :return:
        """
        data: dict = self.__get_primary_source_data()
        self.__folder_creator.create_xbrl_folder()
        self.__data_processor.process_data(raw_data=data, xbrl_folder_path=self.__folder_creator.get_folder_path())
        self.__save_data_to_file()

    def __save_data_to_file(self) -> None:
        """
        Saves processed data to a file
        :return:
        """
        for item in self.__data_saver:
            item.save_data(folder_path_to_save_data=self.__folder_creator.get_folder_path())

    def __get_primary_source_data(self) -> dict:
        """
        Collects data from primary source object
        :return:
        """
        data: dict = {}
        for item in self.__primary_source_list:
            data[item.get_data_key_name()] = item.get_data()

        return data
