from pipeline_manager.data_saver.data_saver_interface import DataSaver
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator_interface import FolderCreator
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.metadata_saver.metadata_saver_interface import MetadataSaverInterface
from pipeline_manager.primary_source.primary_source_interface import PrimarySource
from lib.lib import datetime, timezone


class EntityBase:

    def __init__(self, primary_source_list: list[PrimarySource], data_processor: EntityProcessor,
                 data_saver: list[DataSaver], folder_creator: FolderCreator,
                 data_description: str, metadata_logger: MetadataSaverInterface) -> None:
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
        self.__execution_start_time: str = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.__data_description: str = data_description
        self.__search_page_visit_time: str = None
        self.__metadata_logger: MetadataSaverInterface = metadata_logger
        self.__metadata: dict = {}

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
        self.__build_metadata()
        self.__metadata_logger.save_metadata_to_file(folder_path=self.__folder_creator.get_folder_path(),
                                                     data=self.__metadata)

    def __build_metadata(self):
        """
        Creates metadata dictionary to be processed by metadata_logger
        :return:
        """
        self.__metadata["PythonScriptExecutionStartTime"] = self.__execution_start_time
        self.__metadata["DataDescription"] = self.__data_description
        self.__metadata["NSESearchPageVisitTime"] = self.__search_page_visit_time
        self.__metadata["XBRLDocumentDownloadsStartTime"] = (
            self.__replace_null_for_date(self.__data_processor.get_xbrl_document_download_start_time()))
        self.__metadata["XBRLDocumentDownloadsEndTime"] = (
            self.__replace_null_for_date(self.__data_processor.get_xbrl_document_download_end_time()))
        self.__metadata["XBRLDocumentPageVisitAttemptCount"] = str(self.__data_processor.get_xbrl_document_page_visit_attempt_count())
        self.__metadata["XBRLDocumentDownloadErrorCount"] = str(self.__data_processor.get_xbrl_document_download_error_count())
        self.__metadata["XBRLDocumentDownloadSuccessCount"] = str(self.__data_processor.get_xbrl_document_download_success_count())

    @staticmethod
    def __replace_null_for_date(date_data: datetime) -> str:
        if date_data is None:
            return '-'
        return date_data

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
        self.__search_page_visit_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data: dict = {}
        for item in self.__primary_source_list:
            data[item.get_data_key_name()] = item.get_data()

        return data
