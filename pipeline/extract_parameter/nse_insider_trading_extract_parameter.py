from metadata.source_metadata.nse_insider_trading_source_metadata import NSEIndiaInsiderTradingSourceMetadata
from pipeline.extract_processor.nse_insider_trading_extract_processor import NSEIndiaInsiderTradingExtractProcessor
from pipeline_manager.metadata_saver.metadata_saver import MetadataSaver
from pipeline_manager.metadata_saver.metadata_saver_interface import MetadataSaverInterface
from pipeline_manager.xbrl_downloader.nse_insider_trading_extract_xbrl_downloader import XBRLFileDownloader
from pipeline_manager.xbrl_downloader.xbrl_file_downloader_interface import XBRLFileDownloaderInterface
from pipeline_manager.xbrl_processor.nse_insider_trading_extract_xbrl_processor import XBRLProcessor
from pipeline_manager.data_saver.data_saver import FileSaver
from pipeline_manager.data_saver.data_saver_interface import DataSaver
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator import XBRLFolderCreator
from pipeline_manager.folder_creator.folder_creator_interface import FolderCreator
from pipeline_manager.primary_source.primary_source import NSEIndiaHTTPXBRLFilePrimarySource, \
    HTTPRequestPrimarySource
from pipeline_manager.primary_source.primary_source_interface import PrimarySource, XBRLPrimarySource
from pipeline_manager.xbrl_processor.xbrl_processor_interface import XBRLProcessorInterface


class NSEIndiaInsiderTradingExtractParameter(EntityParameter):
    """ Class for creating necessary objects in order to download, process and save the data
    from the source. Instead of creating objects in main.py file, we create them in here in one place
    for the associated entity, i.e. data processing object NSEIndiaInsiderTradingExtractProcessor """
    def __init__(self, from_date: str = None, to_date: str = None):
        """
        Initializer function for instance variables.
        :param from_date: Optional parameter. When skipped, default value of today - 365 will be used
        :param to_date: Optional parameter. When skipped, default value of today will be used
        """
        self.__data_description: str = "IndiaNSEInsiderTransactions"
        self.__primary_source_data_key_name: str = "IndiaNSEInsiderTransactions"
        self.__from_date: str = from_date
        self.__to_date: str = to_date
        self.__source_metadata: NSEIndiaInsiderTradingSourceMetadata = NSEIndiaInsiderTradingSourceMetadata()
        self.__xbrl_file_primary_source: XBRLPrimarySource = NSEIndiaHTTPXBRLFilePrimarySource(
                header=self.__source_metadata.get_header(),
                cookie_url=self.__source_metadata.get_cookie_url(),
                base_url=self.__source_metadata.get_base_url())
        self.__xbrl_downloader: XBRLFileDownloaderInterface = XBRLFileDownloader(
            primary_source=self.__xbrl_file_primary_source)
        self.xbrl_folder_creator: FolderCreator = XBRLFolderCreator()

        self.__xbrl_processor: XBRLProcessorInterface = XBRLProcessor()
        self.__data_processor: EntityProcessor = NSEIndiaInsiderTradingExtractProcessor(
            primary_source_data_key_name=self.__primary_source_data_key_name,
            xbrl_downloader=self.__xbrl_downloader,
            xbrl_processor=self.__xbrl_processor)
        self.__metadata_logger = MetadataSaver()

    def get_primary_source(self) -> list[PrimarySource]:
        """
        Returns list of HTTPRequestPrimarySource object which inherits PrimarySource class
        :return: List of PrimarySource
        """
        return [HTTPRequestPrimarySource(data_key_name=self.__primary_source_data_key_name,
                                         from_date=self.__from_date,
                                         to_date=self.__to_date,
                                         header=self.__source_metadata.get_header(),
                                         base_url=self.__source_metadata.get_base_url(),
                                         cookie_url=self.__source_metadata.get_cookie_url())]

    def get_data_processor(self) -> EntityProcessor:
        """
        Returns NSEIndiaInsiderTradingExtractProcessor object which inherits EntityProcessor
        :return: NSEIndiaInsiderTradingExtractProcessor object
        """
        return self.__data_processor

    def get_data_saver(self) -> list[DataSaver]:
        """
        Returns list of FileSaver which inherits DataSaver
        :return: list of FileSave
        """
        return [FileSaver(data=self.__data_processor.get_cleaned_data(),
                          file_name="NSEData.txt"),
                FileSaver(data=self.__data_processor.get_changed_data(),
                          file_name="ChangedData.txt")]

    def get_folder_creator(self) -> FolderCreator:
        """
        Returns XBRLFolderCreator which inherits FolderCreator
        :return: XBRLFolderCreator
        """
        return self.xbrl_folder_creator

    def get_data_source_description(self) -> str:
        """
        Returns data source description
        :return:
        """
        return self.__data_description

    def get_metadata_logger(self) -> MetadataSaverInterface:
        """
        Returns MetadataSaverInterface
        :return:
        """
        return self.__metadata_logger
