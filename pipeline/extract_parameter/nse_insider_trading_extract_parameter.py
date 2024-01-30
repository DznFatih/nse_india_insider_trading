from metadata.source_metadata.nse_insider_trading_source_metadata import NSEIndiaInsiderTradingSourceMetadata
from pipeline.extract_processor.nse_insider_trading_extract_processor import NSEIndiaInsiderTradingExtractProcessor
from pipeline.extract_processor.nse_insider_trading_extract_xbrl_downloader import XBRLFileDownloader
from pipeline.extract_processor.nse_insider_trading_extract_xbrl_processor import XBRLProcessor
from pipeline_manager.data_saver.data_saver import DataSaver, FileSaver
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from pipeline_manager.folder_creator.folder_creator import FolderCreator, XBRLFolderCreator
from pipeline_manager.primary_source.primary_source import XBRLPrimarySource, NSEIndiaHTTPXBRLFilePrimarySource, \
    PrimarySource, FilePrimarySource, HTTPRequestPrimarySource
from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from pipeline_manager.xbrl_processor_interface.xbrl_processor_interface import XBRLProcessorABC


class NSEIndiaInsiderTradingExtractParameter(EntityParameter):

    def __init__(self, from_date: str = None, to_date: str = None):
        # from_date='01-01-2024', to_date='01-01-2024'
        # self.__date_format: str = 'DD-MM-YYYY'
        self.__primary_source_data_key_name: str = "insider_trading"
        self.__xbrl_key_name: str = "xbrl_data"
        self.__from_date: str = from_date
        self.__to_date: str = to_date
        self.__source_metadata: NSEIndiaInsiderTradingSourceMetadata = NSEIndiaInsiderTradingSourceMetadata()
        self.__xbrl_file_primary_source: XBRLPrimarySource = NSEIndiaHTTPXBRLFilePrimarySource(
                header=self.__source_metadata.get_header(),
                cookie_url=self.__source_metadata.get_cookie_url(),
                base_url=self.__source_metadata.get_base_url(),
                data_key_name=self.__xbrl_key_name)
        self.__xbrl_downloader: XBRLFileDownloaderABC = XBRLFileDownloader(
            primary_source=self.__xbrl_file_primary_source)
        self.xbrl_folder_creator: FolderCreator = XBRLFolderCreator()

        self.__xbrl_processor: XBRLProcessorABC = XBRLProcessor()
        self.__data_processor: EntityProcessor = NSEIndiaInsiderTradingExtractProcessor(
            primary_source_data_key_name=self.__primary_source_data_key_name,
            source_system=self.__source_metadata.get_source_system(),
            xbrl_downloader=self.__xbrl_downloader,
            xbrl_processor=self.__xbrl_processor)

    def get_primary_source(self) -> list[PrimarySource]:
        return [HTTPRequestPrimarySource(data_key_name=self.__primary_source_data_key_name,
                                         from_date=self.__from_date,
                                         to_date=self.__to_date,
                                         header=self.__source_metadata.get_header(),
                                         base_url=self.__source_metadata.get_base_url(),
                                         cookie_url=self.__source_metadata.get_cookie_url())]
        # [FilePrimarySource(self.__primary_source_data_key_name)]

    def get_data_processor(self) -> EntityProcessor:
        return self.__data_processor

    def get_data_saver(self) -> list[DataSaver]:
        return [FileSaver(data=self.__data_processor.get_cleaned_data(),
                          file_name="NSE Data.txt"),
                FileSaver(data=self.__data_processor.get_orphan_cleaned_data(),
                          file_name="Orphaned NSE Data.txt")]

    def get_folder_creator(self) -> FolderCreator:
        return self.xbrl_folder_creator
