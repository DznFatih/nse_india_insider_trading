from metadata.source_metadata.nse_insider_trading_source_metadata import NSEIndiaInsiderTradingSourceMetadata
from pipeline.extract_processor.nse_insider_trading_extract_processor import NSEIndiaInsiderTradingExtractProcessor
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from primary_source.primary_source import HTTPRequestPrimarySource, PrimarySource, FilePrimarySource


class NSEIndiaInsiderTradingExtractParameter(EntityParameter):

    def __init__(self, from_date: str = None, to_date: str = None):
        # from_date='01-01-2024', to_date='01-01-2024'
        self.__primary_source_data_key_name: str = "insider_trading"
        self.__from_date: str = from_date
        self.__to_date: str = to_date
        self.__source_metadata: NSEIndiaInsiderTradingSourceMetadata = NSEIndiaInsiderTradingSourceMetadata()

    def get_primary_source(self) -> list[PrimarySource]:
        return [FilePrimarySource(self.__primary_source_data_key_name)]
            # [HTTPRequestPrimarySource(data_key_name=self.__primary_source_data_key_name,
            #                              from_date=self.__from_date,
            #                              to_date=self.__to_date,
            #                              header=self.__source_metadata.get_header(),
            #                              base_url=self.__source_metadata.get_base_url(),
            #                              cookie_url=self.__source_metadata.get_cookie_url())]

    def get_data_processor(self) -> EntityProcessor:
        return NSEIndiaInsiderTradingExtractProcessor(primary_source_data_key_name=self.__primary_source_data_key_name)
