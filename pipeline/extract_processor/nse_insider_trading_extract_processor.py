from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from lib.lib import datetime, timezone

class NSEIndiaInsiderTradingExtractProcessor(EntityProcessor):

    def __init__(self, primary_source_data_key_name: str, source_system: str):
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[tuple] = list()
        self.__source_system: str = source_system
        self.__insert_date: datetime = datetime.datetime.strptime(datetime.datetime.now(timezone.utc).
                                                                  strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')

    def get_cleaned_data(self) -> list[tuple]:
        return self.__cleaned_data

    def get_cleaned_row_count(self) -> int:
        return len(self.__cleaned_data)

    def process_data(self, raw_data: dict) -> None:
        self.__unload_data(raw_data)
        self.__process_data()

    def __unload_data(self, raw_data):
        self.__raw_data = raw_data[self.__primary_source_data_key_name]

    def __process_data(self) -> None:
        for item in self.__raw_data:
            self.__cleaned_data.append((
                item["symbol"],
                item["company"],
                item["anex"],
                item["acqName"],
                item["secType"],
                item["secAcq"],
                item["tdpTransactionType"],
                item["date"],
                item["xbrl"],

            ))

    def __read_xbrl_file_return_date(self, file_url: str) -> datetime:
        pass