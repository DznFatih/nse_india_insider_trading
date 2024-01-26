from pipeline_manager.entity_processor.entity_processor import EntityProcessor


class NSEIndiaInsiderTradingExtractProcessor(EntityProcessor):

    def __init__(self, primary_source_data_key_name: str):
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[tuple] = list()

    def get_cleaned_data(self) -> list[tuple]:
        pass

    def get_cleaned_row_count(self) -> int:
        pass

    def process_data(self, raw_data: dict) -> None:
        self.__unload_data(raw_data)
        self.__process_data()

    def __unload_data(self, raw_data):
        self.__raw_data = raw_data[self.__primary_source_data_key_name]

    def __process_data(self) -> None:
        for item in self.__raw_data:
