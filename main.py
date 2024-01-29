
from pipeline.extract_parameter.nse_insider_trading_extract_parameter import NSEIndiaInsiderTradingExtractParameter
from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from primary_source.primary_source import PrimarySource

# from_date='01-01-2024', to_date='01-01-2024'
n = NSEIndiaInsiderTradingExtractParameter(from_date='01-01-2024', to_date='01-01-2024')

primary_source_list: list[PrimarySource] = n.get_primary_source()
data: dict = {}
for item in primary_source_list:
    data[item.get_data_key_name()] = item.get_data()

print(data)







