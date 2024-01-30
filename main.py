
from pipeline.extract_parameter.nse_insider_trading_extract_parameter import NSEIndiaInsiderTradingExtractParameter
from pipeline_manager.entity_base.entity_base import EntityBase
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter


def entity_base_initiator(entity_parameter: EntityParameter) -> None:
    entity_base = EntityBase(primary_source_list=entity_parameter.primary_source(),
                             data_processor=entity_parameter.data_processor(),
                             data_saver=entity_parameter.data_saver(),
                             folder_creator=entity_parameter.folder_creator())
    entity_base.initiate_pipeline()


if __name__ == "__main__":
    # from_date='01-01-2024', to_date='01-01-2024'
    parameter: EntityParameter = NSEIndiaInsiderTradingExtractParameter(from_date='01-01-2024', to_date='01-01-2024')
    entity_base_initiator(entity_parameter=parameter)





