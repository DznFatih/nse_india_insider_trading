
from pipeline.extract_parameter.nse_insider_trading_extract_parameter import NSEIndiaInsiderTradingExtractParameter
from pipeline_manager.entity_base.entity_base import EntityBase
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter
from lib.lib import Path


def entity_base_initiator(entity_parameter: EntityParameter) -> None:
    entity_base = EntityBase(primary_source_list=entity_parameter.primary_source(),
                             data_processor=entity_parameter.data_processor(),
                             data_saver=entity_parameter.data_saver(),
                             folder_creator=entity_parameter.folder_creator())
    entity_base.initiate_pipeline()


def log_info_to_a_file(dict_data: dict) -> None:
    error_info_loc: Path = Path.cwd() / "error_info"
    if not Path.is_dir(error_info_loc):
        Path.mkdir(error_info_loc)
    file_loc: Path = error_info_loc / "error_info_file.txt"
    with open(file_loc, 'w') as f:
        f.write(str(dict_data))


if __name__ == "__main__":
    log_info: dict = {}
    try:
        # date_format -> 'DD-MM-YYYY'
        parameter: EntityParameter = NSEIndiaInsiderTradingExtractParameter(from_date="18-12-2020", to_date="18-12-2020")
        entity_base_initiator(entity_parameter=parameter)
        log_info = {"content": "successful"}
        log_info_to_a_file(log_info)
    except Exception as e:
        log_info_to_a_file(dict_data={"content": "error", "error_message": e})

# create requirements file
# also include Python version