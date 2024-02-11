from pipeline.extract_parameter.nse_insider_trading_extract_parameter import NSEIndiaInsiderTradingExtractParameter
from pipeline_manager.entity_base.entity_base import EntityBase
from pipeline_manager.entity_parameter.entity_parameter import EntityParameter
from lib.lib import Path


def entity_base_initiator(entity_parameter: EntityParameter) -> None:
    """
    Runs workflow by creating EntityBase object and calling its initiate_pipeline method
    :param entity_parameter: EntityParameter
    :return:
    """
    entity_base = EntityBase(primary_source_list=entity_parameter.primary_source(),
                             data_processor=entity_parameter.data_processor(),
                             data_saver=entity_parameter.data_saver(),
                             folder_creator=entity_parameter.folder_creator(),
                             data_description=entity_parameter.get_data_source_description(),
                             metadata_logger=entity_parameter.get_metadata_logger())
    entity_base.initiate_pipeline()


def log_info_to_a_file(dict_data: dict) -> None:
    """
    Logs workflow run result (error or successful run) to log file in current working directory
    :param dict_data: dictionary
    :return:
    """
    error_info_loc: Path = Path.cwd() / "error_info"
    if not Path.is_dir(error_info_loc):
        Path.mkdir(error_info_loc)
    file_loc: Path = error_info_loc / "error_info_file.txt"
    with open(file_loc, 'w') as f:
        f.write(str(dict_data))


if __name__ == "__main__":
    """
        Entry point to the program. Creates NSEIndiaInsiderTradingExtractParameter object and passes it to 
        entity_base_initiator function for starting the workflow.
        
        Object NSEIndiaInsiderTradingExtractParameter provides public methods to access its attributes:
            - Primary source - Responsible for downloading data from NSEIndia website
            - Data processor - Responsible for processing data received to its process_data method. It is also
                               responsible for asking XBRLFileDownloader to download XBRL files and for asking
                               XBRLProcessor to process data in those files
            - Data saver - Responsible for saving processed data to delimited file
            
        EntityBase later is called to run entire workflow.
            
    """
    log_info: dict = {}
    try:
        # date_format -> 'DD-MM-YYYY'
        parameter: EntityParameter = NSEIndiaInsiderTradingExtractParameter(from_date="14-04-2018", to_date="20-04-2018")
        entity_base_initiator(entity_parameter=parameter)
        log_info = {"content": "successful"}
        log_info_to_a_file(log_info)
    except Exception as e:
        log_info_to_a_file(dict_data={"content": "error", "error_message": e})
