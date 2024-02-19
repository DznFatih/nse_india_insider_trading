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
        The entry point to the program involves the instantiation of the NSEIndiaInsiderTradingExtractParameter object, 
        followed by its utilization in the entity_base_initiator function to commence the workflow.

        The NSEIndiaInsiderTradingExtractParameter object encapsulates public methods facilitating access to its 
        attributes, essential for the proper functioning of the entity_base_initiator function. The subsequent execution 
        of EntityBase orchestrates the entire workflow.
        
        Post-Execution Directory Structure:
            XBRLFiles
                Created in the current working directory to store sub-folders and files generated after each execution. 
                This serves as the overarching container folder. Sub-folders within are denoted by the current date and 
                time, for instance, "19022024053426" signifies the timestamp "19/02/2024 05:34:26".
                
        Post-Execution Files:
            ChangedData.txt
            
                Contains data illustrating changes from its original values. The Python script transforms data received 
                from the source, replacing None or '' with '-'. This transformation is crucial for bulk insertion in 
                the SQL script. This file is designed for reviewing original and updated values but is not intended for 
                database loading.
                
            metadata.txt
            
                Encompasses details regarding the Python execution, such as the start time and the count of downloaded 
                XBRL documents. This information is loaded into the database.
                
            NSEData.txt
            
                Presents the actual data received from the source in a delimited format. This file is designated for 
                loading into the database. This documentation clarifies the sequence of operations and the structure of 
                the generated directories and files, providing a comprehensive understanding of the program's execution 
                flow and outcomes.
                  
    """
    log_info: dict = {}
    try:
        # date_format -> 'DD-MM-YYYY'
        parameter: EntityParameter = NSEIndiaInsiderTradingExtractParameter(from_date="01-01-2019", to_date="01-01-2019")
        entity_base_initiator(entity_parameter=parameter)
        log_info = {"content": "successful"}
        log_info_to_a_file(log_info)
    except Exception as e:
        log_info_to_a_file(dict_data={"content": "error", "error_message": e})
