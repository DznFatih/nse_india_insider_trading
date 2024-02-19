from lib.lib import Path
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.metadata_saver.metadata_saver_interface import MetadataSaverInterface


class MetadataSaver(MetadataSaverInterface):

    def __init__(self) -> None:
        """
        Saves metadata for workflow to indicated path
        """
        self.__file_name: str = 'Metadata.txt'

    def save_metadata_to_file(self, folder_path: Path, data: dict) -> None:
        """
        Saves data to a file in the indicated folder its argument. It checks if
        there is any data processed. If there is, it will save to folder.
        :param folder_path: folder path to save file
        :param data: dictionary data
        :return:
        """
        try:
            if data:
                headers = list(data.keys())
                values = list(data.values())
                with open(folder_path / self.__file_name, "w", encoding="utf-8") as f:
                    delimiter = 'þ'
                    f.write(f"{delimiter}".join(headers))
                    f.write("\n")
                    f.write(f"{delimiter}".join(values))
        except Exception as e:
            raise Exception(get_error_details(e))
