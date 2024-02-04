from lib.lib import Path, csv
from pipeline_manager.data_saver.data_saver_interface import DataSaver


class FileSaver(DataSaver):

    def __init__(self, data: list[dict], file_name: str) -> None:
        """
        Saves XBRL file to indicated path
        :param data: Data to be saved in a file
        :param file_name: File name to be saved
        """
        self.__data: list[dict] = data
        self.__file_name: str = file_name

    def save_data(self, folder_path_to_save_data: Path) -> None:
        """
        Saves data to a file in the indicated folder its argument. It checks if
        there is any data processed. If there is, it will save to folder.
        :param folder_path_to_save_data: folder path to save file
        :return:
        """
        if self.__data:
            keys = self.__data[0].keys()
            with open(folder_path_to_save_data / self.__file_name, "w", newline='', encoding="utf-8") as f:
                dict_writer = csv.DictWriter(f, keys, delimiter="|")
                dict_writer.writeheader()
                dict_writer.writerows(self.__data)
