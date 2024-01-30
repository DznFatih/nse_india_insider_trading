from pipeline_manager.error_info.error_logger import get_original_error_message
from pipeline_manager.primary_source.primary_source import XBRLPrimarySource
from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from lib.lib import Path, models


class XBRLFileDownloader(XBRLFileDownloaderABC):

    def __init__(self, primary_source: XBRLPrimarySource):
        self.__xbrl_file_name: str = ""
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_data: models.Response = None
        self.__xbrl_folder_path: Path = Path()
        self.__xbrl_file_list: dict = dict()

    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        try:
            self.__xbrl_file_name: str = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)
            if self.__xbrl_file_list.get(self.__xbrl_file_name) is None:
                self.__xbrl_folder_path = xbrl_folder_path
                self.__xbrl_data: models.Response = self.__primary_source.get_data(xbrl_url=xbrl_url)
                self.__xbrl_file_list[self.__xbrl_file_name] = self.__xbrl_data.text
                with open(self.__xbrl_folder_path / self.__xbrl_file_name, 'w') as f:
                    f.write(self.__xbrl_data.text)
        except KeyError as e:
            raise KeyError(get_original_error_message(e))
        except TypeError as e:
            raise TypeError(get_original_error_message(e))
        except ValueError as e:
            raise ValueError(get_original_error_message(e))
        except Exception as e:
            raise Exception(get_original_error_message(e))

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        return xbrl_url.split("/")[-1]

    def get_xbrl_data(self) -> str:
        return self.__xbrl_file_list[self.__xbrl_file_name]
