from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.primary_source.primary_source_interface import XBRLPrimarySource
from lib.lib import Path, models
from pipeline_manager.xbrl_downloader.xbrl_file_downloader_interface import XBRLFileDownloaderInterface


class XBRLFileDownloader(XBRLFileDownloaderInterface):

    def __init__(self, primary_source: XBRLPrimarySource):
        self.__xbrl_file_name: str = ""
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_data: models.Response = None
        self.__xbrl_folder_path: Path = Path()
        self.__xbrl_file_list: dict = dict()

    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        try:
            self.__xbrl_file_name: str = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)
            print(f"Downloading file -> {self.__xbrl_file_name}")
            if self.__xbrl_file_list.get(self.__xbrl_file_name) is None:
                self.__xbrl_folder_path = xbrl_folder_path
                self.__xbrl_data: models.Response = self.__primary_source.get_data(xbrl_url=xbrl_url)
                self.__xbrl_file_list[self.__xbrl_file_name] = self.__xbrl_data.text
                with open(self.__xbrl_folder_path / self.__xbrl_file_name, 'w') as f:
                    f.write(self.__xbrl_data.text)
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        return xbrl_url.split("/")[-1]

    def get_xbrl_data(self) -> str:
        return self.__xbrl_file_list[self.__xbrl_file_name]
