from pipeline_manager.primary_source.primary_source import XBRLPrimarySource
from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from lib.lib import Path, models


class XBRLFileDownloader(XBRLFileDownloaderABC):

    def __init__(self, primary_source: XBRLPrimarySource):
        self.__xbrl_file_name: str = ""
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_data: models.Response = None
        self.__xbrl_folder_path: Path = Path()

    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        self.__xbrl_folder_path = xbrl_folder_path
        data: dict = self.__primary_source.get_data(xbrl_url=xbrl_url)
        self.__xbrl_data: models.Response = data[self.__primary_source.get_data_key_name()]
        self.__xbrl_file_name: str = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)

        with open(self.__xbrl_folder_path / self.__xbrl_file_name, 'w') as f:
            f.write(self.__xbrl_data.text)

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        return xbrl_url.split("/")[-1]

    def get_xbrl_data(self) -> str:
        return self.__xbrl_data.text
