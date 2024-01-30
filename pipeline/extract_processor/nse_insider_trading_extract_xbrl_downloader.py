from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from primary_source.primary_source import XBRLPrimarySource
from lib.lib import Path, datetime, models


class XBRLFileDownloader(XBRLFileDownloaderABC):

    def __init__(self, primary_source: XBRLPrimarySource):
        self.__xbrl_parent_directory: Path = Path.cwd() / "XBRL Files"
        self.__xbrl_sub_directory: Path = Path()
        self.__xbrl_file_loc: Path = Path()
        self.__xbrl_file_name: str = ""
        self.__current_date_time = datetime.datetime.now()
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_data: models.Response = None

    def create_xbrl_folder(self) -> None:
        # Check if "XBRL Files" parent directory exist. If not, create one
        if not Path.is_dir(self.__xbrl_parent_directory):
            Path.mkdir(self.__xbrl_parent_directory)
        # Create subdirectory under "XBRL Files" with datetime in the name of folder
        self.__xbrl_sub_directory = self.__xbrl_parent_directory / self.__current_date_time.strftime('%d%m%Y%H%M%S')
        if not Path.is_dir(self.__xbrl_sub_directory):
            Path.mkdir(self.__xbrl_sub_directory)

    def download_xbrl_file_to_local_machine(self, xbrl_url: str) -> None:
        data: dict = self.__primary_source.get_data(xbrl_url=xbrl_url)
        self.__xbrl_data: models.Response = data[self.__primary_source.get_data_key_name()]
        self.__xbrl_file_name: str = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)

        with open(self.__xbrl_sub_directory / self.__xbrl_file_name, 'w') as f:
            f.write(self.__xbrl_data.text)

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        return xbrl_url.split("/")[-1]

    def get_xbrl_data(self) -> str:
        return self.__xbrl_data.text
