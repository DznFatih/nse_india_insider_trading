from primary_source.primary_source import XBRLPrimarySource
from lib.lib import Path, datetime, BeautifulSoup


class XBRLFileDownloader:

    def __init__(self, primary_source: XBRLPrimarySource):
        self.__xbrl_directory: Path = Path.cwd() / "XBRL Files"
        self.__current_date_time = datetime.datetime.now()
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_file_loc: Path = Path()
        self.__file_name: str = ""

    def store_xbrl_file(self, xbrl_url: str) -> None:
        # Check if "XBRL Files" directory exist
        if not Path.is_dir(self.__xbrl_directory):
            Path.mkdir(self.__xbrl_directory)
        # Create subdirectory under"XBRL Files" with datetime in the name of directory
        self.__xbrl_file_loc = self.__xbrl_directory / self.__current_date_time.strftime('%d%m%Y%H%M%S')
        if not Path.is_dir(self.__xbrl_file_loc):
            Path.mkdir(self.__xbrl_file_loc)
        self.__download_xbrl_file_to_local_machine(xbrl_url=xbrl_url)

    def __download_xbrl_file_to_local_machine(self, xbrl_url: str) -> None:
        data: dict = self.__primary_source.get_data(xbrl_url=xbrl_url)
        self.__xbrl_data = data[self.__primary_source.get_data_key_name()]
        self.__file_name = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)
        self.__soup = BeautifulSoup(self.__xbrl_data, features="xml")
        with open(self.__xbrl_file_loc / self.__file_name, 'w') as f:
            f.write(self.__xbrl_data.text)

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        return xbrl_url.split("/")[-1]