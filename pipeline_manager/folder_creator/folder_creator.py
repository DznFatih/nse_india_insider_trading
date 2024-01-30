from abc import ABC, abstractmethod

from lib.lib import Path, datetime


class FolderCreator(ABC):

    @abstractmethod
    def create_xbrl_folder(self) -> None:
        pass

    @abstractmethod
    def get_folder_path(self) -> Path:
        pass


class XBRLFolderCreator(FolderCreator):

    def __init__(self):
        self.__xbrl_parent_directory: Path = Path.cwd() / "XBRL Files"
        self.__xbrl_sub_directory: Path = Path()
        self.__xbrl_file_loc: Path = Path()
        self.__current_date_time = datetime.datetime.now()

    def create_xbrl_folder(self) -> None:
        # Check if "XBRL Files" parent directory exist. If not, create one
        if not Path.is_dir(self.__xbrl_parent_directory):
            Path.mkdir(self.__xbrl_parent_directory)
        # Create subdirectory under "XBRL Files" with datetime in the name of folder
        self.__xbrl_sub_directory = self.__xbrl_parent_directory / self.__current_date_time.strftime('%d%m%Y%H%M%S')
        if not Path.is_dir(self.__xbrl_sub_directory):
            Path.mkdir(self.__xbrl_sub_directory)

    def get_folder_path(self) -> Path:
        return self.__xbrl_sub_directory
