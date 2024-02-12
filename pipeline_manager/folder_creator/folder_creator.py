from lib.lib import Path, datetime
from pipeline_manager.folder_creator.folder_creator_interface import FolderCreator
from pipeline_manager.get_error_details.get_error_details import get_error_details


class XBRLFolderCreator(FolderCreator):

    def __init__(self):
        """
        Creates folder for storing XBRL files and data files and provides access to folder path.
        It creates a folder called 'XBRL Files' in current working directory
        """
        self.__xbrl_parent_directory: Path = Path.cwd() / "XBRLFiles"
        self.__xbrl_sub_directory: Path = Path()
        self.__xbrl_file_loc: Path = Path()
        self.__current_date_time = datetime.datetime.now()

    def create_xbrl_folder(self) -> None:
        """
        Creates a folder for storing XBRL files
        :return:
        """
        # Check if "XBRL Files" parent directory exist. If not, create one
        try:
            if not Path.is_dir(self.__xbrl_parent_directory):
                Path.mkdir(self.__xbrl_parent_directory)
            # Create subdirectory under "XBRL Files" with datetime in the name of folder
            self.__xbrl_sub_directory = self.__xbrl_parent_directory / self.__current_date_time.strftime('%d%m%Y%H%M%S')
            if not Path.is_dir(self.__xbrl_sub_directory):
                Path.mkdir(self.__xbrl_sub_directory)
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

    def get_folder_path(self) -> Path:
        """
        Public method to provide folder path for store XBRL files
        :return: returns Path object
        """
        return self.__xbrl_sub_directory
