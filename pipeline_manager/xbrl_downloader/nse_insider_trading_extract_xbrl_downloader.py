from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.primary_source.primary_source_interface import XBRLPrimarySource
from lib.lib import Path, models
from pipeline_manager.xbrl_downloader.xbrl_file_downloader_interface import XBRLFileDownloaderInterface


class XBRLFileDownloader(XBRLFileDownloaderInterface):

    def __init__(self, primary_source: XBRLPrimarySource):
        """
        Asks XBRLPrimarySource to download XBRL file from internet, saves that file to user's machine.
        Before asking to download XBRL file, it checks if the file was downloaded before. It is possible that
        the same XBRL file was downloaded before because the link to the file could be referenced by multiple rows
        of the table for the same company's transactions. Companies can have multiple transactions in the same hour
        by different employees and this company's transactions might be stored in the same file hence referenced by
        those rows.
        :param primary_source: XBRLPrimarySource interface
        """
        self.__xbrl_file_name: str = ""
        self.__primary_source: XBRLPrimarySource = primary_source
        self.__xbrl_data: models.Response = None
        self.__xbrl_folder_path: Path = Path()
        self.__xbrl_file_list: dict = dict()
        self.__xbrl_document_page_visit_attempt_count: int = 0
        self.__xbrl_document_download_success_count: int = 0
        self.__xbrl_document_download_error_count: int = 0

    def download_xbrl_file_to_local_machine(self, xbrl_url: str, xbrl_folder_path: Path) -> None:
        """
        Downloads XBRL documents from target url and saves it to local machine. Counts attempts
        to access to source, successful download and errors
        :param xbrl_url: XBRL link
        :param xbrl_folder_path: folder path to save this file in xml format
        :return:
        """
        self.__xbrl_file_name: str = self.__get_file_name_from_xbrl_url(xbrl_url=xbrl_url)
        print(f"Downloading file -> {self.__xbrl_file_name}")
        if self.__xbrl_file_list.get(self.__xbrl_file_name) is None:
            self.__xbrl_folder_path = xbrl_folder_path
            try:
                self.__xbrl_document_page_visit_attempt_count += 1
                self.__xbrl_data: models.Response = self.__primary_source.get_data(xbrl_url=xbrl_url)
                self.__xbrl_document_download_success_count += 1
            except Exception as e:
                self.__xbrl_document_download_error_count += 1
                self.__xbrl_file_list[self.__xbrl_file_name] = None
                return
            self.__xbrl_file_list[self.__xbrl_file_name] = self.__xbrl_data.text
            with open(self.__xbrl_folder_path / self.__xbrl_file_name, 'w') as f:
                f.write(self.__xbrl_data.text)

    @staticmethod
    def __get_file_name_from_xbrl_url(xbrl_url: str) -> str:
        """
        Returns the file name from xbrl url
        :param xbrl_url: XBRL link
        :return:
        """
        return xbrl_url.split("/")[-1]

    def get_xbrl_data(self) -> str:
        """
        Returns data from XBRL file in string format
        :return: string
        """
        return self.__xbrl_file_list[self.__xbrl_file_name]

    def get_xbrl_document_page_visit_attempt_count(self) -> int:
        return self.__xbrl_document_page_visit_attempt_count

    def get_xbrl_document_download_success_count(self) -> int:
        return self.__xbrl_document_download_success_count

    def get_xbrl_document_download_error_count(self) -> int:
        return  self.__xbrl_document_download_error_count
