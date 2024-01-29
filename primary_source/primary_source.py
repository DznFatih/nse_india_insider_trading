import json
from abc import abstractmethod, ABC
from lib.lib import requests, date, timedelta


class PrimarySource(ABC):

    @abstractmethod
    def get_data(self) -> list[dict]:
        pass

    @abstractmethod
    def get_data_key_name(self) -> str:
        pass


class XBRLPrimarySource(ABC):

    @abstractmethod
    def get_data(self, xbrl_url: str) -> dict:
        pass

    @abstractmethod
    def get_data_key_name(self) -> str:
        pass


class HTTPRequestPrimarySource(PrimarySource):

    def __init__(self, data_key_name: str, base_url: str, cookie_url: str,
                 header: dict, from_date: str = None, to_date: str = None) -> None:
        self.__date_format: str = 'DD-MM-YYYY'
        self.__from_date: str = from_date
        self.__to_date: str = to_date
        self.__data_key_name: str = data_key_name
        self.__nse_url: str = base_url
        self.__header = header
        self.__cookie_url: str = cookie_url

    def get_data(self) -> list[dict]:
        self.__construct_nse_url()
        self.__construct_header_with_cookie()
        response = requests.get(url=self.__nse_url, headers=self.__header)
        return response.json()["data"]

    def __construct_nse_url(self) -> None:
        if self.__from_date is None:
            to_date: date = date.today()
            from_date: date = to_date - timedelta(days=365)
            self.__to_date: str = to_date.strftime('%d-%m-%Y')
            self.__from_date: str = from_date.strftime('%d-%m-%Y')
            self.__nse_url = self.__nse_url + f"from_date={self.__from_date}&to_date={self.__to_date}"
        else:
            self.__nse_url = self.__nse_url + f"from_date={self.__from_date}&to_date={self.__to_date}"

    def __construct_header_with_cookie(self) -> None:
        response = requests.get(url=self.__cookie_url, headers=self.__header)
        cookie: dict = response.cookies.get_dict()
        self.__header["Cookie"] = f"nsit={cookie['nsit']}; nseappid={cookie['nseappid']}; ak_bmsc={cookie['ak_bmsc']}"

    def get_data_key_name(self) -> str:
        return self.__data_key_name


class FilePrimarySource(PrimarySource):

    def __init__(self, data_key_name: str):
        self.__file_location: str = 'primary_source/data.json'
        self.__data_key_name: str = data_key_name

    def get_data(self) -> list[dict]:
        with open(self.__file_location) as f:
            data = json.load(f)
        return data["data"]

    def get_data_key_name(self) -> str:
        return self.__data_key_name


class NSEIndiaHTTPXBRLFilePrimarySource(XBRLPrimarySource):

    def __init__(self, header: dict, cookie_url: str, base_url: str, data_key_name: str):
        self.__header: dict = header
        self.__cookie_url: str = cookie_url
        self.__base_url: str = base_url
        self.__data_key_name: str = data_key_name

    def get_data(self, xbrl_url: str) -> dict:
        self.__get_cookie_info()
        xbrl_resp = requests.get(xbrl_url, headers=self.__header)
        xbrl_str = {self.__data_key_name: xbrl_resp.text}
        return xbrl_str

    def __get_cookie_info(self):
        response = requests.get(url=self.__cookie_url, headers=self.__header)
        cookie: dict = response.cookies.get_dict()
        self.__header["Cookie"] = f"nsit={cookie['nsit']}; nseappid={cookie['nseappid']}; ak_bmsc={cookie['ak_bmsc']}"

    def get_data_key_name(self) -> str:
        return self.__data_key_name
