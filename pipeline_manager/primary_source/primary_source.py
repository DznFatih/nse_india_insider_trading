from requests import models
from lib.lib import requests, date, timedelta
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.primary_source.primary_source_interface import PrimarySource, XBRLPrimarySource


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
        try:
            self.__construct_nse_url()
            self.__construct_header_with_cookie()
            response = requests.get(url=self.__nse_url, headers=self.__header, timeout=10)
            return response.json()["data"]
        except requests.HTTPError as e:
            raise requests.HTTPError(get_error_details(e))
        except requests.ConnectionError as e:
            raise requests.ConnectionError(get_error_details(e))
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

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


class NSEIndiaHTTPXBRLFilePrimarySource(XBRLPrimarySource):
    __cookie_info: dict = dict()

    def __init__(self, header: dict, cookie_url: str, base_url: str, data_key_name: str):
        self.__header: dict = header
        self.__cookie_url: str = cookie_url
        self.__base_url: str = base_url
        self.__data_key_name: str = data_key_name

    def get_data(self, xbrl_url: str) -> models.Response:
        try:
            self.__get_cookie_info()
            xbrl_resp = requests.get(xbrl_url, headers=self.__header)
            return xbrl_resp
        except requests.HTTPError as e:
            raise requests.HTTPError(get_error_details(e))
        except requests.ConnectionError as e:
            raise requests.ConnectionError(get_error_details(e))
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

    def __get_cookie_info(self):
        if NSEIndiaHTTPXBRLFilePrimarySource.__cookie_info is None:
            response = requests.get(url=self.__cookie_url, headers=self.__header)
            NSEIndiaHTTPXBRLFilePrimarySource.__cookie_info = response.cookies.get_dict()
            self.__header["Cookie"] = (f"nsit={NSEIndiaHTTPXBRLFilePrimarySource.__cookie_info['nsit']}; "
                                       f"nseappid={NSEIndiaHTTPXBRLFilePrimarySource.__cookie_info['nseappid']}; "
                                       f"ak_bmsc={NSEIndiaHTTPXBRLFilePrimarySource.__cookie_info['ak_bmsc']}")
