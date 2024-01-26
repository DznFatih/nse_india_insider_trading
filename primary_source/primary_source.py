from abc import abstractmethod, ABC
from lib.lib import requests

class PrimarySource(ABC):

    @abstractmethod
    def get_data(self) -> dict:
        pass


class HTTPRequestPrimarySource(PrimarySource):

    def __init__(self, from_date: str=None, to_date: str=None, default_date: str = None) -> None:
        self.__date_format: str = 'DD-MM-YYYY'
        self.__from_date: str = from_date
        self.__to_date: str = to_date
        self.__default_date: str =
        self.__nse_url: str = f'https://www.nseindia.com/api/corporates-pit?index=equities&'

        self.__header = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9,tr;q=0.8,lt;q=0.7',
                'Cookie': 'defaultLang=en; _ga=GA1.1.620876033.1706008161; nsit=LfNxk9YHV80Eh0XBmI6Yzb1N; AKA_A2=A; ak_bmsc=DD3820098E6939A895A07E7CEB8C9AD6~000000000000000000000000000000~YAAQDx0SAlmuJD6NAQAAxACeRBYCLpWOmEpWkGbBun6ijvBgK2lgxKlLFWcG5DjBbOIwM9JQQ+gNiGs3yud1j7+B6dg6TTE6yBNa3pW2GevLrIeeGlXAyxHQxoA3Q7IwfmpRVpbMz/9hKb2Amc3motkqMOQrfOmMeh20GR5XCLJuvxpNwli50n/PVkvkdt6T+a6iDVOQGUCmaNJpP2cWjst1/MLEK3bmRha2MMdtgBsPz51DSPld/B4TVjdsjY3T8wIDOHfPcXOhsvAqvFhEpHC18scfoPKHmb6AEZaVoGM+vWqzdXWxuWyqWtnfcYWT76xQm9tNRj/CK0gsw7+SiQzfLSEEW/D/u5/JoTD3tN0RozT4TMAMLlcHx+9hlbw9kYmSv2HvCPeg3ujMgZApg17K62BXTF9SX17+F8LIlpwLYCuRuGhAlqk55559u4P+EcB1y2BIjfJegmadfSxt6XME+Gxjj0ShD1wtEfcP44vp7lDGUPV+TjmqfGdrzw==; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcwNjI1MzU5NywiZXhwIjoxNzA2MjYwNzk3fQ.57NZdb01eRhWLeIBAtXzfC86B-Dwu1i3_-hI7TX0-bg; RT="sl=1&ss=lrubbdsa&tt=15c&bcn=%2F%2F684dd327.akstat.io%2F&z=1&dm=nseindia.com&si=ab32e650-12cd-4405-b0ce-f84659e569e5&se=8c&ld=u2w&nu=kpaxjfo&cl=1z0e"; _ga_QJZ4447QD3=GS1.1.1706253218.5.1.1706253653.0.0.0; _ga_87M7PJ3R97=GS1.1.1706253218.8.1.1706253653.0.0.0; bm_sv=CD45A39145887860B95CFB018EC139D9~YAAQDx0SAs4kJT6NAQAA2OikRBa1J2SXViUUAIO2FU5oLaF9qysOtNEIp9AlTBf6Qofz4raaST0QSBCgIuLFs0liGyuyrmWDF1pKW+ni1196wKyQKHazRkLupMibGYWcOIJ9csXOsGicde1k6IGf+N/V6PK7fhQt5y/RbwKGeyUIufNOGO/E1s/6RGd8HvJS2aWi+1E8aIMbADwRs8hoZlE1wLWcXdsiyBHTqhwZKO3GylVhEWOPopYU/l96ZpeqvrHU~1',
                'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }


    def get_data(self) -> dict:
        try:

            response = requests.get(url=self.__nse_url, headers=header_, proxies=proxy, timeout=15)

            return {"content": "successful", "data": response.json()}
        except Exception as e:
            return {"content": "fail", "proxy": f"{proxy} is NOT working", "error_message": str(e)}

    def __construct_nse_url(self) -> None:
        # self.__nse_url = 'https://www.nseindia.com/api/corporates-pit?index=equities&from_date=06-05-2018&to_date=06-05-2018'