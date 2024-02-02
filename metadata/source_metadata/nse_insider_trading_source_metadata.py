from metadata.source_metadata.source_http_metadata_interface import SourceHTTPMetadata


class NSEIndiaInsiderTradingSourceMetadata(SourceHTTPMetadata):
    """ Class to hold hard coded metadata for NSEIndia.com website. This is purely a data class
    and does not have any operations. """
    def __init__(self):
        self.__base_url: str = 'https://www.nseindia.com/api/corporates-pit?index=equities&'
        self.__header = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,tr;q=0.8,lt;q=0.7',
            'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        self.__cookie_url: str = 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading'

    def get_base_url(self) -> str:
        return self.__base_url

    def get_header(self) -> dict:
        return self.__header

    def get_cookie_url(self) -> str:
        return self.__cookie_url
