from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from lib.lib import datetime, timezone, Path
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.xbrl_downloader.xbrl_file_downloader_interface import XBRLFileDownloaderInterface
from pipeline_manager.xbrl_processor.xbrl_processor_interface import XBRLProcessorInterface


class NSEIndiaInsiderTradingExtractProcessor(EntityProcessor):
    """
    Data processor for NSEIndiaInsiderTrading website. This class is only responsible for processing
    the data. It does not care where or how it gets it, the data needs to be passed in to its function process_data.
    It also asks XBRLFileDownloaderInterface to download XBRL documents from internet and asks XBRLProcessorInterface
    to process XBRL document data.
    """
    def __init__(self, primary_source_data_key_name: str, xbrl_downloader: XBRLFileDownloaderInterface,
                 xbrl_processor: XBRLProcessorInterface):
        """
        Collects data and splits them into two files: cleaned_data and orphan_cleaned_data.
        cleaned_data has transaction data from the table visible in webpage and data from XBRL file.
        orphan_cleaned_data has transaction data from the table and it has XBRL file attached to but the transaction
        was not identified in the xbrl file. Therefore, these transactions will have a link to their XBRL file but
        will not have any data from it.
        :param primary_source_data_key_name: Used to deserialize data received in process_data function
        :param xbrl_downloader: Used to download XBRL file to current working directory
        :param xbrl_processor: Used to process data in XBRL file
        """
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[dict] = list()
        self.__orphan_cleaned_data: list[dict] = list()
        self.__insert_date: datetime = datetime.datetime.strptime(
                                                            datetime.datetime.now(timezone.utc).
                                                            strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')
        self.__xbrl_downloader: XBRLFileDownloaderInterface = xbrl_downloader
        self.__xbrl_processor: XBRLProcessorInterface = xbrl_processor
        self.__is_orphaned_data: bool = False
        self.__xbrl_folder_path: Path = Path()

    def get_cleaned_data(self) -> list[dict]:
        """
        Returns clean data
        :return: Returns clean data
        """
        return self.__cleaned_data

    def get_orphan_cleaned_data(self) -> list[dict]:
        """
        Orphan data means transaction has XBRL file but it was not identified in the file by any
        means.
        :return:
        """
        return self.__orphan_cleaned_data

    def get_cleaned_row_count(self) -> int:
        return len(self.__cleaned_data)

    def process_data(self, raw_data: dict, xbrl_folder_path: Path) -> None:
        """
        Process data received as dictionary and makes call to other operations
        :param raw_data: serialized raw data
        :param xbrl_folder_path: path to folder created for storing XBRL file
        :return:
        """
        try:
            self.__xbrl_folder_path = xbrl_folder_path
            self.__unload_data(raw_data)
            self.__process_data()
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

    def __unload_data(self, raw_data: dict) -> None:
        """
        Deserializes raw_data using data key name
        :param raw_data: dictionary data
        :return:
        """
        self.__raw_data = raw_data[self.__primary_source_data_key_name]

    def __process_data(self) -> None:
        """
        Loops through raw data, downloads XBRL files, stores files in the working directory, and processes data from
        xbrl files. Transactions made before May, 3rd 2018 does not have XBRL files so data pulled before that day
        will not have any XBRL files. Logic in this method step by step as follows:

        1- Set transaction status to default which is False. XBRL file processor object checks to identify transactions
           in XBRL file. If it does not find, it sets is_orphaned_transaction to False otherwise to True.
           IF it is True, then it means we were able to identify transaction in XBRL file.
        2- It checks if the transaction has link to XBRL file. If it does not have the link,
           it will set __is_xbrl_link_missing to True so that processor will not search anything
        3- If there is a link to XBRL file, then

            3.1 It will download XBRL file,
            3.2 it will ask XBRL file processor to identify currently processing transaction
            3.3 If transaction found in the file, then it will set is_orphan_transaction status to false
            3.4 It will make a call to its method __get_data to start processing current transaction
        4-  It will check if current transaction is orphan. If it is, it will store it to __orphan_cleaned_data,
            otherwise it will store to __cleaned_data
        :return:
        """
        for item in self.__raw_data:
            self.__xbrl_processor.set_transaction_status_to_default()
            if item["xbrl"] is None or item["xbrl"] == "-":
                self.__xbrl_processor.set_xbrl_link_status(True)
                data: dict = self.__get_data(dict_data=item)
            else:
                self.__xbrl_downloader.download_xbrl_file_to_local_machine(xbrl_url=item["xbrl"],
                                                                           xbrl_folder_path=self.__xbrl_folder_path)
                self.__xbrl_processor.set_beautiful_soup(data=self.__xbrl_downloader.get_xbrl_data())
                self.__xbrl_processor.set_orphan_transaction_status(acqMode=item["acqMode"], secAcq=str(item["secAcq"]),
                                                                    secType=item["secType"], secVal=str(item["secVal"]),
                                                                    tdpTransactionType=item["tdpTransactionType"],
                                                                    befAcqSharesNo=str(item["befAcqSharesNo"]),
                                                                    afterAcqSharesNo=str(item["afterAcqSharesNo"]),
                                                                    afterAcqSharesPer=str(item["afterAcqSharesPer"]),
                                                                    befAcqSharesPer=str(item["befAcqSharesPer"]),
                                                                    acqName=str(item["acqName"]))
                data: dict = self.__get_data(dict_data=item)

            if self.__xbrl_processor.get_orphan_transaction_status():
                self.__orphan_cleaned_data.append(data)
            else:
                self.__cleaned_data.append(data)

    def __get_data(self, dict_data: dict) -> dict:
        """
        Processes data passed through its argument dict_data
        :param dict_data: dictionary data that holds current transaction
        :return: returns currently processed transaction data
        """
        data: dict = dict()

        data["AcquisitionMode"] = dict_data.get("acqMode")
        data["AcquisitionfromDate"] = dict_data.get("acqfromDt")
        data["AcquisitionToDate"] = dict_data.get("acqtoDt")
        data["AfterAcquisitionSharesNo"] = dict_data.get("afterAcqSharesNo")
        data["AfterAcquisitionSharesPercentage"] = dict_data.get("afterAcqSharesPer")
        data["BeforeAcquisitionSharesNo"] = dict_data.get("befAcqSharesNo")
        data["BeforeAcquisitionSharesPercentage"] = dict_data.get("befAcqSharesPer")
        data["BuyQuantity"] = dict_data.get("buyQuantity")
        data["BuyValue"] = dict_data.get("buyValue")
        data["DerivativeType"] = dict_data.get("derivativeType")
        data["Did"] = dict_data.get("did")
        data["Exchange"] = dict_data.get("exchange")
        data["IntimDate"] = dict_data.get("intimDt")
        data["PID"] = dict_data.get("pid")
        data["Remarks"] = dict_data.get("remarks")
        data["SecuritiesValue"] = dict_data.get("secVal")
        data["SecuritiesTypePost"] = dict_data.get("securitiesTypePost")
        data["SellValue"] = dict_data.get("sellValue")
        data["TDPDerivativeContractType"] = dict_data.get("tdpDerivativeContractType")
        data["TKDAcqm"] = dict_data.get("tkdAcqm")
        data["Symbol"] = dict_data.get("symbol")
        data["CompanyName"] = dict_data.get("company")
        data["Regulation"] = dict_data.get("anex")
        data["NameOfTheAcquirerORDisposer"] = dict_data.get("acqName")
        data["TypeOfSecurity"] = dict_data.get("secType")
        data["NoOfSecurities"] = dict_data.get("secAcq")
        data["AcquisitionORDisposal"] = dict_data.get("tdpTransactionType")
        data["BroadcastDateTime"] = dict_data.get("date")
        data["XBRLLink"] = dict_data.get("xbrl")
        data["Period"] = self.__xbrl_processor.process_xbrl_data_to_get_context_info(
                parent_tag_name="context",
                child_tag_name="period")
        data["ScripCode"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="BSEScripCode")
        data["NSESymbol"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NSESymbol")
        data["MSEISymbol"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="MSEISymbol")
        data["NameOfTheCompany"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NameOfTheCompany")
        data["WhetherISINAvailable"] = self.__check_if_isin_data_available(
            self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN"))
        data["ISINCode"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN")
        data["RevisedFilling"] = self.__xbrl_processor.process_general_xbrl_data(
                tag_name="RevisedFilling")
        data["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract")
        data["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable")
        data["ChangeInHoldingOfSecuritiesOfPromotersAxis"] = self.__xbrl_processor.process_general_xbrl_data(
                tag_name="ChangeInHoldingOfSecuritiesOfPromotersAxis")
        data["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems")
        data["TypeOfInstrumentOthers"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="TypeOfInstrumentOthers")
        data["TypeOfInstrument"] = \
            self.__xbrl_processor.process_general_xbrl_data(
                tag_name="TypeOfInstrument")
        data["CategoryOfPerson"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="CategoryOfPerson")
        data["NameOfThePerson"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="NameOfThePerson")
        data["PANNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="PANNumber")
        data["IdentificationNumberOfDirectorOrCompany"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="IdentificationNumberOfDirectorOrCompany")
        data["Address"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="Address")
        data["ContactNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="ContactNumber")
        data["SecuritiesHeldPriorToAcquisitionOrDisposalAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalAbstract")
        data["SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity")
        data["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding")
        data["SecuritiesAcquiredOrDisposedAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesAcquiredOrDisposedAbstract")
        data["SecuritiesAcquiredOrDisposedNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesAcquiredOrDisposedNumberOfSecurity")
        data["SecuritiesAcquiredOrDisposedValueOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesAcquiredOrDisposedValueOfSecurity")
        data["SecuritiesAcquiredOrDisposedTransactionType"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesAcquiredOrDisposedTransactionType")
        data["SecuritiesHeldPostAcquistionOrDisposalAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalAbstract")
        data["SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity")
        data["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding")
        data["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract")
        data["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate")
        data["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate")
        data["DateOfIntimationToCompany"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DateOfIntimationToCompany")
        data["TypeOfContract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="TypeOfContract")
        data["ContractSpecification"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="ContractSpecification")
        data["BuyAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="BuyAbstract")
        data["BuyNotionalValue"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="BuyNotionalValue")
        data["BuyNumberOfUnits"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="BuyNumberOfUnits")
        data["SellAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="SellAbstract")
        data["NotionalValue"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="NotionalValue")
        data["NumberOfUnits"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="NumberOfUnits")
        data["ExchangeOnWhichTheTradeWasExecuted"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="ExchangeOnWhichTheTradeWasExecuted")
        data["TotalValueInAggregate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="ValueInAggregate")
        data["NameOfTheSignatory"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="NameOfTheSignatory")
        data["DesignationOfSignatory"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DesignationOfSignatory")
        data["Place"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="Place")
        data["DateOfFiling"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DateOfFiling")
        data["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock")
        data["ModeOfAcquisitionOrDisposal"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="ModeOfAcquisitionOrDisposal")
        data["GeneralInformationAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="GeneralInformationAbstract")
        data["Currency"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="Currency")
        data["DownloadDate"] = self.__insert_date
        return data

    @staticmethod
    def __check_if_isin_data_available(isin_data: str) -> str:
        """
        checks to see if isin_data has any value
        :param isin_data: string data
        :return: Returns string
        """
        if isin_data:
            return "Y"
        return "N"
