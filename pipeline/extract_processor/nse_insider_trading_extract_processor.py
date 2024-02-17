from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from lib.lib import datetime, timezone, Path, parser
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
        orphan_cleaned_data has transaction data from the table, and it has XBRL file attached to but the transaction
        was not identified in the xbrl file. Therefore, these transactions will have a link to their XBRL file but
        will not have any data from it.
        :param primary_source_data_key_name: Used to deserialize data received in process_data function
        :param xbrl_downloader: Used to download XBRL file to current working directory
        :param xbrl_processor: Used to process data in XBRL file
        """
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[dict] = list()
        self.__changed_data: list[dict] = list()
        self.__insert_date: datetime = datetime.datetime.strptime(
                                                            datetime.datetime.now(timezone.utc).
                                                            strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')
        self.__xbrl_downloader: XBRLFileDownloaderInterface = xbrl_downloader
        self.__xbrl_processor: XBRLProcessorInterface = xbrl_processor
        self.__is_orphaned_data: bool = False
        self.__xbrl_folder_path: Path = Path()
        self.__xbrl_document_download_start_time: str = None
        self.__xbrl_document_download_end_time: str = None
        self.__config = {
                        "AcquisitionfromDate": self.__validate_date,
                        "AcquisitionToDate": self.__validate_date,
                        "IntimDate": self.__validate_date,
                        "BroadcastDateTime": self.__validate_date,
                        "Period": self.__validate_date,
                        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract": self.__validate_date,
                        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate": self.__validate_date,
                        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate": self.__validate_date,
                        "DateOfIntimationToCompany": self.__validate_date,
                        "AfterAcquisitionSharesPercentage": self.__validate_decimal,
                        "BeforeAcquisitionSharesPercentage": self.__validate_decimal,
                        "SecuritiesValue": self.__validate_decimal,
                        "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding": self.__validate_decimal,
                        "SecuritiesAcquiredOrDisposedValueOfSecurity": self.__validate_decimal,
                        "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding": self.__validate_decimal,
                        "BuyNotionalValue": self.__validate_decimal,
                        "NotionalValue": self.__validate_decimal,
                        "TotalValueInAggregate": self.__validate_decimal,
                        "BuyQuantity": self.__validate_integer,
                        "BuyValue": self.__validate_decimal,
                        "Did": self.__validate_integer,
                        "PID": self.__validate_integer,
                        "SellValue": self.__validate_decimal,
                        "NoOfSecurities": self.__validate_integer,
                        "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity": self.__validate_integer,
                        "SecuritiesAcquiredOrDisposedNumberOfSecurity": self.__validate_integer,
                        "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity": self.__validate_integer,
                        "BuyNumberOfUnits": self.__validate_integer,
                        "NumberOfUnits": self.__validate_integer,
                        "AcquisitionMode": self.__validate_string,
                        "AfterAcquisitionSharesNo": self.__validate_string,
                        "BeforeAcquisitionSharesNo": self.__validate_string,
                        "DerivativeType": self.__validate_string,
                        "Exchange": self.__validate_string,
                        "Remarks": self.__validate_string,
                        "SecuritiesTypePost": self.__validate_string,
                        "TDPDerivativeContractType": self.__validate_string,
                        "TKDAcqm": self.__validate_string,
                        "Symbol": self.__validate_string,
                        "CompanyName": self.__validate_string,
                        "Regulation": self.__validate_string,
                        "NameOfTheAcquirerORDisposer": self.__validate_string,
                        "TypeOfSecurity": self.__validate_string,
                        "AcquisitionORDisposal": self.__validate_string,
                        "XBRLLink": self.__validate_string,
                        "ScripCode": self.__validate_string,
                        "NSESymbol": self.__validate_string,
                        "MSEISymbol": self.__validate_string,
                        "NameOfTheCompany": self.__validate_string,
                        "WhetherISINAvailable": self.__validate_string,
                        "ISINCode": self.__validate_string,
                        "RevisedFilling": self.__validate_string,
                        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract": self.__validate_string,
                        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable": self.__validate_string,
                        "ChangeInHoldingOfSecuritiesOfPromotersAxis": self.__validate_string,
                        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems": self.__validate_string,
                        "TypeOfInstrument": self.__validate_string,
                        "TypeOfInstrumentOthers": self.__validate_string,
                        "CategoryOfPerson": self.__validate_string,
                        "NameOfThePerson": self.__validate_string,
                        "PANNumber": self.__validate_string,
                        "IdentificationNumberOfDirectorOrCompany": self.__validate_string,
                        "Address": self.__validate_string,
                        "ContactNumber": self.__validate_string,
                        "SecuritiesHeldPriorToAcquisitionOrDisposalAbstract": self.__validate_string,
                        "SecuritiesAcquiredOrDisposedAbstract": self.__validate_string,
                        "SecuritiesAcquiredOrDisposedTransactionType": self.__validate_string,
                        "SecuritiesHeldPostAcquistionOrDisposalAbstract": self.__validate_string,
                        "TypeOfContract": self.__validate_string,
                        "ContractSpecification": self.__validate_string,
                        "BuyAbstract": self.__validate_string,
                        "SellAbstract": self.__validate_string,
                        "ExchangeOnWhichTheTradeWasExecuted": self.__validate_string,
                        "NameOfTheSignatory": self.__validate_string,
                        "DesignationOfSignatory": self.__validate_string,
                        "Place": self.__validate_string,
                        "DateOfFiling": self.__validate_string,
                        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock": self.__validate_string,
                        "ModeOfAcquisitionOrDisposal": self.__validate_string,
                        "GeneralInformationAbstract": self.__validate_string,
                        "Currency": self.__validate_string,
                        "IsOrphan": self.__validate_string}
        self.__column_list_for_capturing_change_data: list[str] = [
                            "AcquisitionfromDate",
                            "AcquisitionToDate",
                            "AfterAcquisitionSharesPercentage",
                            "BeforeAcquisitionSharesPercentage",
                            "BuyQuantity",
                            "BuyValue",
                            "Did",
                            "IntimDate",
                            "PID",
                            "SecuritiesValue",
                            "SellValue",
                            "NoOfSecurities",
                            "BroadcastDateTime",
                            "Period",
                            "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
                            "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
                            "SecuritiesAcquiredOrDisposedNumberOfSecurity",
                            "SecuritiesAcquiredOrDisposedValueOfSecurity",
                            "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
                            "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
                            "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract",
                            "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate",
                            "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate",
                            "DateOfIntimationToCompany",
                            "BuyNotionalValue",
                            "BuyNumberOfUnits",
                            "NotionalValue",
                            "NumberOfUnits",
                            "TotalValueInAggregate",
                            "AcquisitionMode",
                            "AfterAcquisitionSharesNo",
                            "BeforeAcquisitionSharesNo",
                            "DerivativeType",
                            "Exchange",
                            "Remarks",
                            "SecuritiesTypePost",
                            "TDPDerivativeContractType",
                            "TKDAcqm",
                            "Symbol",
                            "CompanyName",
                            "Regulation",
                            "NameOfTheAcquirerORDisposer",
                            "TypeOfSecurity",
                            "AcquisitionORDisposal",
                            "XBRLLink",
                            "ScripCode",
                            "NSESymbol",
                            "MSEISymbol",
                            "NameOfTheCompany",
                            "WhetherISINAvailable",
                            "ISINCode",
                            "RevisedFilling",
                            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract",
                            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable",
                            "ChangeInHoldingOfSecuritiesOfPromotersAxis",
                            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems",
                            "TypeOfInstrument",
                            "TypeOfInstrumentOthers",
                            "CategoryOfPerson",
                            "NameOfThePerson",
                            "PANNumber",
                            "IdentificationNumberOfDirectorOrCompany",
                            "Address",
                            "ContactNumber",
                            "SecuritiesHeldPriorToAcquisitionOrDisposalAbstract",
                            "SecuritiesAcquiredOrDisposedAbstract",
                            "SecuritiesAcquiredOrDisposedTransactionType",
                            "SecuritiesHeldPostAcquistionOrDisposalAbstract",
                            "TypeOfContract",
                            "ContractSpecification",
                            "BuyAbstract",
                            "SellAbstract",
                            "ExchangeOnWhichTheTradeWasExecuted",
                            "NameOfTheSignatory",
                            "DesignationOfSignatory",
                            "Place",
                            "DateOfFiling",
                            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock",
                            "ModeOfAcquisitionOrDisposal",
                            "GeneralInformationAbstract",
                            "Currency",
                            "IsOrphan"]

    def get_cleaned_data(self) -> list[dict]:
        """
        Returns clean data
        :return: Returns clean data
        """
        return self.__cleaned_data

    def get_changed_data(self) -> list[dict]:
        return self.__changed_data

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
        4-  It will check if current transaction is orphan. If it is, it will flag it as Y, otherwise it will be
            flagged as "N",
        :return:
        """
        for item in self.__raw_data:
            self.__xbrl_processor.set_transaction_status_to_default()
            self.__handle_xbrl_transaction_status(dict_data=item)
            original_data: dict = self.__get_data(dict_data=item)
            updated_data: dict = self.__replace_invalid_value(dict_data=original_data)
            self.__capture_changed_data(original_data=original_data, updated_data=updated_data)
            self.__cleaned_data.append(updated_data)
        self.__set_xbrl_document_download_end_time()

    def __handle_xbrl_transaction_status(self, dict_data: dict) -> None:
        """
        Sets XBRL data status in __xbrl_processor. If there is not a link to XBRL file, then set is_xbrl_link_missing
        to True. If there is XBRL link, download the file. Check if there is data returned. If there is no data returned
        then again set is_xbrl_link_missing to True, otherwise find the transaction in XBRL file.
        :param dict_data: dictionary data
        :return: None
        """
        if dict_data["xbrl"] is None or dict_data["xbrl"] == "-":
            self.__xbrl_processor.set_xbrl_link_status(is_xbrl_link_missing=True)
            return

        self.__set_xbrl_document_download_start_time()

        self.__xbrl_downloader.download_xbrl_file_to_local_machine(dict_data["xbrl"],
                                                                   xbrl_folder_path=self.__xbrl_folder_path)
        if self.__xbrl_downloader.get_xbrl_data() is None:
            self.__xbrl_processor.set_xbrl_link_status(is_xbrl_link_missing=True)
            return
        self.__xbrl_processor.set_beautiful_soup(data=self.__xbrl_downloader.get_xbrl_data())
        self.__xbrl_processor.set_orphan_transaction_status(acqMode=dict_data.get("acqMode"),
                                                            secAcq=str(dict_data.get("secAcq")),
                                                            secType=dict_data.get("secType"),
                                                            secVal=str(dict_data.get("secVal")),
                                                            tdpTransactionType=dict_data.get(
                                                                "tdpTransactionType"),
                                                            befAcqSharesNo=str(dict_data.get("befAcqSharesNo")),
                                                            afterAcqSharesNo=str(
                                                                dict_data.get("afterAcqSharesNo")),
                                                            afterAcqSharesPer=str(
                                                                dict_data.get("afterAcqSharesPer")),
                                                            befAcqSharesPer=str(
                                                                dict_data.get("befAcqSharesPer")),
                                                            acqName=str(dict_data.get("acqName")))

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
            self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(tag_to_search="ISIN"))
        data["ISINCode"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(tag_to_search="ISIN")
        data["RevisedFilling"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="RevisedFilling")
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
                self.__xbrl_processor.process_general_xbrl_data(tag_name="TypeOfInstrumentOthers")
        data["TypeOfInstrument"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="TypeOfInstrument")
        data["CategoryOfPerson"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="CategoryOfPerson")
        data["NameOfThePerson"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="NameOfThePerson")
        data["PANNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="PANNumber")
        data["IdentificationNumberOfDirectorOrCompany"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="IdentificationNumberOfDirectorOrCompany")
        data["Address"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="Address")
        data["ContactNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="ContactNumber")
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
                self.__xbrl_processor.process_general_xbrl_data(tag_name="SecuritiesAcquiredOrDisposedAbstract")
        data["SecuritiesAcquiredOrDisposedNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="SecuritiesAcquiredOrDisposedNumberOfSecurity")
        data["SecuritiesAcquiredOrDisposedValueOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="SecuritiesAcquiredOrDisposedValueOfSecurity")
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
        data["DateOfIntimationToCompany"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="DateOfIntimationToCompany")
        data["TypeOfContract"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="TypeOfContract")
        data["ContractSpecification"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="ContractSpecification")
        data["BuyAbstract"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="BuyAbstract")
        data["BuyNotionalValue"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="BuyNotionalValue")
        data["BuyNumberOfUnits"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="BuyNumberOfUnits")
        data["SellAbstract"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="SellAbstract")
        data["NotionalValue"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="NotionalValue")
        data["NumberOfUnits"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="NumberOfUnits")
        data["ExchangeOnWhichTheTradeWasExecuted"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="ExchangeOnWhichTheTradeWasExecuted")
        data["TotalValueInAggregate"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="ValueInAggregate")
        data["NameOfTheSignatory"] =self.__xbrl_processor.process_general_xbrl_data(tag_name="NameOfTheSignatory")
        data["DesignationOfSignatory"] = self.__xbrl_processor.process_general_xbrl_data(
            tag_name="DesignationOfSignatory")
        data["Place"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="Place")
        data["DateOfFiling"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="DateOfFiling")
        data["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock")
        data["ModeOfAcquisitionOrDisposal"] = \
                self.__xbrl_processor.process_general_xbrl_data(tag_name="ModeOfAcquisitionOrDisposal")
        data["GeneralInformationAbstract"] = self.__xbrl_processor.process_general_xbrl_data(
            tag_name="GeneralInformationAbstract")
        data["Currency"] = self.__xbrl_processor.process_general_xbrl_data(tag_name="Currency")
        data["IsOrphan"] = self.__get_is_orphan_info()
        data["DownloadDate"] = str(self.__insert_date)
        return data

    def __replace_invalid_value(self, dict_data: dict) -> dict:
        """
        Replaces invalid values such as "-" with None. This transformation makes it easier to process data in SQL.
        It first creates temporary dictionary from the dict_data passed as its argument. This is necessary because
        we do not want to update original dictionary values. Then It compares the keys against config to find fields
        that need to be validated. When all fields are compared against config, it returns the new dictionary with
        updated values
        :param dict_data: Dictionary data
        :return: dictionary
        """
        temp_dict = dict()
        for key, value in dict_data.items():
            temp_dict[key] = value

        for key_data, value_data in temp_dict.items():
            for key_config, value_config in self.__config.items():
                if key_data != key_config:
                    continue
                temp_dict[key_data] = value_config(value_data)
                break
        return temp_dict

    def __capture_changed_data(self, original_data: dict, updated_data: dict) -> None:
        """
        Captures changed data by comparing original data against updated data.
        Note: There might be empty string replaced by None in the data. So this will also be considered data alteration.
        ('' <> None)
        :param original_data: dictionary
        :param updated_data:
        :return: None
        """
        for column_name in self.__column_list_for_capturing_change_data:
            if original_data[column_name] == updated_data[column_name]:
                continue

            self.__changed_data.append({
                "did": original_data["Did"],
                "field_name": column_name,
                "from_value": str(original_data[column_name]),
                "to_value": str(updated_data[column_name])})

    def __get_is_orphan_info(self) -> str:
        if self.__xbrl_processor.get_orphan_transaction_status():
            return "Y"
        return "N"

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

    @staticmethod
    def __validate_date(date_string: str) -> str:
        if date_string is None:
            return "-"
        try:
            if bool(parser.parse(date_string)):
                return str(date_string)
        except ValueError:
            return "-"

    @staticmethod
    def __validate_integer(number_string: str) -> str:
        if number_string is None:
            return "-"
        if number_string.isdigit():
            return str(number_string)
        return "-"

    @staticmethod
    def __validate_decimal(number_string: str) -> str:
        """
        Validates whether the number is a decimal or integer data type. It adds 1 to the value to avoid False if the value
        is 0.00 (if value == 0.00 statement returns False but if the value is different than that, then it returns True)
        :param number_string:
        :return:
        """
        if number_string is None:
            return "-"
        try:
            converted_to_float = float(number_string) + 1
            if converted_to_float or number_string.isdigit():
                return str(number_string)
        except ValueError as e:
            return "-"

    @staticmethod
    def __validate_string(string_data: str) -> str:
        if string_data is None:
            return "-"
        return str(string_data)

    def __set_xbrl_document_download_start_time(self) -> None:
        if self.__xbrl_document_download_start_time is None:
            self.__xbrl_document_download_start_time = (
                datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    def __set_xbrl_document_download_end_time(self) -> None:
        if self.__xbrl_document_download_start_time is not None:
            self.__xbrl_document_download_end_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def get_xbrl_document_download_start_time(self) -> str:
        return self.__xbrl_document_download_start_time

    def get_xbrl_document_download_end_time(self) -> str:
        return self.__xbrl_document_download_end_time

    def get_xbrl_document_page_visit_attempt_count(self) -> int:
        return self.__xbrl_downloader.get_xbrl_document_page_visit_attempt_count()

    def get_xbrl_document_download_success_count(self) -> int:
        return self.__xbrl_downloader.get_xbrl_document_download_success_count()

    def get_xbrl_document_download_error_count(self) -> int:
        return self.__xbrl_downloader.get_xbrl_document_download_error_count()
