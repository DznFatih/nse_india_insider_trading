from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from lib.lib import datetime, timezone, Path
from pipeline_manager.error_info.error_logger import get_original_error_message
from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from pipeline_manager.xbrl_processor_interface.xbrl_processor_interface import XBRLProcessorABC


class NSEIndiaInsiderTradingExtractProcessor(EntityProcessor):

    def __init__(self, primary_source_data_key_name: str, xbrl_downloader: XBRLFileDownloaderABC,
                 xbrl_processor: XBRLProcessorABC):
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[dict] = list()
        self.__orphan_cleaned_data: list[dict] = list()
        self.__insert_date: datetime = datetime.datetime.strptime(
                                                            datetime.datetime.now(timezone.utc).
                                                            strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')
        self.__xbrl_downloader: XBRLFileDownloaderABC = xbrl_downloader
        self.__xbrl_processor: XBRLProcessorABC = xbrl_processor
        self.__is_orphaned_data: bool = False
        self.__xbrl_folder_path: Path = Path()

    def get_cleaned_data(self) -> list[dict]:
        return self.__cleaned_data

    def get_orphan_cleaned_data(self) -> list[dict]:
        return self.__orphan_cleaned_data

    def get_cleaned_row_count(self) -> int:
        return len(self.__cleaned_data)

    def process_data(self, raw_data: dict, xbrl_folder_path: Path) -> None:
        try:
            self.__xbrl_folder_path = xbrl_folder_path
            self.__unload_data(raw_data)
            self.__process_data()
        except TypeError as e:
            raise TypeError(get_original_error_message(e))
        except KeyError as e:
            raise KeyError(get_original_error_message(e))
        except ValueError as e:
            raise ValueError(get_original_error_message(e))
        except Exception as e:
            raise Exception(get_original_error_message(e))

    def __unload_data(self, raw_data):
        self.__raw_data = raw_data[self.__primary_source_data_key_name]

    def __process_data(self) -> None:
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
        if isin_data:
            return "Y"
        return "N"
