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
            data: dict = dict()
            if item["xbrl"] is None or item["xbrl"] == "-":
                data = self.__get_data_without_xbrl_data(item)
                self.__xbrl_processor.set_orphan_transaction_status_by_contact_person(set_value=False)
            elif item["acqName"] is not None and item["acqName"] != "-":
                self.__xbrl_processor.set_orphan_transaction_status_by_contact_person(set_value=False)
                data = self.__get_data_by_available_fields(dict_data=item)
            else:
                self.__xbrl_processor.set_orphan_transaction_status(
                                                                    type_of_security=item["secType"],
                                                                    number_of_securities=item["secAcq"],
                                                                    acquisition_disposal=item["tdpTransactionType"])
                data = self.__get_data_by_available_fields(dict_data=item)

            if self.__xbrl_processor.get_orphan_transaction_status():
                self.__orphan_cleaned_data.append(data)
            else:
                self.__cleaned_data.append(data)

    def __get_data_by_available_fields(self, dict_data: dict) -> dict:
        table_rows: dict = self.__get_table_rows(dict_data=dict_data)
        self.__xbrl_downloader.download_xbrl_file_to_local_machine(xbrl_url=dict_data["xbrl"],
                                                                   xbrl_folder_path=self.__xbrl_folder_path)

        table_rows["Period"] = self.__xbrl_processor.process_xbrl_data_to_get_context_info_by_contact_person_name(
                contact_person_name=dict_data["acqName"],
                parent_tag_name="context",
                child_tag_name="period",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ScripCode"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="BSEScripCode",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NSESymbol"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NSESymbol",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["MSEISymbol"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="MSEISymbol",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NameOfTheCompany"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NameOfTheCompany",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["WhetherISINAvailable"] = self.__check_if_isin_data_available(
            self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN",
                data=self.__xbrl_downloader.get_xbrl_data()))
        table_rows["ISINCode"] = self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["RevisedFilling"] = self.__xbrl_processor.process_general_xbrl_data(
                contact_person_name=dict_data["acqName"],
                tag_name="RevisedFilling",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ChangeInHoldingOfSecuritiesOfPromotersAxis"] = self.__xbrl_processor.process_general_xbrl_data(
                contact_person_name=dict_data["acqName"],
                tag_name="ChangeInHoldingOfSecuritiesOfPromotersAxis",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["TypeOfInstrumentOthers"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="TypeOfInstrumentOthers",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["TypeOfInstrument"] = \
            self.__xbrl_processor.process_general_xbrl_data(
                contact_person_name=dict_data["acqName"],
                tag_name="TypeOfInstrument",
                data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["CategoryOfPerson"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="CategoryOfPerson",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NameOfThePerson"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="NameOfThePerson",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["PANNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="PANNumber",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["IdentificationNumberOfDirectorOrCompany"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="IdentificationNumberOfDirectorOrCompany",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["Address"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="Address",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ContactNumber"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="ContactNumber",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesAcquiredOrDisposedAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesAcquiredOrDisposedAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesAcquiredOrDisposedNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesAcquiredOrDisposedNumberOfSecurity",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesAcquiredOrDisposedValueOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesAcquiredOrDisposedValueOfSecurity",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesAcquiredOrDisposedTransactionType"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesAcquiredOrDisposedTransactionType",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPostAcquistionOrDisposalAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DateOfIntimationToCompany"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DateOfIntimationToCompany",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["TypeOfContract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="TypeOfContract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ContractSpecification"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="ContractSpecification",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["BuyAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="BuyAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["BuyNotionalValue"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="BuyNotionalValue",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["BuyNumberOfUnits"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="BuyNumberOfUnits",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["SellAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="SellAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NotionalValue"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="NotionalValue",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NumberOfUnits"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="NumberOfUnits",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ExchangeOnWhichTheTradeWasExecuted"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="ExchangeOnWhichTheTradeWasExecuted",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["TotalValueInAggregate"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="ValueInAggregate",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["NameOfTheSignatory"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="NameOfTheSignatory",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DesignationOfSignatory"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DesignationOfSignatory",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["Place"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="Place",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DateOfFiling"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DateOfFiling",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["ModeOfAcquisitionOrDisposal"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="ModeOfAcquisitionOrDisposal",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["GeneralInformationAbstract"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="GeneralInformationAbstract",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["Currency"] = \
                self.__xbrl_processor.process_general_xbrl_data(
                    contact_person_name=dict_data["acqName"],
                    tag_name="Currency",
                    data=self.__xbrl_downloader.get_xbrl_data())
        table_rows["DownloadDate"] = self.__insert_date
        return table_rows

    @staticmethod
    def __check_if_isin_data_available(isin_data: str) -> str:
        if isin_data:
            return "Y"
        return "N"

    def __get_data_without_xbrl_data(self, dict_data: dict) -> dict:
        table_rows: dict = self.__get_table_rows(dict_data=dict_data)
        table_rows["Period"] = None
        table_rows["ScripCode"] = None
        table_rows["NSESymbol"] = None
        table_rows["MSEISymbol"] = None
        table_rows["NameOfTheCompany"] = None
        table_rows["WhetherISINAvailable"] = None
        table_rows["ISINCode"] = None
        table_rows["RevisedFilling"] = None
        table_rows[
            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract"] = None
        table_rows[
            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable"] = None
        table_rows["ChangeInHoldingOfSecuritiesOfPromotersAxis"] = None
        table_rows[
            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems"] = None
        table_rows["TypeOfInstrument"] = None
        table_rows["TypeOfInstrumentOthers"] = None
        table_rows["CategoryOfPerson"] = None
        table_rows["NameOfThePerson"] = None
        table_rows["PANNumber"] = None
        table_rows["IdentificationNumberOfDirectorOrCompany"] = None
        table_rows["Address"] = None
        table_rows["ContactNumber"] = None
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalAbstract"] = None
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity"] = None
        table_rows["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"] = None
        table_rows["SecuritiesAcquiredOrDisposedAbstract"] = None
        table_rows["SecuritiesAcquiredOrDisposedNumberOfSecurity"] = None
        table_rows["SecuritiesAcquiredOrDisposedValueOfSecurity"] = None
        table_rows["SecuritiesAcquiredOrDisposedTransactionType"] = None
        table_rows["SecuritiesHeldPostAcquistionOrDisposalAbstract"] = None
        table_rows["SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity"] = None
        table_rows["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"] = None
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract"] = None
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate"] = None
        table_rows["DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate"] = None
        table_rows["DateOfIntimationToCompany"] = None
        table_rows["TypeOfContract"] = None
        table_rows["ContractSpecification"] = None
        table_rows["BuyAbstract"] = None
        table_rows["BuyNotionalValue"] = None
        table_rows["BuyNumberOfUnits"] = None
        table_rows["SellAbstract"] = None
        table_rows["NotionalValue"] = None
        table_rows["NumberOfUnits"] = None
        table_rows["ExchangeOnWhichTheTradeWasExecuted"] = None
        table_rows["TotalValueInAggregate"] = None
        table_rows["NameOfTheSignatory"] = None
        table_rows["DesignationOfSignatory"] = None
        table_rows["Place"] = None
        table_rows["DateOfFiling"] = None
        table_rows[
            "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock"] = None
        table_rows["ModeOfAcquisitionOrDisposal"] = None
        table_rows["GeneralInformationAbstract"] = None
        table_rows["Currency"] = None
        table_rows["DownloadDate"] = self.__insert_date
        return table_rows

    @staticmethod
    def __get_table_rows(dict_data: dict) -> dict:
        return {
            "Symbol": dict_data["symbol"],
            "CompanyName": dict_data["company"],
            "Regulation": dict_data["anex"],
            "NameOfTheAcquirerORDisposer": dict_data["acqName"],
            "TypeOfSecurity": dict_data["secType"],
            "NoOfSecurities": dict_data["secAcq"],
            "AcquisitionORDisposal": dict_data["tdpTransactionType"],
            "BroadcastDateTime": dict_data["date"],
            "XBRLLink": dict_data["xbrl"]
        }
