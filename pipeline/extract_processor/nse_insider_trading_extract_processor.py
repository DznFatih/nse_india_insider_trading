from pipeline_manager.entity_processor.entity_processor import EntityProcessor
from lib.lib import datetime, timezone
from pipeline_manager.xbrl_file_downloader_interface.xbrl_file_downloader_interface import XBRLFileDownloaderABC
from pipeline_manager.xbrl_processor_interface.xbrl_processor_interface import XBRLProcessorABC


class NSEIndiaInsiderTradingExtractProcessor(EntityProcessor):

    def __init__(self, primary_source_data_key_name: str, source_system: str, xbrl_downloader: XBRLFileDownloaderABC,
                 xbrl_processor: XBRLProcessorABC):
        self.__primary_source_data_key_name: str = primary_source_data_key_name
        self.__raw_data: list[dict] = list()
        self.__cleaned_data: list[dict] = list()
        self.__orphan_cleaned_data: list[dict] = list()
        self.__source_system: str = source_system
        self.__insert_date: datetime = datetime.datetime.strptime(
                                                            datetime.datetime.now(timezone.utc).
                                                            strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S')
        self.__xbrl_downloader: XBRLFileDownloaderABC = xbrl_downloader
        self.__xbrl_processor: XBRLProcessorABC = xbrl_processor
        self.__is_orphaned_data: bool = False

    def get_cleaned_data(self) -> list[dict]:
        return self.__cleaned_data

    def get_orphan_cleaned_data(self) -> list[dict]:
        return self.__orphan_cleaned_data

    def get_cleaned_row_count(self) -> int:
        return len(self.__cleaned_data)

    def process_data(self, raw_data: dict) -> None:
        self.__unload_data(raw_data)
        self.__xbrl_downloader.create_xbrl_folder()
        self.__process_data()

    def __unload_data(self, raw_data):
        self.__raw_data = raw_data[self.__primary_source_data_key_name]

    def __process_data(self) -> None:
        for item in self.__raw_data:
            data: dict = dict()
            self.__is_orphaned_data = False

            if item["xbrl"] == "-":
                data: dict = self.__get_data_without_xbrl_data(item)
            elif item["acqName"] != "-":
                data: dict = self.__get_data_by_name_of_contact_person(item)
            elif item["secType"] != "-" and item["secAcq"] != "-" and item["tdpTransactionType"] != "-":
                data: dict = self.__get_data_by_other_means(item)
            else:
                self.__is_orphaned_data = True
                data: dict = self.__get_data_without_xbrl_data(item)

            if self.__is_orphaned_data:
                self.__orphan_cleaned_data.append(data)
            else:
                self.__cleaned_data.append(data)

    def __get_data_by_name_of_contact_person(self, dict_data: dict) -> dict:
        table_rows: dict = self.__get_table_rows(dict_data=dict_data)
        self.__xbrl_downloader.download_xbrl_file_to_local_machine(dict_data["xbrl"])
        return {
            "Period": self.__xbrl_processor.process_xbrl_data_to_get_context_info_by_contact_person_name(
                contact_person_name=dict_data["acqName"],
                parent_tag_name="context",
                child_tag_name="period",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "ScripCode": self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="BSEScripCode",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "NSESymbol": self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NSESymbol",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "MSEISymbol": self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="MSEISymbol",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "NameOfTheCompany": self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="NameOfTheCompany",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "WhetherISINAvailable": self.__check_if_isin_data_available(self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN",
                data=self.__xbrl_downloader.get_xbrl_data())),
            "ISINCode": self.__xbrl_processor.process_xbrl_data_to_get_text_from_single_tag(
                tag_to_search="ISIN",
                data=self.__xbrl_downloader.get_xbrl_data()),
            "RevisedFilling": self.__xbrl_processor.process_xbrl_data_by_contact_person_name(
                contact_person_name=dict_data["acqName"],
                tag_name="RevisedFilling",
                data=self.__xbrl_downloader.get_xbrl_data())
        }

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
        table_rows["DataSource"] = None
        table_rows["InsertDate"] = None
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