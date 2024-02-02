from lib.lib import BeautifulSoup, element, decimal
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.xbrl_processor.xbrl_processor_interface import XBRLProcessorInterface


class XBRLProcessor(XBRLProcessorInterface):

    def __init__(self) -> None:
        self.__soup: BeautifulSoup = BeautifulSoup()
        self.__context_ref: str = ""
        self.__is_transaction_orphan: bool = False
        self.__data_to_compare: dict = {}
        self.__context_ref_list: list[str] = list()
        self.__distinct_context_refs_with_their_unique_tags_dict: dict = {}
        self.__is_xbrl_link_missing: bool = False
        self.__tags_to_search = ["ModeOfAcquisitionOrDisposal",
                                 "SecuritiesAcquiredOrDisposedNumberOfSecurity",
                                 "TypeOfInstrument",
                                 "SecuritiesAcquiredOrDisposedValueOfSecurity",
                                 "SecuritiesAcquiredOrDisposedTransactionType",
                                 "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
                                 "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
                                 "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
                                 "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
                                 "NameOfThePerson"]

    def set_xbrl_link_status(self, set_value: bool) -> None:
        self.__is_xbrl_link_missing = set_value

    def set_transaction_status_to_default(self) -> None:
        self.__is_transaction_orphan = False
        self.__is_xbrl_link_missing = False

    def set_beautiful_soup(self, data: str) -> None:
        self.__soup = BeautifulSoup(data, features="xml")

    def set_orphan_transaction_status(self, acqMode: str, secAcq: str, secType: str, secVal: str,
                                      tdpTransactionType: str, befAcqSharesNo: str, afterAcqSharesNo: str,
                                      afterAcqSharesPer: str, befAcqSharesPer: str, acqName: str) -> None:
        self.__fill_data_to_compare(acqMode, secAcq, secType, secVal, tdpTransactionType,
                                    befAcqSharesNo, afterAcqSharesNo, afterAcqSharesPer,
                                    befAcqSharesPer, acqName)
        self.__fill_distinct_context_ref()
        self.__fill_distinct_context_refs_with_their_unique_tags_dict()
        self.__find_context_ref()
        if self.__context_ref:
            self.__is_transaction_orphan = False
        else:
            self.__is_transaction_orphan = True

    def get_orphan_transaction_status(self) -> bool:
        return self.__is_transaction_orphan

    def process_general_xbrl_data(self, tag_name: str) -> str | None:
        try:
            if self.__is_searchable_in_xbrl_file() is False:
                return None
            return self.__get_value_from_multiple_tag_result_based_on_context_ref(tag_name)
        except TypeError as e:
            raise TypeError(get_error_details(e))
        except KeyError as e:
            raise KeyError(get_error_details(e))
        except ValueError as e:
            raise ValueError(get_error_details(e))
        except Exception as e:
            raise Exception(get_error_details(e))

    def process_xbrl_data_to_get_text_from_single_tag(self, tag_to_search: str) -> str | None:
        if self.__is_searchable_in_xbrl_file() is False:
            return None
        xml_tag = self.__soup.find(tag_to_search)
        if xml_tag:
            return xml_tag.text
        return None

    def process_xbrl_data_to_get_context_info(self, parent_tag_name: str,
                                              child_tag_name: str) -> str | None:
        if self.__is_searchable_in_xbrl_file() is False:
            return None
        xml_tag_context = self.__soup.find_all(parent_tag_name)
        if xml_tag_context is None:
            return None
        for tag in xml_tag_context:
            if tag["id"] == self.__context_ref:
                return tag.find(child_tag_name).text
        return None

    def __is_searchable_in_xbrl_file(self) -> bool:
        if self.__is_transaction_orphan is True or self.__is_xbrl_link_missing is True:
            return False
        return True

    def __fill_data_to_compare(self, acqMode: str, secAcq: str, secType: str, secVal: str, tdpTransactionType: str,
                                     befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                     befAcqSharesPer: str, acqName: str) -> None:
        acq_name_with_default_value: str = self.__handle_text(acqName)
        if acq_name_with_default_value != "N/A":
            self.__data_to_compare["NameOfThePerson"] = acq_name_with_default_value
        self.__data_to_compare["ModeOfAcquisitionOrDisposal"] = self.__handle_text(acqMode)
        self.__data_to_compare["SecuritiesAcquiredOrDisposedNumberOfSecurity"] = self.__handle_text(secAcq)
        self.__data_to_compare["TypeOfInstrument"] = self.__handle_text(secType)
        self.__data_to_compare["SecuritiesAcquiredOrDisposedValueOfSecurity"] = self.__handle_text(secVal)
        self.__data_to_compare["SecuritiesAcquiredOrDisposedTransactionType"] = (
            self.__handle_text(tdpTransactionType))
        self.__data_to_compare["SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity"] = (
            self.__handle_text(befAcqSharesNo))
        self.__data_to_compare["SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity"] = (
            self.__handle_text(afterAcqSharesNo))
        self.__data_to_compare["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"] = (
            self.__handle_text(self.__normalize_fraction(afterAcqSharesPer)))
        self.__data_to_compare["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"] = (
            self.__handle_text(self.__normalize_fraction(befAcqSharesPer)))

    def __fill_distinct_context_ref(self) -> None:
        xml_tag_context_ref = self.__soup.find_all("context")
        for tag in xml_tag_context_ref:
            self.__context_ref_list.append(tag["id"])

    def __fill_distinct_context_refs_with_their_unique_tags_dict(self) -> None:
        for tag_context_ref in self.__context_ref_list:
            temp_list = []
            for tag_to_search in self.__tags_to_search:
                tag_search_results = self.__soup.find_all(tag_to_search)
                for tag_search_result in tag_search_results:
                    if tag_search_result["contextRef"] == tag_context_ref:
                        temp_list.append(tag_search_result)
            self.__distinct_context_refs_with_their_unique_tags_dict[tag_context_ref] = temp_list

    def __find_context_ref(self) -> None:
        for key, value in self.__distinct_context_refs_with_their_unique_tags_dict.items():
            temp_dict = {}
            for item in value:
                temp_dict[item.name] = str(item.text)
            if self.__is_xbrl_data_match_with_table_data(dict_data=temp_dict) is True:
                self.__context_ref = key
                break

    def __is_xbrl_data_match_with_table_data(self, dict_data: dict) -> bool:
        if dict_data.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"):
            dict_data["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"] = (
                self.__normalize_fraction(dict_data["SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding"]))
        if dict_data.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"):
            dict_data["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"] = (
                self.__normalize_fraction(dict_data["SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding"]))

        if (dict_data.get("ModeOfAcquisitionOrDisposal", 1) ==
                self.__data_to_compare.get("ModeOfAcquisitionOrDisposal", 2) and
                dict_data.get("SecuritiesAcquiredOrDisposedNumberOfSecurity", 1) ==
                self.__data_to_compare.get("SecuritiesAcquiredOrDisposedNumberOfSecurity", 2) and
                dict_data.get("TypeOfInstrument", 1) ==
                self.__data_to_compare.get("TypeOfInstrument", 2) and
                dict_data.get("SecuritiesAcquiredOrDisposedValueOfSecurity", 1) ==
                self.__data_to_compare.get("SecuritiesAcquiredOrDisposedValueOfSecurity", 2) and
                dict_data.get("SecuritiesAcquiredOrDisposedTransactionType", 1) ==
                self.__data_to_compare.get("SecuritiesAcquiredOrDisposedTransactionType", 2) and
                dict_data.get("SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity", 1) ==
                self.__data_to_compare.get("SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity", 2) and
                dict_data.get("SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity", 1) ==
                self.__data_to_compare.get("SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity", 2) and
                dict_data.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding", 1) ==
                self.__data_to_compare.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding", 2) and
                dict_data.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding", 1) ==
                self.__data_to_compare.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding", 2) and
                dict_data.get("NameOfThePerson", 1) == self.__data_to_compare.get("NameOfThePerson", 1)):
            return True
        return False

    @staticmethod
    def __handle_text(str_from: str | None) -> str:
        if str_from is None or str_from == "" or str_from == " ":
            return "N/A"
        return str_from

    def __get_value_from_multiple_tag_result_based_on_context_ref(self, tag_name: str) -> str:
        return_value: str = ""
        xml_tag = self.__find_all_tags(tag_name=tag_name)
        for tag in xml_tag:
            if tag["contextRef"] == self.__context_ref:
                return_value = tag.text
        return return_value

    def __find_all_tags(self, tag_name: str) -> element.ResultSet:
        xml_tag = self.__soup.find_all(tag_name)
        return xml_tag

    @staticmethod
    def __normalize_fraction(str_data: str) -> str | None:
        if str_data == "-" or str_data is None:
            return None
        d = decimal.Decimal(str_data)
        normalized = d.normalize()
        sign, digits, exponent = normalized.as_tuple()
        if exponent > 0:
            return str(decimal.Decimal((sign, digits + (0,) * exponent, 0)))
        else:
            return str(normalized)
