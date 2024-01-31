from lib.lib import BeautifulSoup, element
from pipeline_manager.error_info.error_logger import get_original_error_message
from pipeline_manager.xbrl_processor_interface.xbrl_processor_interface import XBRLProcessorABC


class XBRLProcessor(XBRLProcessorABC):

    def __init__(self) -> None:
        self.__soup: BeautifulSoup = BeautifulSoup()
        self.__context_ref: str = ""
        self.__is_transaction_orphan: bool = False
        self.__data_to_compare: dict = {}
        self.__context_ref_list: list[str] = list()
        self.__distinct_context_refs_with_their_unique_tags_dict: dict = {}
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

    def process_general_xbrl_data(self, tag_name: str) -> str:
        try:
            if self.__is_transaction_orphan is True:
                return ""
            return self.__process_xbrl_data(tag_name=tag_name)
        except TypeError as e:
            raise TypeError(get_original_error_message(e))
        except KeyError as e:
            raise KeyError(get_original_error_message(e))
        except ValueError as e:
            raise ValueError(get_original_error_message(e))
        except Exception as e:
            raise Exception(get_original_error_message(e))

    def __fill_data_to_compare(self, acqMode: str, secAcq: str, secType: str, secVal: str, tdpTransactionType: str,
                                     befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                     befAcqSharesPer: str, acqName: str) -> None:
        self.__data_to_compare = {
            "ModeOfAcquisitionOrDisposal": acqMode,
            "SecuritiesAcquiredOrDisposedNumberOfSecurity": secAcq,
            "TypeOfInstrument": secType,
            "SecuritiesAcquiredOrDisposedValueOfSecurity": secVal,
            "SecuritiesAcquiredOrDisposedTransactionType": tdpTransactionType,
            "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity": befAcqSharesNo,
            "SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity": afterAcqSharesNo,
            "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding": afterAcqSharesPer,
            "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding": befAcqSharesPer,
            "NameOfThePerson": acqName}

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
            if temp_dict.get("ModeOfAcquisitionOrDisposal", 1) == \
                    self.__data_to_compare.get("ModeOfAcquisitionOrDisposal", 2) and \
                    temp_dict.get("SecuritiesAcquiredOrDisposedNumberOfSecurity", 1) == \
                    self.__data_to_compare.get("SecuritiesAcquiredOrDisposedNumberOfSecurity", 2) and \
                    temp_dict.get("TypeOfInstrument", 1) == \
                    self.__data_to_compare.get("TypeOfInstrument", 2) and \
                    temp_dict.get("SecuritiesAcquiredOrDisposedValueOfSecurity", 1) == \
                    self.__data_to_compare.get("SecuritiesAcquiredOrDisposedValueOfSecurity", 2) and \
                    temp_dict.get("SecuritiesAcquiredOrDisposedTransactionType", 1) == \
                    self.__data_to_compare.get("SecuritiesAcquiredOrDisposedTransactionType", 2) and \
                    temp_dict.get("SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity", 1) == \
                    self.__data_to_compare.get("SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity", 2) and \
                    temp_dict.get("SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity", 1) == \
                    self.__data_to_compare.get("SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity", 2) and \
                    temp_dict.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding", 1) == \
                    self.__data_to_compare.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding", 2) and \
                    temp_dict.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding", 1) == \
                    self.__data_to_compare.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding", 2) and \
                    temp_dict.get("NameOfThePerson", 1) == self.__data_to_compare.get("NameOfThePerson", 1):
                self.__context_ref = key
                break

    def set_orphan_transaction_status(self, acqMode: str, secAcq: str, secType: str, secVal: str,
                                      tdpTransactionType: str,
                                      befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                      befAcqSharesPer: str, acqName: str, data: str) -> None:
        self.__soup = BeautifulSoup(data, features="xml")
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

    @staticmethod
    def __handle_text(str_from) -> str:
        if str_from is None or str_from == "" or str_from == " ":
            return "-"
        return str_from

    def set_orphan_transaction_status_by_contact_person(self, set_value: bool) -> None:
        self.__is_transaction_orphan = set_value

    def get_orphan_transaction_status(self) -> bool:
        return self.__is_transaction_orphan

    def __process_xbrl_data(self, tag_name: str) -> str:
        self.__find_context_ref_by_contact_person()
        return self.__get_value_from_multiple_tag_result_based_on_context_ref(tag_name)

    def process_xbrl_data_to_get_text_from_single_tag(self, tag_to_search: str, data: str) -> str:
        self.__soup = BeautifulSoup(data, features="xml")
        xml_tag = self.__soup.find(tag_to_search)
        if xml_tag:
            return xml_tag.text
        return ""

    def process_xbrl_data_to_get_context_info_by_contact_person_name(self, parent_tag_name: str,
                                                                     child_tag_name: str, contact_person_name: str,
                                                                     data: str) -> str:
        return_text: str = ""
        if self.__is_transaction_orphan:
            return return_text
        self.__soup = BeautifulSoup(data, features="xml")
        self.__contact_person_name = contact_person_name
        self.__find_context_ref_by_contact_person()
        xml_tag_context = self.__soup.find_all(parent_tag_name)
        if xml_tag_context is None:
            return return_text
        for tag in xml_tag_context:
            if tag["id"] == self.__context_ref:
                return_text = tag.find(child_tag_name).text
        return return_text

    def __check_for_other_values_if_they_match(self, xml_tag: element.ResultSet,
                                               match_text: str) -> bool:
        for tag in xml_tag:
            if tag["contextRef"] != self.__context_ref:
                continue
            if tag.text == match_text:
                return True
        return False

    @staticmethod
    def __check_for_match_by_other_means(xml_tag: element.ResultSet, match_text) -> bool:
        counter: int = 0
        for tag in xml_tag:
            if tag.text == match_text:
                counter += 1
        if counter > 1:
            return False
        return True

    def __get_value_from_multiple_tag_result_based_on_context_ref(self, tag_name: str) -> str:
        return_value: str = ""
        xml_tag = self.__find_all_tags(tag_name=tag_name)
        for tag in xml_tag:
            if tag["contextRef"] == self.__context_ref:
                return_value = tag.text
        return return_value

    @staticmethod
    def __find_context_ref_by_using_other_means(xml_tag: element.ResultSet, match_text: str) -> str:
        for tag in xml_tag:
            if tag.text != match_text:
                continue
            return tag['contextRef']
        return ""

    def __find_context_ref_by_contact_person(self) -> None:
        xml_tag = self.__find_all_tags('NameOfThePerson')
        for tag in xml_tag:
            if tag.text == self.__contact_person_name:
                self.__context_ref = tag['contextRef']

    def __find_all_tags(self, tag_name: str) -> element.ResultSet:
        xml_tag = self.__soup.find_all(tag_name)
        return xml_tag
