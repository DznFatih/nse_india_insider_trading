from lib.lib import BeautifulSoup, element
from pipeline_manager.xbrl_processor_interface.xbrl_processor_interface import XBRLProcessorABC


class XBRLProcessor(XBRLProcessorABC):

    def __init__(self) -> None:
        self.__soup: BeautifulSoup = BeautifulSoup()
        self.__change_in_holding_securities_num: str = ""
        self.__contact_person_name: str = ""
        self.__type_of_security: str = ""
        self.__number_of_securities: str = ""
        self.__acquisition_disposal: str = ""
        self.__change_in_holding_securities_num: str = ""
        self.__transaction_in_xbrl_file_by_other_means_found: bool = False

    def process_xbrl_data_by_contact_person_name(self, contact_person_name: str, tag_name: str, data: str) -> str:
        self.__soup = BeautifulSoup(data, features="xml")
        self.__contact_person_name = contact_person_name
        self.__find_context_ref_by_contact_person()
        return self.__get_value_from_multiple_tag_result_based_on_context_ref(tag_name)

    def process_xbrl_data_by_other_means(self, tag_name: str, data: str) -> str:
        if self.__transaction_in_xbrl_file_by_other_means_found:
            self.__soup = BeautifulSoup(data, features="xml")
            return self.__get_value_from_multiple_tag_result_based_on_context_ref(tag_name=tag_name)
        return ""

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
        self.__soup = BeautifulSoup(data, features="xml")
        self.__contact_person_name = contact_person_name
        self.__find_context_ref_by_contact_person()
        xml_tag_context = self.__soup.find_all(parent_tag_name)
        if xml_tag_context is None:
            return return_text
        for tag in xml_tag_context:
            if tag["id"] == self.__change_in_holding_securities_num:
                return_text = tag.find(child_tag_name).text
        return return_text

    def process_xbrl_data_to_get_context_info_by_other_means(self, parent_tag_name: str,
                                                             child_tag_name: str,
                                                             data: str) -> str:
        return_text: str = ""

        if not self.__transaction_in_xbrl_file_by_other_means_found:
            return return_text

        self.__soup = BeautifulSoup(data, features="xml")

        xml_tag_context = self.__soup.find_all(parent_tag_name)
        if xml_tag_context

        for tag in xml_tag_context:
            if tag["id"] == self.__change_in_holding_securities_num:
                return_text = tag.find(child_tag_name).text
        return return_text

    def check_if_transaction_available_in_file_by_other_means(self, type_of_security: str,
                                                              number_of_securities: str,
                                                              acquisition_disposal: str) -> None:
        self.__type_of_security = type_of_security
        self.__number_of_securities = number_of_securities
        self.__acquisition_disposal = acquisition_disposal
        xml_tag_type_of_security = self.__find_all_tags('TypeOfInstrument')
        xml_tag_number_of_securities = self.__find_all_tags(
            'SecuritiesAcquiredOrDisposedNumberOfSecurity')
        xml_tag_acquisition_disposal = self.__find_all_tags(
            'SecuritiesAcquiredOrDisposedTransactionType')

        if self.__check_for_match_by_other_means(xml_tag_number_of_securities, match_text=self.__number_of_securities):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                                                                                xml_tag_number_of_securities,
                                                                                match_text=self.__number_of_securities)
            type_of_security_match: bool = self.__check_for_other_values_if_they_match(xml_tag_type_of_security,
                                                                                       self.__type_of_security)
            acquisition_disposal_match: bool = self.__check_for_other_values_if_they_match(xml_tag_acquisition_disposal,
                                                                                           self.__acquisition_disposal)
            if type_of_security_match and acquisition_disposal_match:
                self.__transaction_in_xbrl_file_by_other_means_found = True
            self.__transaction_in_xbrl_file_by_other_means_found = False

        elif self.__check_for_match_by_other_means(xml_tag_type_of_security, match_text=self.__type_of_security):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                xml_tag_type_of_security,
                match_text=self.__type_of_security)
            number_of_security_match: bool = self.__check_for_other_values_if_they_match(xml_tag_number_of_securities,
                                                                                       self.__number_of_securities)
            acquisition_disposal_match: bool = self.__check_for_other_values_if_they_match(xml_tag_acquisition_disposal,
                                                                                           self.__acquisition_disposal)
            if number_of_security_match and acquisition_disposal_match:
                self.__transaction_in_xbrl_file_by_other_means_found = True
            self.__transaction_in_xbrl_file_by_other_means_found = False

        elif self.__check_for_match_by_other_means(xml_tag_acquisition_disposal, match_text=self.__acquisition_disposal):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                xml_tag_acquisition_disposal,
                match_text=self.__acquisition_disposal)
            number_of_security_match: bool = self.__check_for_other_values_if_they_match(xml_tag_number_of_securities,
                                                                                         self.__number_of_securities)
            type_of_security_match: bool = self.__check_for_other_values_if_they_match(xml_tag_type_of_security,
                                                                                       self.__type_of_security)
            if number_of_security_match and type_of_security_match:
                self.__transaction_in_xbrl_file_by_other_means_found = True
            self.__transaction_in_xbrl_file_by_other_means_found = False

    def __check_for_other_values_if_they_match(self, xml_tag: element.ResultSet,
                                               change_in_holding_securities_num: str) -> bool:
        for tag in xml_tag:
            if tag["contextRef"] != change_in_holding_securities_num:
                continue
            if tag.text == self.__type_of_security:
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
            if tag["contextRef"] == self.__change_in_holding_securities_num:
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
                self.__change_in_holding_securities_num = tag['contextRef']

    def __find_all_tags(self, tag_name: str) -> element.ResultSet:
        xml_tag = self.__soup.find_all(tag_name)
        return xml_tag

    def is_transaction_in_xbrl_file_by_other_means_found(self) -> bool:
        return self.__transaction_in_xbrl_file_by_other_means_found
