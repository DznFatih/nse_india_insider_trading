from lib.lib import BeautifulSoup, element
from pipeline_manager.error_info.error_logger import get_original_error_message
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
        self.__is_transaction_orphan: bool = False

    def process_general_xbrl_data(self, contact_person_name: str, tag_name: str, data: str) -> str:
        try:
            if self.__is_transaction_orphan is True:
                return ""
            self.__contact_person_name = self.__handle_text(contact_person_name)
            self.__soup = BeautifulSoup(data, features="xml")
            if self.__contact_person_name != "-":
                return self.__process_xbrl_data_by_contact_person_name(tag_name=tag_name)
            return self.__process_xbrl_data_by_other_means(tag_name=tag_name)
        except TypeError as e:
            raise TypeError(get_original_error_message(e))
        except KeyError as e:
            raise KeyError(get_original_error_message(e))
        except ValueError as e:
            raise ValueError(get_original_error_message(e))
        except Exception as e:
            raise Exception(get_original_error_message(e))

    def set_orphan_transaction_status_by_other_means(self, type_of_security: str, number_of_securities: str,
                                                     acquisition_disposal: str) -> None:
        try:
            self.__type_of_security = self.__handle_text(str_from=type_of_security)
            self.__number_of_securities = self.__handle_text(str_from=number_of_securities)
            self.__acquisition_disposal = self.__handle_text(str_from=acquisition_disposal)
            if (self.__type_of_security == "-" or self.__number_of_securities == "-" or
                    self.__acquisition_disposal == "-"):
                self.__is_transaction_orphan = True
            else:
                self.__check_if_transaction_available_in_file_by_other_means()
        except TypeError as e:
            raise TypeError(get_original_error_message(e))
        except KeyError as e:
            raise KeyError(get_original_error_message(e))
        except ValueError as e:
            raise ValueError(get_original_error_message(e))
        except Exception as e:
            raise Exception(get_original_error_message(e))

    @staticmethod
    def __handle_text(str_from) -> str:
        if str_from is None or str_from == "" or str_from == " ":
            return "-"
        return str_from

    def set_orphan_transaction_status_by_contact_person(self, set_value: bool) -> None:
        self.__is_transaction_orphan = set_value

    def get_orphan_transaction_status(self) -> bool:
        return self.__is_transaction_orphan

    def __process_xbrl_data_by_contact_person_name(self, tag_name: str) -> str:
        self.__find_context_ref_by_contact_person()
        return self.__get_value_from_multiple_tag_result_based_on_context_ref(tag_name)

    def __process_xbrl_data_by_other_means(self, tag_name: str) -> str:
        if self.__is_transaction_orphan:
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
        if self.__is_transaction_orphan:
            return return_text
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
        if self.__is_transaction_orphan:
            return return_text
        self.__soup = BeautifulSoup(data, features="xml")
        xml_tag_context = self.__soup.find_all(parent_tag_name)
        if not xml_tag_context:
            return return_text
        for tag in xml_tag_context:
            if tag["id"] == self.__change_in_holding_securities_num:
                return_text = tag.find(child_tag_name).text
                break
        return return_text

    def __check_if_transaction_available_in_file_by_other_means(self) -> None:
        xml_tag_type_of_security = self.__find_all_tags('TypeOfInstrument')
        xml_tag_number_of_securities = self.__find_all_tags(
            'SecuritiesAcquiredOrDisposedNumberOfSecurity')
        xml_tag_acquisition_disposal = self.__find_all_tags(
            'SecuritiesAcquiredOrDisposedTransactionType')

        if self.__check_for_match_by_other_means(xml_tag_number_of_securities, match_text=self.__number_of_securities):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                                                                                xml_tag=xml_tag_number_of_securities,
                                                                                match_text=self.__number_of_securities)
            type_of_security_match: bool = self.__check_for_other_values_if_they_match(
                                                                                   xml_tag=xml_tag_type_of_security,
                                                                                   match_text=self.__type_of_security)
            acquisition_disposal_match: bool = self.__check_for_other_values_if_they_match(
                                                                               xml_tag=xml_tag_acquisition_disposal,
                                                                               match_text=self.__acquisition_disposal)
            if type_of_security_match and acquisition_disposal_match:
                self.__is_transaction_orphan = False
            else:
                self.__is_transaction_orphan = True

        elif self.__check_for_match_by_other_means(xml_tag=xml_tag_type_of_security,
                                                   match_text=self.__type_of_security):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                                                    xml_tag=xml_tag_type_of_security,
                                                    match_text=self.__type_of_security)
            number_of_security_match: bool = self.__check_for_other_values_if_they_match(
                                                    xml_tag=xml_tag_number_of_securities,
                                                    match_text=self.__number_of_securities)
            acquisition_disposal_match: bool = self.__check_for_other_values_if_they_match(
                                                    xml_tag=xml_tag_acquisition_disposal,
                                                    match_text=self.__acquisition_disposal)
            if number_of_security_match and acquisition_disposal_match:
                self.__is_transaction_orphan = False
            else:
                self.__is_transaction_orphan = True

        elif self.__check_for_match_by_other_means(xml_tag=xml_tag_acquisition_disposal,
                                                   match_text=self.__acquisition_disposal):
            self.__change_in_holding_securities_num = self.__find_context_ref_by_using_other_means(
                                                   xml_tag=xml_tag_acquisition_disposal,
                                                   match_text=self.__acquisition_disposal)
            number_of_security_match: bool = self.__check_for_other_values_if_they_match(
                                                   xml_tag=xml_tag_number_of_securities,
                                                   match_text=self.__number_of_securities)
            type_of_security_match: bool = self.__check_for_other_values_if_they_match(
                                                   xml_tag=xml_tag_type_of_security,
                                                   match_text=self.__type_of_security)
            if number_of_security_match and type_of_security_match:
                self.__is_transaction_orphan = False
            else:
                self.__is_transaction_orphan = True

    def __check_for_other_values_if_they_match(self, xml_tag: element.ResultSet,
                                               match_text: str) -> bool:
        for tag in xml_tag:
            if tag["contextRef"] != self.__change_in_holding_securities_num:
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
