from lib.lib import BeautifulSoup, element, decimal
from pipeline_manager.get_error_details.get_error_details import get_error_details
from pipeline_manager.xbrl_processor.xbrl_processor_interface import XBRLProcessorInterface


class XBRLProcessor(XBRLProcessorInterface):

    def __init__(self) -> None:
        """
        This method processes data within the XBRL file, which may contain multiple transactions associated with the same company.
        To ensure the accurate capture of a single transaction from the company's row and to avoid duplicates,
        it employs a combination of fields from the table to match with the file's data.

        Upon finding a matching transaction, it captures the contextRef (e.g., contextRef="ChangeInHoldingOfSecurities001I"),
        which holds the specific transaction details for the company. The fields utilized to match data between the XBRL file
        and the table row visible on the website are listed in the __tags_to_search instance variable.
        """
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

    def set_xbrl_link_status(self, is_xbrl_link_missing: bool) -> None:
        """
        Allows caller to set xbrl link missing status. If set to False, this object wont take any further action in
        process_general_xbrl_data method
        :param is_xbrl_link_missing: Boolean values
        :return:
        """
        self.__is_xbrl_link_missing = is_xbrl_link_missing

    def set_transaction_status_to_default(self) -> None:
        """
        Allows to set instance variables __is_transaction_orphan and __is_xbrl_link_missing to False
        for the next run
        :return:
        """
        self.__is_transaction_orphan = False
        self.__is_xbrl_link_missing = False
        self.__context_ref: str = ""
        self.__data_to_compare = {}
        self.__context_ref_list = list()
        self.__distinct_context_refs_with_their_unique_tags_dict = {}

    def set_beautiful_soup(self, data: str) -> None:
        """
        Passes string data into BeautifulSoup class to create __soup object
        :param data:
        :return:
        """
        self.__soup = BeautifulSoup(data, features="xml")

    def set_orphan_transaction_status(self, acqMode: str, secAcq: str, secType: str, secVal: str,
                                      tdpTransactionType: str, befAcqSharesNo: str, afterAcqSharesNo: str,
                                      afterAcqSharesPer: str, befAcqSharesPer: str, acqName: str) -> None:
        """
        This method iterates through XBRL data to identify the associated transaction from the table row.
        If found, it sets the `__is_transaction_orphan` value to False; otherwise, it sets it to True.

        Orphan transactions refer to transactions with a linked XBRL file, but there is no means of identifying them in
        that file. This is often due to the inability to match transaction information from the table row with the
        corresponding transaction in the XBRL file. If a match is found, and the transaction is identified in the
        XBRL file, it is considered a non-orphaned transaction.

        Not all arguments listed below may be visible in the table on the NSEIndia.com website.
        When data is downloaded using the Requests library, the response includes additional information, encompassing
        details from the table visible on the website.

        Parameters:
        - acqMode (str): ModeOfAcquisitionOrDisposal in XBRL File
        - secAcq (str): SecuritiesAcquiredOrDisposedNumberOfSecurity in XBRL File
        - secType (str): TypeOfInstrument in XBRL File
        - secVal (str): SecuritiesAcquiredOrDisposedValueOfSecurity in XBRL File
        - tdpTransactionType (str): SecuritiesAcquiredOrDisposedTransactionType in XBRL File
        - befAcqSharesNo (str): SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity in XBRL File
        - afterAcqSharesNo (str): SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity in XBRL File
        - afterAcqSharesPer (str): SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding in XBRL File
        - befAcqSharesPer (str): SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding in XBRL File
        - acqName (str): NameOfThePerson in XBRL File

        Returns:
        - None
        """
        self.__fill_data_to_compare(acqMode, secAcq, secType, secVal, tdpTransactionType,
                                    befAcqSharesNo, afterAcqSharesNo, afterAcqSharesPer,
                                    befAcqSharesPer, acqName)
        self.__fill_distinct_context_ref()
        self.__fill_distinct_context_refs_with_their_unique_tags_dict()
        context_ref: list[str] = self.__find_context_ref()
        if len(context_ref) > 1 or len(context_ref) == 0:
            self.__is_transaction_orphan = True
        elif len(context_ref) == 1:
            self.__context_ref = context_ref[0]
            self.__is_transaction_orphan = False

    def get_orphan_transaction_status(self) -> bool:
        """
        Returns the status of orphan transaction status for the current transaction
        :return: Boolean
        """
        return self.__is_transaction_orphan

    def process_general_xbrl_data(self, tag_name: str) -> str | None:
        """
        Returns the text value from tag of the XBRL file
        :param tag_name: Tag name to search in XBRL file
        :return: string if value exists, None if it's not
        """
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
        """
        Returns value from tags that are unique in XBRL file meaning they are exist only once in the file and
        there is not any duplicate of the same tag (i.e. CompanyName)
        :param tag_to_search: Tag name
        :return: string if value exists, None if it's not
        """
        if self.__is_searchable_in_xbrl_file() is False:
            return None
        xml_tag = self.__soup.find(tag_to_search)
        if xml_tag:
            return xml_tag.text
        return None

    def process_xbrl_data_to_get_context_info(self, parent_tag_name: str,
                                              child_tag_name: str) -> str | None:
        """
        Returns context info text from XBRL file (i.e. <xbrli:context id="ChangeInHoldingOfSecurities001I"> <xbrli:period>)
        :param parent_tag_name: Parent tag that has sub tags
        :param child_tag_name: Child tags under parent
        :return: string or None
        """
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
        """
        Checks to see if it should process XBRL file
        :return: boolean
        """
        if self.__is_transaction_orphan is True or self.__is_xbrl_link_missing is True:
            return False
        return True

    def __fill_data_to_compare(self, acqMode: str, secAcq: str, secType: str, secVal: str, tdpTransactionType: str,
                                     befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                     befAcqSharesPer: str, acqName: str) -> None:
        """
        Stores transaction details for comparing against XBRL file
        :param acqMode: ModeOfAcquisitionOrDisposal in XBRL File
        :param secAcq: SecuritiesAcquiredOrDisposedNumberOfSecurity in XBRL File
        :param secType: TypeOfInstrument in XBRL File
        :param secVal: SecuritiesAcquiredOrDisposedValueOfSecurity in XBRL File
        :param tdpTransactionType: SecuritiesAcquiredOrDisposedTransactionType in XBRL File
        :param befAcqSharesNo: SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity in XBRL File
        :param afterAcqSharesNo: SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity in XBRL File
        :param afterAcqSharesPer: SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding in XBRL File
        :param befAcqSharesPer: SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding in XBRL File
        :param acqName: NameOfThePerson in XBRL File
        :return:
        """
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
        """
        Captures distinct contextRef info from XBRL data (i.e. contextRef="ChangeInHoldingOfSecurities001I")
        :return:
        """
        xml_tag_context_ref = self.__soup.find_all("context")
        for tag in xml_tag_context_ref:
            self.__context_ref_list.append(tag["id"])

    def __fill_distinct_context_refs_with_their_unique_tags_dict(self) -> None:
        """
        Captures all contextRef attributes (i.e. CategoryOfPerson, NameOfThePerson, TypeOfInstrument etc.), their
        name and values. It is possible that some of the attributes might be missing in XBRL file. It is necessary
        to capture these attributes directly from XBRL file.

        Data structure is as follows:

            __distinct_context_refs_with_their_unique_tags_dict = {
                ChangeInHoldingOfSecurities001I: ["CategoryOfPerson", "NameOfThePerson", ...]
                ChangeInHoldingOfSecurities002I: ["CategoryOfPerson", "NameOfThePerson", ...]
                ChangeInHoldingOfSecurities003I: ["CategoryOfPerson", "NameOfThePerson", ...]
                ...
            }

        :return:
        """
        for tag_context_ref in self.__context_ref_list:
            temp_list = []
            for tag_to_search in self.__tags_to_search:
                tag_search_results = self.__soup.find_all(tag_to_search)
                for tag_search_result in tag_search_results:
                    if tag_search_result["contextRef"] == tag_context_ref:
                        temp_list.append(tag_search_result)
            self.__distinct_context_refs_with_their_unique_tags_dict[tag_context_ref] = temp_list

    def __find_context_ref(self) -> list[str]:
        """
        Finds contextRef which is the attribute that holds all transactional details for person who made the
        transaction. If there is only single contextRef found, then this transaction will be considered the
        corresponding transaction to the row, and it will return value immediately.

        contextRef is used to identify transactional details of the person.

        :return:
        """
        context_ref: list[str] = list()
        if len(self.__distinct_context_refs_with_their_unique_tags_dict) == 1:
            context_ref.append(list(self.__distinct_context_refs_with_their_unique_tags_dict.keys())[0])
            return context_ref

        for key, value in self.__distinct_context_refs_with_their_unique_tags_dict.items():
            temp_dict = {}
            for item in value:
                temp_dict[item.name] = str(item.text)
            if self.__is_xbrl_data_match_with_multiple_fields(dict_data=temp_dict) is True:
                context_ref.append(key)
        if not context_ref:
            context_ref = self.__is_xbrl_data_match_with_single_field()
        return context_ref

    def __is_xbrl_data_match_with_multiple_fields(self, dict_data: dict) -> bool:
        """
        Compares XBRL attribute values against the table row values. If all details match, it returns True,
        otherwise False.

        NameOfThePerson attribute might be missing in XBRL file. If all other details match, we can skip matching this
        information.

        Even if we use all fields from table row to match data in XBRL file to find associated transaction,
        it is possible that two rows of the table will have exactly the same information
        (i.e. https://nsearchives.nseindia.com/corporate/xbrl/IT_1088925_375556_01012021042258_WEB.xml) which will
        make it impossible to find associated transaction in XBRL file. They will be considered as orphaned transactions
        too.

        :param dict_data: Currently running contextRef detail
        :return:
        """
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

    def __is_xbrl_data_match_with_single_field(self) -> list[str]:
        """
        It is sometimes possible to capture contextRef info by name.
        :return:
        """
        context_ref: list[str] = list()
        name_of_the_person: str = self.__data_to_compare.get("NameOfThePerson", 1)
        for key, value in self.__distinct_context_refs_with_their_unique_tags_dict.items():
            name_of_the_person_in_xbrl_document: str = ""
            for item in value:
                if item.name == 'NameOfThePerson':
                    name_of_the_person_in_xbrl_document = str(item.text)
                    break
            if name_of_the_person == name_of_the_person_in_xbrl_document:
                context_ref.append(key)
        return context_ref

    @staticmethod
    def __handle_text(str_from: str | None) -> str:
        """
        Replaces None or space characters with '-'.
        :param str_from: string value
        :return:
        """
        if str_from is None or str_from.casefold() in ['none', '', '-', ' ']:
            return "N/A"
        return str_from

    def __get_value_from_multiple_tag_result_based_on_context_ref(self, tag_name: str) -> str:
        """
        Returns values based on tag_name and contextRef if matches
        :param tag_name: string value
        :return:
        """
        return_value: str = ""
        xml_tag = self.__find_all_tags(tag_name=tag_name)
        for tag in xml_tag:
            if tag["contextRef"] == self.__context_ref:
                return_value = tag.text
        return return_value

    def __find_all_tags(self, tag_name: str) -> element.ResultSet:
        """
        Uses Beautiful to search tags in file
        :param tag_name: string value
        :return:
        """
        xml_tag = self.__soup.find_all(tag_name)
        return xml_tag

    @staticmethod
    def __normalize_fraction(str_data: str) -> str | None:
        """
        Deals with decimal values and removes additional 0 after dots. (i.e. 2.20 -> 2.2)
        :param str_data: string value
        :return: string or None
        """
        if str_data in ["-", "None", "", " "] or str_data is None:
            return None
        d = decimal.Decimal(str_data)
        normalized = d.normalize()
        sign, digits, exponent = normalized.as_tuple()
        if exponent > 0:
            return str(decimal.Decimal((sign, digits + (0,) * exponent, 0)))
        else:
            return str(normalized)
