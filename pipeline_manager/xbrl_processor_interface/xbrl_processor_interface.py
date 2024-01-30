from abc import abstractmethod, ABC


class XBRLProcessorABC(ABC):

    @abstractmethod
    def process_xbrl_data_by_contact_person_name(self, contact_person_name: str, tag_name: str, data) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_by_other_means(self, tag_name: str, data: str) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_text_from_single_tag(self, tag_to_search: str, data: str) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_context_info_by_contact_person_name(self, parent_tag_name: str,
                                                                child_tag_name: str, contact_person_name: str,
                                                                data: str) -> str:
        pass

    def check_if_transaction_available_in_file_by_other_means(self, type_of_security: str,
                                                              number_of_securities: str,
                                                              acquisition_disposal: str) -> None:
        pass

    @abstractmethod
    def is_transaction_in_xbrl_file_by_other_means_found(self) -> bool:
        pass
