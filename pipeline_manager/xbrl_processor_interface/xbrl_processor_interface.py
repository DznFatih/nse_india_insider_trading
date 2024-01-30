from abc import abstractmethod, ABC


class XBRLProcessorABC(ABC):

    @abstractmethod
    def process_general_xbrl_data(self, contact_person_name: str, tag_name: str, data: str) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_text_from_single_tag(self, tag_to_search: str, data: str) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_context_info_by_contact_person_name(self, parent_tag_name: str,
                                                                     child_tag_name: str, contact_person_name: str,
                                                                     data: str) -> str:
        pass

    @abstractmethod
    def get_orphan_transaction_status(self) -> bool:
        pass

    @abstractmethod
    def set_orphan_transaction_status_by_other_means(self, type_of_security: str, number_of_securities: str,
                                      acquisition_disposal: str) -> None:
        pass

    @abstractmethod
    def set_orphan_transaction_status_by_contact_person(self, set_value: bool) -> None:
        pass
