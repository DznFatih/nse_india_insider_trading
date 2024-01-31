from abc import abstractmethod, ABC


class XBRLProcessorABC(ABC):

    @abstractmethod
    def process_general_xbrl_data(self, tag_name: str) -> str:
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
    def set_orphan_transaction_status(self, acqMode: str, secAcq: str, secType: str, secVal: str,
                                      tdpTransactionType: str,
                                      befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                      befAcqSharesPer: str, acqName: str, data: str) -> None:
        pass
