from abc import abstractmethod, ABC


class XBRLProcessorInterface(ABC):

    @abstractmethod
    def process_general_xbrl_data(self, tag_name: str) -> str:
        pass

    @abstractmethod
    def set_xbrl_link_status(self, is_xbrl_link_missing: bool) -> None:
        pass

    @abstractmethod
    def set_transaction_status_to_default(self) -> None:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_text_from_single_tag(self, tag_to_search: str) -> str:
        pass

    @abstractmethod
    def process_xbrl_data_to_get_context_info(self, parent_tag_name: str,
                                              child_tag_name: str) -> str:
        pass

    @abstractmethod
    def get_orphan_transaction_status(self) -> bool:
        pass

    @abstractmethod
    def set_orphan_transaction_status(self, acqMode: str, secAcq: str, secType: str, secVal: str,
                                      tdpTransactionType: str,
                                      befAcqSharesNo: str, afterAcqSharesNo: str, afterAcqSharesPer: str,
                                      befAcqSharesPer: str, acqName: str) -> None:
        pass

    @abstractmethod
    def set_beautiful_soup(self, data: str) -> None:
        pass
