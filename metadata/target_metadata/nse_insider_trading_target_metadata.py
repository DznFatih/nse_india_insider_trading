from metadata.main.target_metadata import TargetMetadata


class NSEIndiaInsiderTradingTargetMetadata(TargetMetadata):

    def __init__(self):
        self.__schema_name: str = "extract_stage"
        self.__table_name: str = "InsiderTrading"
        self.__pipeline_name: str = "NSEIndiaInsiderTrading"

    @property
    def get_schema_name(self) -> str:
        return self.__schema_name

    @property
    def get_table_name(self) -> str:
        return self.__table_name

    @property
    def get_table_column_name_list(self) -> list[str]:
        return [
        "Symbol",	"CompanyName",	"Regulation",	"NameOfTheAcquirerORDisposer",	"TypeOfSecurity",
        "NoOfSecurities",	"AcquisitionORDisposal",	"BroadcastDateTime",	"XBRLLink",	"Period",
        "ScripCode",	"NSESymbol",	"MSEISymbol",	"NameOfTheCompany",	"WhetherISINAvailable",
        "ISINCode",	"RevisedFilling",
        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract",
        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable",
        "ChangeInHoldingOfSecuritiesOfPromotersAxis",
        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems",
        "TypeOfInstrument",	"TypeOfInstrumentOthers",	"CategoryOfPerson",	"NameOfThePerson",	"PANNumber",
        "IdentificationNumberOfDirectorOrCompany",	"Address",	"ContactNumber",
        "SecuritiesHeldPriorToAcquisitionOrDisposalAbstract",
        "SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity",
        "SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding",
        "SecuritiesAcquiredOrDisposedAbstract",	"SecuritiesAcquiredOrDisposedNumberOfSecurity",
        "SecuritiesAcquiredOrDisposedValueOfSecurity",	"SecuritiesAcquiredOrDisposedTransactionType",
        "SecuritiesHeldPostAcquistionOrDisposalAbstract",	"SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity",
        "SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding",
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract",
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate",
        "DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate",
        "DateOfIntimationToCompany",	"TypeOfContract",	"ContractSpecification",
        "BuyAbstract",	"BuyNotionalValue",	"BuyNumberOfUnits",	"SellAbstract",	"NotionalValue",
        "NumberOfUnits",	"ExchangeOnWhichTheTradeWasExecuted",	"TotalValueInAggregate",
        "NameOfTheSignatory",	"DesignationOfSignatory",	"Place",	"DateOfFiling",
        "DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock",
        "ModeOfAcquisitionOrDisposal",	"GeneralInformationAbstract",	"SourceSystem",	"InsertDate"]
