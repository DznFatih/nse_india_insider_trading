

--CREATE DATABASE
create database dwh;


-- CREATE LOGIN AND DWH USER

use [master]
CREATE LOGIN [sql_user] WITH PASSWORD=N'sql_user[]', DEFAULT_DATABASE=[dwh], CHECK_EXPIRATION=OFF, CHECK_POLICY=ON
ALTER SERVER ROLE [sysadmin] ADD MEMBER [sql_user]
USE [dwh]
CREATE USER [sql_user] FOR LOGIN [sql_user] WITH DEFAULT_SCHEMA=[dbo]


create schema extract_stage;


Create table extract_stage.InsiderTrading (
    Symbol	varchar(20)	Null	,
    CompanyName	varchar(20)	Null	,
    Regulation	varchar(20)	Null	,
    NameOfTheAcquirerORDisposer	nvarchar(400)	Null	,
    TypeOfSecurity	varchar(50)	Null	,
    NoOfSecurities	integer	Null	,
    AcquisitionORDisposal	varchar(20)	Null	,
    BroadcastDateTime	DateTime	Null	,
    XBRLLink	varchar(250)	Null	,
    Period	Date	Null	,
    ScripCode	varchar(20)	Null	,
    NSESymbol 	varchar(20)	Null	,
    MSEISymbol	varchar(50)	Null	,
    NameOfTheCompany	nvarchar(400)	Null	,
    WhetherISINAvailable	varchar(2)	Null	,
    ISINCode	varchar(50)	Null	,
    RevisedFilling	varchar(200)	Null	,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract	varchar(200)	Null	,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable	varchar(200)	Null	,
    ChangeInHoldingOfSecuritiesOfPromotersAxis	varchar(200)	Null	,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems	varchar(200)	Null	,
    TypeOfInstrument	varchar(50)	Null	,
    TypeOfInstrumentOthers	varchar(200)	Null	,
    CategoryOfPerson	varchar(200)	Null	,
    NameOfThePerson	nvarchar(400)	Null	,
    PANNumber	varchar(200)	Null	,
    IdentificationNumberOfDirectorOrCompany	varchar(200)	Null	,
    Address	varchar(200)	Null	,
    ContactNumber	varchar(200)	Null	,
    SecuritiesHeldPriorToAcquisitionOrDisposalAbstract	varchar(200)	Null	,
    SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity	decimal(19, 4)	Null	,
    SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding	decimal(19, 4)	Null	,
    SecuritiesAcquiredOrDisposedAbstract	varchar(200)	Null	,
    SecuritiesAcquiredOrDisposedNumberOfSecurity	decimal(19, 4)	Null	,
    SecuritiesAcquiredOrDisposedValueOfSecurity	decimal(19, 4)	Null	,
    SecuritiesAcquiredOrDisposedTransactionType	varchar(200)	Null	,
    SecuritiesHeldPostAcquistionOrDisposalAbstract	varchar(50)	Null	,
    SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity	decimal(19, 4)	Null	,
    SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding	decimal(19, 4)	Null	,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract	varchar(200)	Null	,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate	Date	Null	,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate	Date	Null	,
    DateOfIntimationToCompany	Date	Null	,
    TypeOfContract	varchar(200)	Null	,
    ContractSpecification	varchar(200)	Null	,
    BuyAbstract	varchar(200)	Null	,
    BuyNotionalValue	decimal(19, 4)	Null	,
    BuyNumberOfUnits	varchar(200)	Null	,
    SellAbstract	varchar(200)	Null	,
    NotionalValue	decimal(19, 4)	Null	,
    NumberOfUnits	varchar(200)	Null	,
    ExchangeOnWhichTheTradeWasExecuted	varchar(200)	Null	,
    TotalValueInAggregate	decimal(19, 4)	Null	,
    NameOfTheSignatory	varchar(200)	Null	,
    DesignationOfSignatory	varchar(200)	Null	,
    Place	varchar(200)	Null	,
    DateOfFiling	varchar(200)	Null	,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock	varchar(50)	Null	,
    ModeOfAcquisitionOrDisposal	varchar(200)	Null	,
    GeneralInformationAbstract	varchar(200)	Null	,
    DataSource	varchar(200)	Not Null	,
    InsertDate	varchar(200)	Not Null	,
);
