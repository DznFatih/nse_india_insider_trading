

--CREATE DATABASE
create database dwh;


create table dwh.dbo.Statisticts
 (
	ID int IDENTITY(1,1) NOT NULL,
 	OperationName VARCHAR(100) NOT NULL, -- Insert or Update
 	ReadRowCount int NOT NULL,
	UpdatedDataRowCount int NOT NULL,
	InsertRowCount int NOT NULL,
 	StartedAt DATETIME NOT NULL,
 	FinishedAt DATETIME NOT NULL,
	RunTimeInSeconds int NOT NULL
 );


drop table if exists dwh.dbo.NSEDataBulkInsert
CREATE TABLE dbo.NSEDataBulkInsert(
	AcquisitionMode	nvarchar(200)	Null,
	AcquisitionfromDate	nvarchar(200)	Null,
	AcquisitionToDate	nvarchar(200)	Null,
	AfterAcquisitionSharesNo	nvarchar(200)	Null,
	AfterAcquisitionSharesPercentage	nvarchar(200)	Null,
	BeforeAcquisitionSharesNo	nvarchar(200)	Null,
	BeforeAcquisitionSharesPercentage	nvarchar(200)	Null,
	BuyQuantity	nvarchar(200)	Null,
	BuyValue	nvarchar(200)	Null,
	DerivativeType	nvarchar(200)	Null,
	Did	nvarchar(200)	Null,
	Exchange	nvarchar(200)	Null,
	IntimDate	nvarchar(200)	Null,
	PID	nvarchar(200)	Null,
	Remarks	nvarchar(200)	Null,
	SecuritiesValue	nvarchar(200)	Null,
	SecuritiesTypePost	nvarchar(200)	Null,
	SellValue	nvarchar(200)	Null,
	TDPDerivativeContractType	nvarchar(200)	Null,
	TKDAcqm	nvarchar(200)	Null,
	Symbol nvarchar(50) NULL,
	CompanyName nvarchar(50) NULL,
	Regulation varchar(20) NULL,
	NameOfTheAcquirerORDisposer nvarchar(500) NULL,
	TypeOfSecurity nvarchar(50) NULL,
	NoOfSecurities int NULL,
	AcquisitionORDisposal nvarchar(50) NULL,
	BroadcastDateTime datetime2(7) NULL,
	XBRLLink varchar(250) NULL,
	Period date NULL,
	ScripCode varchar(20) NULL,
	NSESymbol nvarchar(50) NULL,
	MSEISymbol nvarchar(50) NULL,
	NameOfTheCompany nvarchar(400) NULL,
	WhetherISINAvailable varchar(50) NULL,
	ISINCode nvarchar(50) NULL,
	RevisedFilling nvarchar(200) NULL,
	DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract nvarchar(200) NULL,
	DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable nvarchar(200) NULL,
	ChangeInHoldingOfSecuritiesOfPromotersAxis nvarchar(200) NULL,
	DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems nvarchar(200) NULL,
	TypeOfInstrumentOthers nvarchar(200) NULL,
	TypeOfInstrument nvarchar(200) NULL,
	CategoryOfPerson nvarchar(200) NULL,
	NameOfThePerson nvarchar(500) NULL,
	PANNumber nvarchar(1) NULL,
	IdentificationNumberOfDirectorOrCompany nvarchar(200) NULL,
	Address nvarchar(200) NULL,
	ContactNumber nvarchar(200) NULL,
	SecuritiesHeldPriorToAcquisitionOrDisposalAbstract nvarchar(200) NULL,
	SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity varchar(300) NULL,
	SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding varchar(300) NULL,
	SecuritiesAcquiredOrDisposedAbstract nvarchar(200) NULL,
	SecuritiesAcquiredOrDisposedNumberOfSecurity varchar(300) NULL,
	SecuritiesAcquiredOrDisposedValueOfSecurity varchar(300) NULL,
	SecuritiesAcquiredOrDisposedTransactionType nvarchar(200) NULL,
	SecuritiesHeldPostAcquistionOrDisposalAbstract nvarchar(200) NULL,
	SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity nvarchar(200) NULL,
	SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding nvarchar(200) NULL,
	DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract nvarchar(200) NULL,
	DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate date NULL,
	DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate date NULL,
	DateOfIntimationToCompany date NULL,
	TypeOfContract nvarchar(200) NULL,
	ContractSpecification nvarchar(200) NULL,
	BuyAbstract nvarchar(200) NULL,
	BuyNotionalValue nvarchar(200) NULL,
	BuyNumberOfUnits nvarchar(200) NULL,
	SellAbstract nvarchar(200) NULL,
	NotionalValue nvarchar(200) NULL,
	NumberOfUnits varchar(200) NULL,
	ExchangeOnWhichTheTradeWasExecuted varchar(200) NULL,
	TotalValueInAggregate varchar(200) NULL,
	NameOfTheSignatory nvarchar(200) NULL,
	DesignationOfSignatory nvarchar(200) NULL,
	Place nvarchar(200) NULL,
	DateOfFiling nvarchar(200) NULL,
	DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock nvarchar(200) NULL,
	ModeOfAcquisitionOrDisposal nvarchar(200) NULL,
	GeneralInformationAbstract nvarchar(200) NULL,
	Currency nvarchar(200) NULL,
	DownloadDate datetime NULL
)


drop table if exists dwh.dbo.NSEDataCleaned
create table dbo.NSEDataCleaned (
    ID int IDENTITY(1, 1) NOT NULL,
    RowID_PK varchar(500) NOT NULL,
    AcquisitionMode	varchar(250)	Null,
	AcquisitionfromDate	Date	Null,
	AcquisitionToDate	Date	Null,
	AfterAcquisitionSharesNo	nvarchar(200)	Null,
	AfterAcquisitionSharesPercentage	decimal(19,2)	Null,
	BeforeAcquisitionSharesNo	nvarchar(200)	Null,
	BeforeAcquisitionSharesPercentage	decimal(19,2)	Null,
	BuyQuantity	integer	Null,
	BuyValue	Integer	Null,
	DerivativeType	nvarchar(200)	Null,
	Did	integer	Null,
	Exchange	nvarchar(200)	Null,
	IntimDate	Date	Null,
	PID	integer	Null,
	Remarks	nvarchar(200)	Null,
	SecuritiesValue	decimal(19,4)	Null,
	SecuritiesTypePost	nvarchar(200)	Null,
	SellValue	integer	Null,
	TDPDerivativeContractType	nvarchar(200)	Null,
	TKDAcqm	nvarchar(200)	Null,
    Symbol nvarchar(50) Null,
    CompanyName nvarchar(50) Null,
    Regulation varchar(20) Null,
    NameOfTheAcquirerORDisposer nvarchar(500) Null,
    TypeOfSecurity nvarchar(50) Null,
    NoOfSecurities integer Null,
    AcquisitionORDisposal nvarchar(50) Null,
    BroadcastDateTime DateTime Null,
    XBRLLink varchar(250) Null,
    Period Date Null,
    ScripCode varchar(20) Null,
    NSESymbol nvarchar(50) Null,
    MSEISymbol nvarchar(50) Null,
    NameOfTheCompany nvarchar(400) Null,
    WhetherISINAvailable varchar(50) Null,
    ISINCode nvarchar(50) Null,
    RevisedFilling nvarchar(200) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract nvarchar(200) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable nvarchar(200) Null,
    ChangeInHoldingOfSecuritiesOfPromotersAxis nvarchar(200) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems nvarchar(200) Null,
    TypeOfInstrument nvarchar(200) Null,
    TypeOfInstrumentOthers nvarchar(200) Null,
    CategoryOfPerson nvarchar(200) Null,
    NameOfThePerson nvarchar(500) Null,
    PANNumber nvarchar(200) Null,
    IdentificationNumberOfDirectorOrCompany nvarchar(200) Null,
    Address nvarchar(200) Null,
    ContactNumber nvarchar(200) Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalAbstract nvarchar(200) Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity integer Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    SecuritiesAcquiredOrDisposedAbstract nvarchar(200) Null,
    SecuritiesAcquiredOrDisposedNumberOfSecurity integer Null,
    SecuritiesAcquiredOrDisposedValueOfSecurity decimal(19, 4) Null,
    SecuritiesAcquiredOrDisposedTransactionType nvarchar(200) Null,
    SecuritiesHeldPostAcquistionOrDisposalAbstract nvarchar(200) Null,
    SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity integer Null,
    SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate Date Null,
    DateOfIntimationToCompany Date Null,
    TypeOfContract nvarchar(200) Null,
    ContractSpecification nvarchar(200) Null,
    BuyAbstract nvarchar(200) Null,
    BuyNotionalValue decimal(19, 4) Null,
    BuyNumberOfUnits integer Null,
    SellAbstract nvarchar(200) Null,
    NotionalValue decimal(19, 4) Null,
    NumberOfUnits integer Null,
    ExchangeOnWhichTheTradeWasExecuted varchar(200) Null,
    TotalValueInAggregate decimal(19, 4) Null,
    NameOfTheSignatory nvarchar(200) Null,
    DesignationOfSignatory nvarchar(200) Null,
    Place nvarchar(200) Null,
    DateOfFiling nvarchar(200) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock nvarchar(200) Null,
    ModeOfAcquisitionOrDisposal nvarchar(200) Null,
    GeneralInformationAbstract nvarchar(200) Null,
    Currency nvarchar(200) Null,
    UpdateDate Datetime Not Null,
    InsertDate DateTime Not Null

	, CONSTRAINT PK_NSEDataCleaned_RowID_PK PRIMARY KEY CLUSTERED (RowID_PK)
)



--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================

------------------------------------------------------------------- INITIAL LOAD To Clean Table


drop table if exists dbo.temp_update_table;

Select
	COALESCE(Symbol,'') + COALESCE(AcquisitionMode,'') + COALESCE(CompanyName,'') + COALESCE(NameOfTheAcquirerORDisposer,'') + COALESCE(TypeOfSecurity,'')
	+ COALESCE(cast(NoOfSecurities as varchar(100)),'') + COALESCE(AcquisitionORDisposal,'') + COALESCE(cast(AfterAcquisitionSharesNo as nvarchar(200)),'')
	+ COALESCE(cast(AfterAcquisitionSharesPercentage as nvarchar(200)),'') + COALESCE(cast(BeforeAcquisitionSharesNo as nvarchar(200)),'')
	+ COALESCE(cast(BeforeAcquisitionSharesPercentage as nvarchar(200)),'') + COALESCE(cast(BuyQuantity as nvarchar(200)),'') + COALESCE(cast(SecuritiesValue as nvarchar(200)),'')
	+ COALESCE(cast(BuyValue as nvarchar(200)),'') + COALESCE(cast(BroadcastDateTime as nvarchar(200)),'') as RowID_PK
	,AcquisitionMode
	,case
		when AcquisitionfromDate = '-' then Null
		else cast(AcquisitionfromDate as date)
	end AcquisitionfromDate
	,case
		when AcquisitionToDate = '-' then Null
		else cast(AcquisitionToDate as date)
	end AcquisitionToDate
	,Cast(AfterAcquisitionSharesNo as nvarchar(200)) as AfterAcquisitionSharesNo
	, case
		when AfterAcquisitionSharesPercentage = '-' then Null
		else Cast(AfterAcquisitionSharesPercentage as decimal(19,2))
	end AfterAcquisitionSharesPercentage
	,Cast(BeforeAcquisitionSharesNo as nvarchar(200)) as BeforeAcquisitionSharesNo
	,case
		when BeforeAcquisitionSharesPercentage = '-' then Null
		else Cast(BeforeAcquisitionSharesPercentage as decimal(19,2))
	end BeforeAcquisitionSharesPercentage
	,Cast(BuyQuantity as integer) as BuyQuantity
	,Cast(BuyValue as integer) as BuyValue
	,DerivativeType
	,Cast(Did as integer) as Did
	,Exchange
	,case
		when IntimDate = '-' then Null
		else cast(IntimDate as date)
	end IntimDate
	,Cast(PID as integer) as PID
	,Remarks
	,case
		when SecuritiesValue = '-' then Null
		else Cast(SecuritiesValue as decimal(19,4))
	end SecuritiesValue
	,SecuritiesTypePost
	,Cast(SellValue as integer) as SellValue
	,TDPDerivativeContractType
	,TKDAcqm
	,Symbol
	,CompanyName
	,Regulation
	,NameOfTheAcquirerORDisposer
	,TypeOfSecurity
	,Cast(NoOfSecurities as integer) as NoOfSecurities
	,AcquisitionORDisposal
	,Cast(BroadcastDateTime as smalldatetime) as BroadcastDateTime
	,XBRLLink
	,Period
	,ScripCode
	,NSESymbol
	,MSEISymbol
	,NameOfTheCompany
	,WhetherISINAvailable
	,ISINCode
	,RevisedFilling
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable
	,ChangeInHoldingOfSecuritiesOfPromotersAxis
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems
	,TypeOfInstrument
	,TypeOfInstrumentOthers
	,CategoryOfPerson
	,NameOfThePerson
	,PANNumber
	,IdentificationNumberOfDirectorOrCompany
	,Address
	,ContactNumber
	,SecuritiesHeldPriorToAcquisitionOrDisposalAbstract
	,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity as integer) as SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity
	,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding
	,SecuritiesAcquiredOrDisposedAbstract
	,Cast(SecuritiesAcquiredOrDisposedNumberOfSecurity as integer) as SecuritiesAcquiredOrDisposedNumberOfSecurity
	,Cast(SecuritiesAcquiredOrDisposedValueOfSecurity as decimal(19,4)) as SecuritiesAcquiredOrDisposedValueOfSecurity
	,SecuritiesAcquiredOrDisposedTransactionType
	,SecuritiesHeldPostAcquistionOrDisposalAbstract
	,cast(SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity as Integer) SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity
	,Cast(SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate
	,cast(DateOfIntimationToCompany as date) as DateOfIntimationToCompany
	,TypeOfContract
	,ContractSpecification
	,BuyAbstract
	,Cast(BuyNotionalValue as decimal(19, 4)) as BuyNotionalValue
	,Cast(BuyNumberOfUnits as integer) as BuyNumberOfUnits
	,SellAbstract
	,Cast(NotionalValue as decimal(19, 4)) as NotionalValue
	,Cast(NumberOfUnits as integer) as NumberOfUnits
	,ExchangeOnWhichTheTradeWasExecuted
	,Cast(TotalValueInAggregate as decimal(19, 4)) as TotalValueInAggregate
	,NameOfTheSignatory
	,DesignationOfSignatory
	,Place
	,DateOfFiling
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock
	,ModeOfAcquisitionOrDisposal
	,GeneralInformationAbstract
	,Currency
	,GETDATE() as InsertDate
	,GETDATE() as UpdateDate
into dbo.temp_update_table
from dbo.NSEDataBulkInsert

insert into dbo.NSEDataCleaned
Select *
from dbo.temp_update_table

drop table if exists dbo.temp_update_table;

--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================


 /*
BEGIN TRANSACTION

	declare @EmployeeID int = 17;

	declare @Name varchar(max) = 'Bidisha';

	IF EXISTS ( Select * from dbo.Employee with (UPDLOCK, SERIALIZABLE) where EmpID = @EmployeeID)

	Update dbo.EMployee

	Set Name = @Name

	Where EmpID = @EmployeeID

	ELSE

	INSERT into dbo.Employee (EmpID, Name)

	VALUES

	(@EmployeeID, @Name)

COMMIT TRANSACTION;
*/


create database dwh;
GO

use dwh
GO

truncate table dwh.dbo.NSEDataBulkInsert;

bulk insert dwh.dbo.NSEDataBulkInsert
from 'C:\Users\dznfa\OneDrive\Desktop\NSETrading\app\XBRL Files\01022024095541\NSE Data.txt'
with
(
	FORMAT = 'CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = '|',
	MAXERRORS = 10,
	ROWTERMINATOR = '\n'
)

-- UPDATE
drop table if exists dbo.temp_update_table;

Select
	 Symbol
	,CompanyName
	,Regulation
	,NameOfTheAcquirerORDisposer
	,TypeOfSecurity
	,Cast(NoOfSecurities as integer) as NoOfSecurities
	,AcquisitionORDisposal
	,Cast(BroadcastDateTime as smalldatetime) as BroadcastDateTime
	,XBRLLink
	,Period
	,ScripCode
	,NSESymbol
	,MSEISymbol
	,NameOfTheCompany
	,WhetherISINAvailable
	,ISINCode
	,RevisedFilling
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable
	,ChangeInHoldingOfSecuritiesOfPromotersAxis
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems
	,TypeOfInstrument
	,TypeOfInstrumentOthers
	,CategoryOfPerson
	,NameOfThePerson
	,PANNumber
	,IdentificationNumberOfDirectorOrCompany
	,Address
	,ContactNumber
	,SecuritiesHeldPriorToAcquisitionOrDisposalAbstract
	,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity as integer) as SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity
	,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding
	,SecuritiesAcquiredOrDisposedAbstract
	,Cast(SecuritiesAcquiredOrDisposedNumberOfSecurity as integer) as SecuritiesAcquiredOrDisposedNumberOfSecurity
	,Cast(SecuritiesAcquiredOrDisposedValueOfSecurity as decimal(19,4)) as SecuritiesAcquiredOrDisposedValueOfSecurity
	,SecuritiesAcquiredOrDisposedTransactionType
	,SecuritiesHeldPostAcquistionOrDisposalAbstract
	,cast(SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity as Integer) SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity
	,Cast(SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate
	,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate
	,cast(DateOfIntimationToCompany as date) as DateOfIntimationToCompany
	,TypeOfContract
	,ContractSpecification
	,BuyAbstract
	,Cast(BuyNotionalValue as decimal(19, 4)) as BuyNotionalValue
	,Cast(BuyNumberOfUnits as integer) as BuyNumberOfUnits
	,SellAbstract
	,Cast(NotionalValue as decimal(19, 4)) as NotionalValue
	,Cast(NumberOfUnits as integer) as NumberOfUnits
	,ExchangeOnWhichTheTradeWasExecuted
	,Cast(TotalValueInAggregate as decimal(19, 4)) as TotalValueInAggregate
	,NameOfTheSignatory
	,DesignationOfSignatory
	,Place
	,DateOfFiling
	,DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock
	,ModeOfAcquisitionOrDisposal
	,GeneralInformationAbstract
	,Currency
	,GETDATE() as InsertDate
	,GETDATE() as UpdateDate
into dbo.temp_update_table
from dbo.NSEDataBulkInsert


insert into dwh.dbo.NSEDataCleaned
Select *
from dbo.temp_update_table




with primary_key as (
Select
	  COALESCE(Symbol,'') + COALESCE(AcquisitionMode,'') + COALESCE(CompanyName,'') + COALESCE(NameOfTheAcquirerORDisposer,'') + COALESCE(TypeOfSecurity,'')
	+ COALESCE(cast(NoOfSecurities as varchar(100)),'') + COALESCE(AcquisitionORDisposal,'') + COALESCE(cast(AfterAcquisitionSharesNo as nvarchar(200)),'')
	+ COALESCE(cast(AfterAcquisitionSharesPercentage as nvarchar(200)),'') + COALESCE(cast(BeforeAcquisitionSharesNo as nvarchar(200)),'')
	+ COALESCE(cast(BeforeAcquisitionSharesPercentage as nvarchar(200)),'') + COALESCE(cast(BuyQuantity as nvarchar(200)),'') + COALESCE(cast(SecuritiesValue as nvarchar(200)),'')
	+ COALESCE(cast(BuyValue as nvarchar(200)),'') + COALESCE(cast(BroadcastDateTime as nvarchar(200)),'') as key_
from dwh.dbo.NSEDataCleaned
), rw_num as (
Select
	 *
	,ROW_NUMBER() over (partition by key_ order by key_) as rw
from primary_key
)
Select *
from rw_num
--where rw > 1




