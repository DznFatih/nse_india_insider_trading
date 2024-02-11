

drop table if exists dbo.NSEDataBulkInsert
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
	CompanyName nvarchar(200) NULL,
	Regulation varchar(20) NULL,
	NameOfTheAcquirerORDisposer nvarchar(500) NULL,
	TypeOfSecurity nvarchar(200) NULL,
	NoOfSecurities int NULL,
	AcquisitionORDisposal nvarchar(200) NULL,
	BroadcastDateTime datetime2(7) NULL,
	XBRLLink varchar(250) NULL,
	Period date NULL,
	ScripCode varchar(20) NULL,
	NSESymbol nvarchar(200) NULL,
	MSEISymbol nvarchar(200) NULL,
	NameOfTheCompany nvarchar(400) NULL,
	WhetherISINAvailable varchar(200) NULL,
	ISINCode nvarchar(200) NULL,
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
    IsOrphan	varchar(10)	Not Null,
	DownloadDate datetime NULL
)


drop table if exists dbo.NSEDataCleaned
create table dbo.NSEDataCleaned (
    ID int IDENTITY(1, 1) NOT NULL,
    AcquisitionMode	varchar(250)	Null,
	AcquisitionfromDate	Date	Null,
	AcquisitionToDate	Date	Null,
	AfterAcquisitionSharesNo	nvarchar(200)	Null,
	AfterAcquisitionSharesPercentage	decimal(19,2)	Null,
	BeforeAcquisitionSharesNo	nvarchar(200)	Null,
	BeforeAcquisitionSharesPercentage	decimal(19,2)	Null,
	BuyQuantity	bigint	Null,
	BuyValue	decimal(19, 4)	Null,
	DerivativeType	nvarchar(200)	Null,
	Did	bigint	NOT Null,
	Exchange	nvarchar(200)	Null,
	IntimDate	Date	Null,
	PID	bigint	Null,
	Remarks	nvarchar(200)	Null,
	SecuritiesValue	decimal(19,4)	Null,
	SecuritiesTypePost	nvarchar(200)	Null,
	SellValue	decimal(19, 4)	Null,
	TDPDerivativeContractType	nvarchar(200)	Null,
	TKDAcqm	nvarchar(200)	Null,
    Symbol nvarchar(200) Null,
    CompanyName nvarchar(200) Null,
    Regulation varchar(200) Null,
    NameOfTheAcquirerORDisposer nvarchar(500) Null,
    TypeOfSecurity nvarchar(200) Null,
    NoOfSecurities bigint Null,
    AcquisitionORDisposal nvarchar(200) Null,
    BroadcastDateTime DateTime Null,
    XBRLLink varchar(250) Null,
    Period Date Null,
    ScripCode varchar(200) Null,
    NSESymbol nvarchar(200) Null,
    MSEISymbol nvarchar(200) Null,
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
    SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity bigint Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    SecuritiesAcquiredOrDisposedAbstract nvarchar(200) Null,
    SecuritiesAcquiredOrDisposedNumberOfSecurity bigint Null,
    SecuritiesAcquiredOrDisposedValueOfSecurity decimal(19, 4) Null,
    SecuritiesAcquiredOrDisposedTransactionType nvarchar(200) Null,
    SecuritiesHeldPostAcquistionOrDisposalAbstract nvarchar(200) Null,
    SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity bigint Null,
    SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate Date Null,
    DateOfIntimationToCompany Date Null,
    TypeOfContract nvarchar(200) Null,
    ContractSpecification nvarchar(200) Null,
    BuyAbstract nvarchar(200) Null,
    BuyNotionalValue decimal(19, 4) Null,
    BuyNumberOfUnits bigint Null,
    SellAbstract nvarchar(200) Null,
    NotionalValue decimal(19, 4) Null,
    NumberOfUnits bigint Null,
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
    IsOrphan	varchar(10)	Not Null,
    UpdateDate Datetime Not Null,
    InsertDate DateTime Not Null

	, CONSTRAINT PK_NSEDataCleaned_DID PRIMARY KEY CLUSTERED (Did)
)



drop table if exists dbo.PythonETLStatistics
create table dbo.PythonETLStatistics (
    PythonScriptExecutionStartTime varchar(200) NOT NULL,
    DataDescription	varchar(200)	Null,
	NSESearchPageVisitTime	varchar(200)	Null,
	XBRLDocumentDownloadsStartTime	varchar(200)	Null,
	XBRLDocumentDownloadsEndTime	varchar(200)	Null,
	XBRLDocumentPageVisitAttemptCount	varchar(200)	Null,
	XBRLDocumentDownloadErrorCount	varchar(200)	Null,
	XBRLDocumentDownloadSuccessCount	varchar(200)	Null,
	DocumentDownloadErrors NVARCHAR(MAX) Null
);


drop table if exists dbo.SQLETLStatisticts
create table dbo.SQLETLStatisticts
 (
	ID int IDENTITY(1,1) NOT NULL,
	SQLETLStartTime DATETIME NOT NULL,
	RowsBulkInserted int NOT NULL,
	PreviouslyInsertedRowsUpdated  int NOT NULL,
	NewRecordsInserted int NOT NULL,
	SQLETLFinishTime DATETIME NOT NULL,
	NumberOfRecordsInDestinationTableAfterExecution int NOT NULL,
	ErrorMessage NVARCHAR(MAX) Null
 );

--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================


----------------------------------------------------------- SP_SQLETLStatisticts Procedure



CREATE  or ALTER PROCEDURE dbo.SP_SQLETLStatisticts @SQLETLStartTime DateTime,
											@RowsBulkInserted int,
											@PreviouslyInsertedRowsUpdated int,
											@NewRecordsInserted int,
											@SQLETLFinishTime datetime,
											@NumberOfRecordsInDestinationTableAfterExecution int,
											@ErrorMessage NVARCHAR(MAX)
AS
BEGIN

	Insert into dbo.SQLETLStatisticts
		(SQLETLStartTime,
		RowsBulkInserted,
		PreviouslyInsertedRowsUpdated,
		NewRecordsInserted,
		SQLETLFinishTime,
		NumberOfRecordsInDestinationTableAfterExecution,
		ErrorMessage)
	values
		(@SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime, @NumberOfRecordsInDestinationTableAfterExecution, @ErrorMessage)
END



--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================


--------------------------------------------------------------  dbo.SP_NSEDataCleaned



create or alter procedure dbo.SP_NSEDatainsert @folder_path varchar(100)
AS
BEGIN

	--DECLARE @folder_path NVARCHAR(max) = 'C:\Users\dznfa\OneDrive\Desktop\NSETrading\app\XBRL Files\10022024103954';

	DECLARE @python_etl_stat_file_name NVARCHAR(max) = 'metadata.txt';
	DECLARE @nse_data_bulk_file_name NVARCHAR(max) = 'NSEData.txt';
	DECLARE @python_etl_stat_file_path NVARCHAR(max) = @folder_path + '/' + @python_etl_stat_file_name;
	DECLARE @nse_data_bulk_file_path NVARCHAR(max) = @folder_path + '/' + @nse_data_bulk_file_name;
	declare @SQLETLStartTime datetime = (Select GETDATE())
	declare @RowsBulkInserted int = 0;
	declare @PreviouslyInsertedRowsUpdated int = 0;
	declare @NewRecordsInserted int = 0;
	declare @SQLETLFinishTime datetime;
	declare @NumberOfRecordsInDestinationTableAfterExecution int = (Select count(1) from dbo.NSEDataCleaned);
	DECLARE @ErrorMessage NVARCHAR(max);

	/*
		Dynamic query to run later. We concatinate bulk insert sql statement with file path.
	*/

	Declare @sql_nse_data_bulk nvarchar(500) = 'bulk insert dbo.NSEDataBulkInsert
								  from ''' +  @nse_data_bulk_file_path + '''
								  with
								  (
										FORMAT = ''CSV'',
										FIRSTROW = 2,
										FIELDTERMINATOR = ''|'',
										MAXERRORS = 10,
										ROWTERMINATOR = ''\n''
								   )'

    Declare @sql_python_stats nvarchar(500) = 'bulk insert dbo.PythonETLStatistics
									  from ''' +  @python_etl_stat_file_path + '''
									  with
									  (
											FORMAT = ''CSV'',
											FIRSTROW = 2,
											FIELDTERMINATOR = ''|'',
											MAXERRORS = 10,
											ROWTERMINATOR = ''\n''
									   )'

	truncate table dbo.NSEDataBulkInsert;
	truncate table dbo.PythonETLStatistics;


BEGIN TRY
	BEGIN TRANSACTION

	/*
		Execute dynamic sql to bulk insert Python script metadata and NSE data.
	*/

	exec(@sql_nse_data_bulk)
	exec(@sql_python_stats)

	/*
		Row count of bulk insert data into dbo.NSEDataBulkInsert. This is for SQL ETL statistics table.
	*/

	Set @RowsBulkInserted = (Select count(1) from dbo.NSEDataBulkInsert)

	/*
		Delete temporary table if it exists
	*/

	drop table if exists dbo.temp_update_table;

	/*
		Select data from table in which we bulk inserted NSEData in previous step. The data is transformed into correct datatype to match with target table,
		i.e. from nvarchar to date or from nvarchar to bigint.
	*/

	Select
		 AcquisitionMode
		,cast(AcquisitionfromDate as date) AS AcquisitionfromDate
		,cast(AcquisitionToDate as date) AS AcquisitionToDate
		,Cast(AfterAcquisitionSharesNo as nvarchar(200)) as AfterAcquisitionSharesNo
		,Cast(AfterAcquisitionSharesPercentage as decimal(19,2)) AS AfterAcquisitionSharesPercentage
		,Cast(BeforeAcquisitionSharesNo as nvarchar(200)) as BeforeAcquisitionSharesNo
		,Cast(BeforeAcquisitionSharesPercentage as decimal(19,2)) AS BeforeAcquisitionSharesPercentage
		,Cast(BuyQuantity as bigint) as BuyQuantity
		,Cast(BuyValue as bigint) as BuyValue
		,DerivativeType
		,Cast(Did as bigint) as Did
		,Exchange
		,cast(IntimDate as date) AS IntimDate
		,Cast(PID as bigint) as PID
		,Remarks
		,Cast(SecuritiesValue as decimal(19,4)) AS SecuritiesValue
		,SecuritiesTypePost
		,Cast(SellValue as bigint) as SellValue
		,TDPDerivativeContractType
		,TKDAcqm
		,Symbol
		,CompanyName
		,Regulation
		,NameOfTheAcquirerORDisposer
		,TypeOfSecurity
		,Cast(NoOfSecurities as bigint) as NoOfSecurities
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
		,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity as bigint) as SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity
		,Cast(SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding
		,SecuritiesAcquiredOrDisposedAbstract
		,Cast(SecuritiesAcquiredOrDisposedNumberOfSecurity as bigint) as SecuritiesAcquiredOrDisposedNumberOfSecurity
		,Cast(SecuritiesAcquiredOrDisposedValueOfSecurity as decimal(19,4)) as SecuritiesAcquiredOrDisposedValueOfSecurity
		,SecuritiesAcquiredOrDisposedTransactionType
		,SecuritiesHeldPostAcquistionOrDisposalAbstract
		,cast(SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity as bigint) SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity
		,Cast(SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding as decimal(19,2)) as SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding
		,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract
		,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate
		,cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate as date) as DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate
		,cast(DateOfIntimationToCompany as date) as DateOfIntimationToCompany
		,TypeOfContract
		,ContractSpecification
		,BuyAbstract
		,Cast(BuyNotionalValue as decimal(19, 4)) as BuyNotionalValue
		,Cast(BuyNumberOfUnits as bigint) as BuyNumberOfUnits
		,SellAbstract
		,Cast(NotionalValue as decimal(19, 4)) as NotionalValue
		,Cast(NumberOfUnits as bigint) as NumberOfUnits
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
		,IsOrphan
		,GETDATE() as InsertDate
		,GETDATE() as UpdateDate
	into dbo.temp_update_table
	from dbo.NSEDataBulkInsert

	-- UPDATE DATA

	/*
		Before running update statement, we want to check if new data exist in our target table with an if statement.
		If it returns True then, update statement will be executed.
	*/

	if exists (Select count(1) from dbo.temp_update_table where Did in (Select Did from dbo.NSEDataCleaned))

		Update dbo.NSEDataCleaned
		set AcquisitionMode = temp_.AcquisitionMode,
			AcquisitionfromDate = temp_.AcquisitionfromDate,
			AcquisitionToDate = temp_.AcquisitionToDate,
			AfterAcquisitionSharesNo = temp_.AfterAcquisitionSharesNo,
			AfterAcquisitionSharesPercentage = temp_.AfterAcquisitionSharesPercentage,
			BeforeAcquisitionSharesNo = temp_.BeforeAcquisitionSharesNo,
			BeforeAcquisitionSharesPercentage = temp_.BeforeAcquisitionSharesPercentage,
			BuyQuantity = temp_.BuyQuantity,
			BuyValue = temp_.BuyValue,
			DerivativeType = temp_.DerivativeType,
			Did = temp_.Did,
			Exchange = temp_.Exchange,
			IntimDate = temp_.IntimDate,
			PID = temp_.PID,
			Remarks = temp_.Remarks,
			SecuritiesValue = temp_.SecuritiesValue,
			SecuritiesTypePost = temp_.SecuritiesTypePost,
			SellValue = temp_.SellValue,
			TDPDerivativeContractType = temp_.TDPDerivativeContractType,
			TKDAcqm = temp_.TKDAcqm,
			Symbol = temp_.Symbol,
			CompanyName = temp_.CompanyName,
			Regulation = temp_.Regulation,
			NameOfTheAcquirerORDisposer = temp_.NameOfTheAcquirerORDisposer,
			TypeOfSecurity = temp_.TypeOfSecurity,
			NoOfSecurities = temp_.NoOfSecurities,
			AcquisitionORDisposal = temp_.AcquisitionORDisposal,
			BroadcastDateTime = temp_.BroadcastDateTime,
			XBRLLink = temp_.XBRLLink,
			Period = temp_.Period,
			ScripCode = temp_.ScripCode,
			NSESymbol = temp_.NSESymbol,
			MSEISymbol = temp_.MSEISymbol,
			NameOfTheCompany = temp_.NameOfTheCompany,
			WhetherISINAvailable = temp_.WhetherISINAvailable,
			ISINCode = temp_.ISINCode,
			RevisedFilling = temp_.RevisedFilling,
			DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract = temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract,
			DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable = temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable,
			ChangeInHoldingOfSecuritiesOfPromotersAxis = temp_.ChangeInHoldingOfSecuritiesOfPromotersAxis,
			DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems = temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems,
			TypeOfInstrument = temp_.TypeOfInstrument,
			TypeOfInstrumentOthers = temp_.TypeOfInstrumentOthers,
			CategoryOfPerson = temp_.CategoryOfPerson,
			NameOfThePerson = temp_.NameOfThePerson,
			PANNumber = temp_.PANNumber,
			IdentificationNumberOfDirectorOrCompany = temp_.IdentificationNumberOfDirectorOrCompany,
			Address = temp_.Address,
			ContactNumber = temp_.ContactNumber,
			SecuritiesHeldPriorToAcquisitionOrDisposalAbstract = temp_.SecuritiesHeldPriorToAcquisitionOrDisposalAbstract,
			SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity = temp_.SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity,
			SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding = temp_.SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding,
			SecuritiesAcquiredOrDisposedAbstract = temp_.SecuritiesAcquiredOrDisposedAbstract,
			SecuritiesAcquiredOrDisposedNumberOfSecurity = temp_.SecuritiesAcquiredOrDisposedNumberOfSecurity,
			SecuritiesAcquiredOrDisposedValueOfSecurity = temp_.SecuritiesAcquiredOrDisposedValueOfSecurity,
			SecuritiesAcquiredOrDisposedTransactionType = temp_.SecuritiesAcquiredOrDisposedTransactionType,
			SecuritiesHeldPostAcquistionOrDisposalAbstract = temp_.SecuritiesHeldPostAcquistionOrDisposalAbstract,
			SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity = temp_.SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity,
			SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding = temp_.SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding,
			DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract = temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract,
			DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate = temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate,
			DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate = temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate,
			DateOfIntimationToCompany = temp_.DateOfIntimationToCompany,
			TypeOfContract = temp_.TypeOfContract,
			ContractSpecification = temp_.ContractSpecification,
			BuyAbstract = temp_.BuyAbstract,
			BuyNotionalValue = temp_.BuyNotionalValue,
			BuyNumberOfUnits = temp_.BuyNumberOfUnits,
			SellAbstract = temp_.SellAbstract,
			NotionalValue = temp_.NotionalValue,
			NumberOfUnits = temp_.NumberOfUnits,
			ExchangeOnWhichTheTradeWasExecuted = temp_.ExchangeOnWhichTheTradeWasExecuted,
			TotalValueInAggregate = temp_.TotalValueInAggregate,
			NameOfTheSignatory = temp_.NameOfTheSignatory,
			DesignationOfSignatory = temp_.DesignationOfSignatory,
			Place = temp_.Place,
			DateOfFiling = temp_.DateOfFiling,
			DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock = temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock,
			ModeOfAcquisitionOrDisposal = temp_.ModeOfAcquisitionOrDisposal,
			GeneralInformationAbstract = temp_.GeneralInformationAbstract,
			isorphan = temp_.isorphan,
			Currency = temp_.Currency,
			UpdateDate = temp_.UpdateDate
		from dbo.NSEDataCleaned c
		inner join dbo.temp_update_table temp_
			on temp_.did = c.did
		where   c.AcquisitionMode <> temp_.AcquisitionMode or
				c.AcquisitionfromDate <> temp_.AcquisitionfromDate or
				c.AcquisitionToDate <> temp_.AcquisitionToDate or
				c.AfterAcquisitionSharesNo <> temp_.AfterAcquisitionSharesNo or
				c.AfterAcquisitionSharesPercentage <> temp_.AfterAcquisitionSharesPercentage or
				c.BeforeAcquisitionSharesNo <> temp_.BeforeAcquisitionSharesNo or
				c.BeforeAcquisitionSharesPercentage <> temp_.BeforeAcquisitionSharesPercentage or
				c.BuyQuantity <> temp_.BuyQuantity or
				c.BuyValue <> temp_.BuyValue or
				c.DerivativeType <> temp_.DerivativeType or
				c.Did <> temp_.Did or
				c.Exchange <> temp_.Exchange or
				c.IntimDate <> temp_.IntimDate or
				c.PID <> temp_.PID or
				c.Remarks <> temp_.Remarks or
				c.SecuritiesValue <> temp_.SecuritiesValue or
				c.SecuritiesTypePost <> temp_.SecuritiesTypePost or
				c.SellValue <> temp_.SellValue or
				c.TDPDerivativeContractType <> temp_.TDPDerivativeContractType or
				c.TKDAcqm <> temp_.TKDAcqm or
				c.Symbol <> temp_.Symbol or
				c.CompanyName <> temp_.CompanyName or
				c.Regulation <> temp_.Regulation or
				c.NameOfTheAcquirerORDisposer <> temp_.NameOfTheAcquirerORDisposer or
				c.TypeOfSecurity <> temp_.TypeOfSecurity or
				c.NoOfSecurities <> temp_.NoOfSecurities or
				c.AcquisitionORDisposal <> temp_.AcquisitionORDisposal or
				c.BroadcastDateTime <> temp_.BroadcastDateTime or
				c.XBRLLink <> temp_.XBRLLink or
				c.Period <> temp_.Period or
				c.ScripCode <> temp_.ScripCode or
				c.NSESymbol <> temp_.NSESymbol or
				c.MSEISymbol <> temp_.MSEISymbol or
				c.NameOfTheCompany <> temp_.NameOfTheCompany or
				c.WhetherISINAvailable <> temp_.WhetherISINAvailable or
				c.ISINCode <> temp_.ISINCode or
				c.RevisedFilling <> temp_.RevisedFilling or
				c.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract <> temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract or
				c.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable <> temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable or
				c.ChangeInHoldingOfSecuritiesOfPromotersAxis <> temp_.ChangeInHoldingOfSecuritiesOfPromotersAxis or
				c.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems <> temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems or
				c.TypeOfInstrument <> temp_.TypeOfInstrument or
				c.TypeOfInstrumentOthers <> temp_.TypeOfInstrumentOthers or
				c.CategoryOfPerson <> temp_.CategoryOfPerson or
				c.NameOfThePerson <> temp_.NameOfThePerson or
				c.PANNumber <> temp_.PANNumber or
				c.IdentificationNumberOfDirectorOrCompany <> temp_.IdentificationNumberOfDirectorOrCompany or
				c.Address <> temp_.Address or
				c.ContactNumber <> temp_.ContactNumber or
				c.SecuritiesHeldPriorToAcquisitionOrDisposalAbstract <> temp_.SecuritiesHeldPriorToAcquisitionOrDisposalAbstract or
				c.SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity <> temp_.SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity or
				c.SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding <> temp_.SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding or
				c.SecuritiesAcquiredOrDisposedAbstract <> temp_.SecuritiesAcquiredOrDisposedAbstract or
				c.SecuritiesAcquiredOrDisposedNumberOfSecurity <> temp_.SecuritiesAcquiredOrDisposedNumberOfSecurity or
				c.SecuritiesAcquiredOrDisposedValueOfSecurity <> temp_.SecuritiesAcquiredOrDisposedValueOfSecurity or
				c.SecuritiesAcquiredOrDisposedTransactionType <> temp_.SecuritiesAcquiredOrDisposedTransactionType or
				c.SecuritiesHeldPostAcquistionOrDisposalAbstract <> temp_.SecuritiesHeldPostAcquistionOrDisposalAbstract or
				c.SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity <> temp_.SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity or
				c.SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding <> temp_.SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding or
				c.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract <> temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract or
				c.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate <> temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate or
				c.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate <> temp_.DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate or
				c.DateOfIntimationToCompany <> temp_.DateOfIntimationToCompany or
				c.TypeOfContract <> temp_.TypeOfContract or
				c.ContractSpecification <> temp_.ContractSpecification or
				c.BuyAbstract <> temp_.BuyAbstract or
				c.BuyNotionalValue <> temp_.BuyNotionalValue or
				c.BuyNumberOfUnits <> temp_.BuyNumberOfUnits or
				c.SellAbstract <> temp_.SellAbstract or
				c.NotionalValue <> temp_.NotionalValue or
				c.NumberOfUnits <> temp_.NumberOfUnits or
				c.ExchangeOnWhichTheTradeWasExecuted <> temp_.ExchangeOnWhichTheTradeWasExecuted or
				c.TotalValueInAggregate <> temp_.TotalValueInAggregate or
				c.NameOfTheSignatory <> temp_.NameOfTheSignatory or
				c.DesignationOfSignatory <> temp_.DesignationOfSignatory or
				c.Place <> temp_.Place or
				c.DateOfFiling <> temp_.DateOfFiling or
				c.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock <> temp_.DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock or
				c.ModeOfAcquisitionOrDisposal <> temp_.ModeOfAcquisitionOrDisposal or
				c.GeneralInformationAbstract <> temp_.GeneralInformationAbstract or
				c.Currency <> temp_.Currency or
				c.isorphan <> temp_.isorphan
				set @PreviouslyInsertedRowsUpdated = @@ROWCOUNT


	-- INSERT NEW DATA

	/*
		Insert newly downloaded data into our target table.

		Before running an Insert statement, we first want to check with an if statement whether there is any new data comparing agaist our target table.
		If it returns True, then insert statement will be executed.

	*/
	set @NewRecordsInserted = (Select count(1) from dbo.temp_update_table where Did not in (Select Did from dbo.NSEDataCleaned))
	if exists (Select count(1) from dbo.temp_update_table where Did not in (Select Did from dbo.NSEDataCleaned))

		insert into dbo.NSEDataCleaned
		Select *
		from dbo.temp_update_table
		where Did not in (Select Did from dbo.NSEDataCleaned);

	drop table if exists dbo.temp_update_table;

	set @SQLETLFinishTime = (Select GETDATE())

	set @NumberOfRecordsInDestinationTableAfterExecution = (Select count(1) from dbo.NSEDataCleaned)

	exec dbo.SP_SQLETLStatisticts @SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime, @NumberOfRecordsInDestinationTableAfterExecution, Null
	COMMIT Transaction;
END TRY



BEGIN CATCH
	SELECT @ErrorMessage = ERROR_MESSAGE()
	PRINT @ErrorMessage
	set @SQLETLFinishTime = (Select GETDATE())
	exec dbo.SP_SQLETLStatisticts @SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime, @NumberOfRecordsInDestinationTableAfterExecution, @ErrorMessage

    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
END CATCH;
end

/*

exec dbo.SP_NSEDatainsert 'C:\Users\dznfa\OneDrive\Desktop\NSETrading\app\XBRL Files\10022024103954'

*/