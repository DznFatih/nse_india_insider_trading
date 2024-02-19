
drop table if exists dbo.NSEDataCleaned
create table dbo.NSEDataCleaned (
    ID int IDENTITY(1, 1) NOT NULL,
    AcquisitionMode	nvarchar(1000)	Null,
	AcquisitionfromDate	Date	Null,
	AcquisitionToDate	Date	Null,
	AfterAcquisitionSharesNo	nvarchar(1000)	Null,
	AfterAcquisitionSharesPercentage	decimal(19,2)	Null,
	BeforeAcquisitionSharesNo	nvarchar(1000)	Null,
	BeforeAcquisitionSharesPercentage	decimal(19,2)	Null,
	BuyQuantity	decimal(19, 4)	Null,
	BuyValue	decimal(19, 4)	Null,
	DerivativeType	nvarchar(1000)	Null,
	Did	bigint	NOT Null,
	Exchange	nvarchar(1000)	Null,
	IntimDate	Date	Null,
	PID	decimal(19, 4)	Null,
	Remarks	nvarchar(1000)	Null,
	SecuritiesValue	decimal(19,4)	Null,
	SecuritiesTypePost	nvarchar(1000)	Null,
	SellValue	decimal(19, 4)	Null,
	TDPDerivativeContractType	nvarchar(1000)	Null,
	TKDAcqm	nvarchar(1000)	Null,
    Symbol nvarchar(1000) Null,
    CompanyName nvarchar(1000) Null,
    Regulation nvarchar(1000) Null,
    NameOfTheAcquirerORDisposer nvarchar(1000) Null,
    TypeOfSecurity nvarchar(1000) Null,
    NoOfSecurities decimal(19, 4) Null,
    AcquisitionORDisposal nvarchar(1000) Null,
    BroadcastDateTime Datetime Null,
    XBRLLink nvarchar(1000) Null,
    Period nvarchar(1000) Null,
    ScripCode nvarchar(1000) Null,
    NSESymbol nvarchar(1000) Null,
    MSEISymbol nvarchar(1000) Null,
    NameOfTheCompany nvarchar(1000) Null,
    WhetherISINAvailable nvarchar(1000) Null,
    ISINCode nvarchar(1000) Null,
    RevisedFilling nvarchar(1000) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract nvarchar(1000) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable nvarchar(1000) Null,
    ChangeInHoldingOfSecuritiesOfPromotersAxis nvarchar(1000) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems nvarchar(1000) Null,
    TypeOfInstrument nvarchar(1000) Null,
    TypeOfInstrumentOthers nvarchar(1000) Null,
    CategoryOfPerson nvarchar(1000) Null,
    NameOfThePerson nvarchar(1000) Null,
    PANNumber nvarchar(1000) Null,
    IdentificationNumberOfDirectorOrCompany nvarchar(1000) Null,
    Address nvarchar(1000) Null,
    ContactNumber nvarchar(1000) Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalAbstract nvarchar(1000) Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity decimal(19, 4) Null,
    SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    SecuritiesAcquiredOrDisposedAbstract nvarchar(1000) Null,
    SecuritiesAcquiredOrDisposedNumberOfSecurity decimal(19, 4) Null,
    SecuritiesAcquiredOrDisposedValueOfSecurity decimal(19, 4) Null,
    SecuritiesAcquiredOrDisposedTransactionType nvarchar(1000) Null,
    SecuritiesHeldPostAcquistionOrDisposalAbstract nvarchar(1000) Null,
    SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity decimal(19, 4) Null,
    SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding decimal(19, 2) Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate Date Null,
    DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate Date Null,
    DateOfIntimationToCompany DateTime Null,
    TypeOfContract nvarchar(1000) Null,
    ContractSpecification nvarchar(1000) Null,
    BuyAbstract nvarchar(1000) Null,
    BuyNotionalValue decimal(19, 4) Null,
    BuyNumberOfUnits decimal(19, 4) Null,
    SellAbstract nvarchar(1000) Null,
    NotionalValue decimal(19, 4) Null,
    NumberOfUnits decimal(19, 4) Null,
    ExchangeOnWhichTheTradeWasExecuted nvarchar(1000) Null,
    TotalValueInAggregate decimal(19, 4) Null,
    NameOfTheSignatory nvarchar(1000) Null,
    DesignationOfSignatory nvarchar(1000) Null,
    Place nvarchar(1000) Null,
    DateOfFiling nvarchar(1000) Null,
    DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock nvarchar(1000) Null,
    ModeOfAcquisitionOrDisposal nvarchar(1000) Null,
    GeneralInformationAbstract nvarchar(1000) Null,
    Currency nvarchar(1000) Null,
    IsOrphan	nvarchar(1000)	Not Null,
    UpdateDate Datetime Not Null,
    InsertDate DateTime Not Null

	, CONSTRAINT PK_NSEDataCleaned_DID PRIMARY KEY CLUSTERED (Did)
)


drop table if exists dbo.ETLStatistics
create table dbo.ETLStatistics
 (
	ID int IDENTITY(1,1) NOT NULL,
	PythonScriptExecutionStartTime varchar(200) NULL,
    DataDescription	varchar(200)	Null,
	NSESearchPageVisitTime	varchar(200)	Null,
	XBRLDocumentDownloadsStartTime	varchar(200)	Null,
	XBRLDocumentDownloadsEndTime	varchar(200)	Null,
	XBRLDocumentPageVisitAttemptCount	varchar(200)	Null,
	XBRLDocumentDownloadErrorCount	varchar(200)	Null,
	XBRLDocumentDownloadSuccessCount	varchar(200)	Null,
	SQLETLStartTime DATETIME NULL,
	RowsBulkInserted int NULL,
	PreviouslyInsertedRowsUpdated  int NULL,
	NewRecordsInserted int NULL,
	SQLETLFinishTime DATETIME NULL,
	NumberOfRecordsInDestinationTableAfterExecution int NULL,
	ErrorMessage NVARCHAR(MAX) Null
 );

--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================


----------------------------------------------------------- SP_SQLETLStatisticts Procedure
CREATE  or ALTER PROCEDURE dbo.SP_ETLStatistics
											@PythonScriptExecutionStartTime varchar(200),
											@DataDescription	varchar(200),
											@NSESearchPageVisitTime	varchar(200),
											@XBRLDocumentDownloadsStartTime	varchar(200),
											@XBRLDocumentDownloadsEndTime	varchar(200),
											@XBRLDocumentPageVisitAttemptCount	varchar(200),
											@XBRLDocumentDownloadErrorCount	varchar(200),
											@XBRLDocumentDownloadSuccessCount int,
											@SQLETLStartTime DateTime,
											@RowsBulkInserted int,
											@PreviouslyInsertedRowsUpdated int,
											@NewRecordsInserted int,
											@SQLETLFinishTime datetime,
											@NumberOfRecordsInDestinationTableAfterExecution int,
											@ErrorMessage NVARCHAR(MAX)
AS
BEGIN

	Insert into dbo.ETLStatistics
		(
		PythonScriptExecutionStartTime,
		DataDescription,
		NSESearchPageVisitTime,
		XBRLDocumentDownloadsStartTime,
		XBRLDocumentDownloadsEndTime,
		XBRLDocumentPageVisitAttemptCount,
		XBRLDocumentDownloadErrorCount,
		XBRLDocumentDownloadSuccessCount,
		SQLETLStartTime,
		RowsBulkInserted,
		PreviouslyInsertedRowsUpdated,
		NewRecordsInserted,
		SQLETLFinishTime,
		NumberOfRecordsInDestinationTableAfterExecution,
		ErrorMessage)
	values
		(@PythonScriptExecutionStartTime, @DataDescription, @NSESearchPageVisitTime, @XBRLDocumentDownloadsStartTime, @XBRLDocumentDownloadsEndTime, @XBRLDocumentPageVisitAttemptCount, @XBRLDocumentDownloadErrorCount,
		 @XBRLDocumentDownloadSuccessCount, @SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime, @NumberOfRecordsInDestinationTableAfterExecution, @ErrorMessage)
END

--========================================================================================================================================================================
--========================================================================================================================================================================
--========================================================================================================================================================================


--------------------------------------------------------------  dbo.SP_NSEDataCleaned



create or alter procedure dbo.SP_NSEDatainsert @folder_path varchar(500)
AS
BEGIN

	/*
		Declare variables
	*/

	DECLARE @python_etl_stat_file_name NVARCHAR(max) = 'metadata.txt';
	DECLARE @nse_data_bulk_file_name NVARCHAR(max) = 'NSEData.txt';
	DECLARE @python_etl_stat_file_path NVARCHAR(max) = @folder_path + '/' + @python_etl_stat_file_name;
	DECLARE @nse_data_bulk_file_path NVARCHAR(max) = @folder_path + '/' + @nse_data_bulk_file_name;

	DECLARE @PythonScriptExecutionStartTime nvarchar(1000);
	DECLARE @DataDescription	nvarchar(1000);
	DECLARE @NSESearchPageVisitTime	nvarchar(1000);
	DECLARE @XBRLDocumentDownloadsStartTime	nvarchar(1000);
	DECLARE @XBRLDocumentDownloadsEndTime	nvarchar(1000);
	DECLARE @XBRLDocumentPageVisitAttemptCount	nvarchar(1000);
	DECLARE @XBRLDocumentDownloadErrorCount	nvarchar(1000);
	DECLARE @XBRLDocumentDownloadSuccessCount int;

	declare @SQLETLStartTime datetime = (Select GETDATE())
	declare @RowsBulkInserted int = 0;
	declare @PreviouslyInsertedRowsUpdated int = 0;
	declare @NewRecordsInserted int = 0;
	declare @SQLETLFinishTime datetime;
	declare @NumberOfRecordsInDestinationTableAfterExecution int = (Select count(1) from dbo.NSEDataCleaned);
	DECLARE @ErrorMessage NVARCHAR(max);


	/*
		Temporary table to hold bulk inserted data from txt files.
	*/

	drop table if exists #NSEDataBulkInsert;
	CREATE TABLE #NSEDataBulkInsert(
		AcquisitionMode	nvarchar(1000)	Null,
		AcquisitionfromDate	nvarchar(1000)	Null,
		AcquisitionToDate	nvarchar(1000)	Null,
		AfterAcquisitionSharesNo	nvarchar(1000)	Null,
		AfterAcquisitionSharesPercentage	nvarchar(1000)	Null,
		BeforeAcquisitionSharesNo	nvarchar(1000)	Null,
		BeforeAcquisitionSharesPercentage	nvarchar(1000)	Null,
		BuyQuantity	nvarchar(1000)	Null,
		BuyValue	nvarchar(1000)	Null,
		DerivativeType	nvarchar(1000)	Null,
		Did	nvarchar(1000)	Null,
		Exchange	nvarchar(1000)	Null,
		IntimDate	nvarchar(1000)	Null,
		PID	nvarchar(1000)	Null,
		Remarks	nvarchar(1000)	Null,
		SecuritiesValue	nvarchar(1000)	Null,
		SecuritiesTypePost	nvarchar(1000)	Null,
		SellValue	nvarchar(1000)	Null,
		TDPDerivativeContractType	nvarchar(1000)	Null,
		TKDAcqm	nvarchar(1000)	Null,
		Symbol nvarchar(1000) NULL,
		CompanyName nvarchar(1000) NULL,
		Regulation nvarchar(1000) NULL,
		NameOfTheAcquirerORDisposer nvarchar(1000) NULL,
		TypeOfSecurity nvarchar(1000) NULL,
		NoOfSecurities nvarchar(1000) NULL,
		AcquisitionORDisposal nvarchar(1000) NULL,
		BroadcastDateTime datetime NULL,
		XBRLLink nvarchar(1000) NULL,
		Period nvarchar(1000) NULL,
		ScripCode nvarchar(1000) NULL,
		NSESymbol nvarchar(1000) NULL,
		MSEISymbol nvarchar(1000) NULL,
		NameOfTheCompany nvarchar(1000) NULL,
		WhetherISINAvailable nvarchar(1000) NULL,
		ISINCode nvarchar(1000) NULL,
		RevisedFilling nvarchar(1000) NULL,
		DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsAbstract nvarchar(1000) NULL,
		DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTable nvarchar(1000) NULL,
		ChangeInHoldingOfSecuritiesOfPromotersAxis nvarchar(1000) NULL,
		DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsLineItems nvarchar(1000) NULL,
		TypeOfInstrumentOthers nvarchar(1000) NULL,
		TypeOfInstrument nvarchar(1000) NULL,
		CategoryOfPerson nvarchar(1000) NULL,
		NameOfThePerson nvarchar(1000) NULL,
		PANNumber nvarchar(1000) NULL,
		IdentificationNumberOfDirectorOrCompany nvarchar(1000) NULL,
		Address nvarchar(1000) NULL,
		ContactNumber nvarchar(1000) NULL,
		SecuritiesHeldPriorToAcquisitionOrDisposalAbstract nvarchar(1000) NULL,
		SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity nvarchar(1000) NULL,
		SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding nvarchar(1000) NULL,
		SecuritiesAcquiredOrDisposedAbstract nvarchar(1000) NULL,
		SecuritiesAcquiredOrDisposedNumberOfSecurity nvarchar(1000) NULL,
		SecuritiesAcquiredOrDisposedValueOfSecurity nvarchar(1000) NULL,
		SecuritiesAcquiredOrDisposedTransactionType nvarchar(1000) NULL,
		SecuritiesHeldPostAcquistionOrDisposalAbstract nvarchar(1000) NULL,
		SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity nvarchar(1000) NULL,
		SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding nvarchar(1000) NULL,
		DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract nvarchar(1000) NULL,
		DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate nvarchar(1000) NULL,
		DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate nvarchar(1000) NULL,
		DateOfIntimationToCompany nvarchar(1000) NULL,
		TypeOfContract nvarchar(1000) NULL,
		ContractSpecification nvarchar(1000) NULL,
		BuyAbstract nvarchar(1000) NULL,
		BuyNotionalValue nvarchar(1000) NULL,
		BuyNumberOfUnits nvarchar(1000) NULL,
		SellAbstract nvarchar(1000) NULL,
		NotionalValue nvarchar(1000) NULL,
		NumberOfUnits nvarchar(1000) NULL,
		ExchangeOnWhichTheTradeWasExecuted nvarchar(1000) NULL,
		TotalValueInAggregate nvarchar(1000) NULL,
		NameOfTheSignatory nvarchar(1000) NULL,
		DesignationOfSignatory nvarchar(1000) NULL,
		Place nvarchar(1000) NULL,
		DateOfFiling nvarchar(1000) NULL,
		DetailsOfChangeInHoldingOfSecuritiesOfPromotersEmployeeOrDirectorOfAListedCompanyAndOtherSuchPersonsTextBlock nvarchar(1000) NULL,
		ModeOfAcquisitionOrDisposal nvarchar(1000) NULL,
		GeneralInformationAbstract nvarchar(1000) NULL,
		Currency nvarchar(1000) NULL,
		IsOrphan	varchar(10)	Null,
		DownloadDate nvarchar(1000) NULL
	);

	drop table if exists #PythonETLStatistics
	create table #PythonETLStatistics (
		PythonScriptExecutionStartTime nvarchar(1000) NULL,
		DataDescription	nvarchar(1000)	Null,
		NSESearchPageVisitTime	nvarchar(1000)	Null,
		XBRLDocumentDownloadsStartTime	nvarchar(1000)	Null,
		XBRLDocumentDownloadsEndTime	nvarchar(1000)	Null,
		XBRLDocumentPageVisitAttemptCount	nvarchar(1000)	Null,
		XBRLDocumentDownloadErrorCount	nvarchar(1000)	Null,
		XBRLDocumentDownloadSuccessCount	nvarchar(1000)	Null
	);


	/*
		Dynamic query to run later. We concatinate bulk insert sql statement with file path. It is neccessary to use dynamic query with bulk insert because we need to dynamically
		add file path to our sql statement. Later in the code we execute this statement using exec(@sql_statement) command.
	*/

	Declare @sql_nse_data_bulk nvarchar(1000) = 'bulk insert #NSEDataBulkInsert
								  from ''' +  @nse_data_bulk_file_path + '''
								  with
								  (
										FORMAT = ''CSV'',
										FIRSTROW = 2,
										CODEPAGE = ''65001'',
										FIELDTERMINATOR = ''þ'',
										MAXERRORS = 1,
										ROWTERMINATOR = ''\n''
								   )'

    Declare @sql_python_stats nvarchar(1000) = 'bulk insert #PythonETLStatistics
									  from ''' +  @python_etl_stat_file_path + '''
									  with
									  (
											FORMAT = ''CSV'',
											FIRSTROW = 2,
											CODEPAGE = ''65001'',
											FIELDTERMINATOR = ''þ'',
											MAXERRORS = 1,
											ROWTERMINATOR = ''\n''
									   )'


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

	Set @RowsBulkInserted = (Select count(1) from #NSEDataBulkInsert)

	/*
		Delete temporary table if it exists
	*/

	drop table if exists dbo.temp_update_table;

	/*
		Select data from table in which we bulk inserted NSEData in previous step. The data is transformed into correct datatype to match with target table,
		i.e. from nvarchar to date or from nvarchar to decimal(19, 4).
	*/

	Select
		 AcquisitionMode
		,case
			when AcquisitionfromDate = '-' then Null
			else cast(AcquisitionfromDate as date)
		end AcquisitionfromDate
		,case
			when AcquisitionToDate = '-' then Null
			else cast(AcquisitionToDate as date)
		end AcquisitionToDate
		,Cast(AfterAcquisitionSharesNo as nvarchar(1000)) as AfterAcquisitionSharesNo
		, case
			when AfterAcquisitionSharesPercentage = '-' then Null
			else Cast(AfterAcquisitionSharesPercentage as decimal(19,2))
		end AfterAcquisitionSharesPercentage
		,Cast(BeforeAcquisitionSharesNo as nvarchar(1000)) as BeforeAcquisitionSharesNo
		,case
			when BeforeAcquisitionSharesPercentage = '-' then Null
			else Cast(BeforeAcquisitionSharesPercentage as decimal(19,2))
		end BeforeAcquisitionSharesPercentage
		,case
			when BuyQuantity = '-' then Null
			else Cast(BuyQuantity as decimal(19, 4))
		end BuyQuantity
		,case
			when BuyValue = '-' then Null
			else Cast(BuyValue as decimal(19, 4))
		end BuyValue
		,DerivativeType
		,Cast(Did as bigint) as Did
		,Exchange
		,case
			when IntimDate = '-' then Null
			else Cast(IntimDate as date)
		end IntimDate
		,case
			when PID = '-' then Null
			else Cast(PID as decimal(19, 4))
		end PID
		,Remarks
		,case
			when SecuritiesValue = '-' then Null
			else Cast(SecuritiesValue as decimal(19, 4))
		end SecuritiesValue
		,SecuritiesTypePost
		,case
			when SellValue = '-' then Null
			else Cast(SellValue as decimal(19, 4))
		end SellValue
		,TDPDerivativeContractType
		,TKDAcqm
		,Symbol
		,CompanyName
		,Regulation
		,NameOfTheAcquirerORDisposer
		,TypeOfSecurity
		,case
			when NoOfSecurities = '-' then Null
			else Cast(NoOfSecurities as decimal(19, 4))
		end NoOfSecurities
		,AcquisitionORDisposal
		,CONVERT(VARCHAR, BroadcastDateTime, 120) as BroadcastDateTime
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
		,case
			when SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity = '-' then Null
			else Cast(SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity as decimal(19, 4))
		end SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity
		,case
			when SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding = '-' then Null
			else Cast(SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding as decimal(19, 2))
		end SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding
		,SecuritiesAcquiredOrDisposedAbstract
		,case
			when SecuritiesAcquiredOrDisposedNumberOfSecurity = '-' then Null
			else Cast(SecuritiesAcquiredOrDisposedNumberOfSecurity as decimal(19, 4))
		end SecuritiesAcquiredOrDisposedNumberOfSecurity
		,case
			when SecuritiesAcquiredOrDisposedValueOfSecurity = '-' then Null
			else Cast(SecuritiesAcquiredOrDisposedValueOfSecurity as decimal(19, 4))
		end SecuritiesAcquiredOrDisposedValueOfSecurity
		,SecuritiesAcquiredOrDisposedTransactionType
		,SecuritiesHeldPostAcquistionOrDisposalAbstract
		,case
			when SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity = '-' then Null
			else Cast(SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity as decimal(19, 4))
		end SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity
		,case
			when SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding = '-' then Null
			else Cast(SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding as decimal(19, 2))
		end SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding
		,case
			when DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract = '-' then Null
			else Cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract as datetime)
		end DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyAbstract
		,case
			when DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate = '-' then Null
			else Cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate as datetime)
		end DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate
		,case
			when DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate = '-' then Null
			else Cast(DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate as datetime)
		end DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate
		,case
			when DateOfIntimationToCompany = '-' then Null
			else Cast(DateOfIntimationToCompany as datetime)
		end DateOfIntimationToCompany
		,TypeOfContract
		,ContractSpecification
		,BuyAbstract
		,case
			when BuyNotionalValue = '-' then Null
			else Cast(BuyNotionalValue as decimal(19, 4))
		end BuyNotionalValue
		,case
			when BuyNumberOfUnits = '-' then Null
			else Cast(BuyNumberOfUnits as decimal(19, 4))
		end BuyNumberOfUnits
		,SellAbstract
		,case
			when NotionalValue = '-' then Null
			else Cast(NotionalValue as decimal(19, 4))
		end NotionalValue
		,case
			when NumberOfUnits = '-' then Null
			else Cast(NumberOfUnits as decimal(19, 4))
		end NumberOfUnits
		,ExchangeOnWhichTheTradeWasExecuted
		,case
			when TotalValueInAggregate = '-' then Null
			else Cast(TotalValueInAggregate as decimal(19, 4))
		end TotalValueInAggregate
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
	from #NSEDataBulkInsert

	-- UPDATE DATA

	/*
		Update data in our target table
	*/

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
		Set new inserted record count to our variable NewRecordsInserted
	*/

	set @NewRecordsInserted = (Select count(1) from dbo.temp_update_table where Did not in (Select Did from dbo.NSEDataCleaned))

	/*
		Insert newly downloaded data into our target table.

	*/

		insert into dbo.NSEDataCleaned
		Select tut.*
		from dbo.temp_update_table tut
		left join dbo.NSEDataCleaned nse_cleaned
			on nse_cleaned.Did = tut.Did
		where nse_cleaned.Did is null


	drop table if exists dbo.temp_update_table;

	/*
		Set values for ETL statisticts

	*/

	set @SQLETLFinishTime = (Select GETDATE())
	set @NumberOfRecordsInDestinationTableAfterExecution = (Select count(1) from dbo.NSEDataCleaned)
	set @PythonScriptExecutionStartTime = (Select PythonScriptExecutionStartTime from #PythonETLStatistics)
	set @DataDescription = (Select DataDescription from #PythonETLStatistics)
	set @NSESearchPageVisitTime = (Select NSESearchPageVisitTime from #PythonETLStatistics)
	set @XBRLDocumentDownloadsStartTime = (Select XBRLDocumentDownloadsStartTime from #PythonETLStatistics)
	set @XBRLDocumentDownloadsEndTime = (Select XBRLDocumentDownloadsEndTime from #PythonETLStatistics)
	set @XBRLDocumentPageVisitAttemptCount = (Select XBRLDocumentPageVisitAttemptCount from #PythonETLStatistics)
	set @XBRLDocumentDownloadErrorCount = (Select XBRLDocumentDownloadErrorCount from #PythonETLStatistics)
	set @XBRLDocumentDownloadSuccessCount = (Select XBRLDocumentDownloadSuccessCount from #PythonETLStatistics)

	/*
		Insert values into ETLStatisticts table by calling SP_SQLETLStatisticts stored procedure

	*/
	exec dbo.SP_ETLStatistics @PythonScriptExecutionStartTime, @DataDescription, @NSESearchPageVisitTime, @XBRLDocumentDownloadsStartTime, @XBRLDocumentDownloadsEndTime,
								  @XBRLDocumentPageVisitAttemptCount, @XBRLDocumentDownloadErrorCount, @XBRLDocumentDownloadSuccessCount,
								  @SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime,
								  @NumberOfRecordsInDestinationTableAfterExecution, Null
	COMMIT Transaction;
END TRY


/*
	In case of an error, catch block will be executed and will store values to our statistics table.
*/
BEGIN CATCH
	SELECT @ErrorMessage = ERROR_MESSAGE()
	PRINT @ErrorMessage
	set @SQLETLFinishTime = (Select GETDATE())
	exec dbo.SP_ETLStatistics @PythonScriptExecutionStartTime, @DataDescription, @NSESearchPageVisitTime, @XBRLDocumentDownloadsStartTime, @XBRLDocumentDownloadsEndTime,
								  @XBRLDocumentPageVisitAttemptCount, @XBRLDocumentDownloadErrorCount, @XBRLDocumentDownloadSuccessCount,
								  @SQLETLStartTime, @RowsBulkInserted, @PreviouslyInsertedRowsUpdated, @NewRecordsInserted, @SQLETLFinishTime,
								  @NumberOfRecordsInDestinationTableAfterExecution, @ErrorMessage
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
END CATCH;
end

/*
	exec dbo.SP_NSEDatainsert 'C:\Users\dznfa\OneDrive\Desktop\NSETrading\app\XBRLFiles\11022024092527'
*/