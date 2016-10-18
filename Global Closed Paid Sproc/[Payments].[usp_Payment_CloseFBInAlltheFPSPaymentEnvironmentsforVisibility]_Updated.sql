ALTER PROCEDURE [Payments].[usp_Payment_CloseFBInAlltheFPSPaymentEnvironmentsforVisibility]
AS

DECLARE @ENV_COUNT				AS INT
DECLARE @LOOP_COUNT				AS INT
DECLARE @DNS					AS VARCHAR(255)
DECLARE @DB						AS VARCHAR(255)
DECLARE @QUERY					AS NVARCHAR(4000)
DECLARE @CompleteInvoiceOnly	AS BIT
DECLARE	@GenerateRemitAdv		AS BIT
DECLARE	@NoOfDaysPrior			AS INT
DECLARE @ProcessName			AS VARCHAR(255)
DECLARE @ProcessId				AS INT
DECLARE @EnvironmentId			AS UNIQUEIDENTIFIER
DECLARE @BusinessFlow			AS VARCHAR(100)
DECLARE @ExecPath				AS VARCHAR(100)
DECLARE @CheckDateToUse			AS VARCHAR(50)

SET @ProcessName = 'CloseFBVisibility'

SELECT
  @ProcessId = ProcessId
FROM [Payments].[tbl_Payments_Processes]
WHERE ProcessName = @ProcessName

--Create the temporary table that is going to get all the Enviroments where the CloseFBDenied SPROC needs to be executed
--Executed = 0 the CloseFBDenied SPROC was not executed yet
--ErrorMsg contains an error message if something goes wrong
IF object_id('tempdb..#t_CloseFBEnvironments') is NOT NULL
  TRUNCATE TABLE #t_CloseFBEnvironments
ELSE BEGIN
  create table #t_CloseFBEnvironments (
   ENV_ID int IDENTITY(1,1) PRIMARY KEY,
   BusinessFlow  	[nvarchar](100),
   ExecPath	   	 	[nvarchar](100),
   CheckDateToUse   [nvarchar](50),
   StartDate datetime,
   EndDate datetime,
   EnvironmentId uniqueidentifier NOT NULL,
   EnvironmentName varchar(100) NOT NULL,
   Executed bit DEFAULT 0,
   Error bit DEFAULT 0,
   ErrorMsg varchar(255)
   )
END

--GET all the Environments where to executed the CloseFBDenied SPROC
INSERT INTO #t_CloseFBEnvironments
SELECT 
	CV.BusinessFlow as BusinessFlow,
	CV.ExecPath as ExecPath,
	CV.CheckDateToUse as CheckDateToUse,
	NULL as StartDate,
	NULL as EndDate,
	CV.EnvironmentId,
	CE.EnvironmentName,
	0 as Executed,
	0 as Error,
	NULL as ErrorMsg
FROM [Payments].[tbl_CentralConfiguration_CloseFB_Visibility] CV
INNER JOIN [Payments].[tbl_CentralConfigurationEnvironments] CE ON CE.EnvironmentId = CV.EnvironmentId
WHERE StatusFlag = 1
ORDER BY CE.EnvironmentName

SELECT @ENV_COUNT = @@ROWCOUNT
SELECT @LOOP_COUNT = 1

WHILE @LOOP_COUNT <= @ENV_COUNT
BEGIN

	--SET the StartDate
	UPDATE #t_CloseFBEnvironments
	SET StartDate = GETDATE()
	WHERE ENV_ID = @LOOP_COUNT
	
	--GET the environment DNS and Database Name
	SELECT 
		@DNS = DNS,
		@DB = DB,
		@EnvironmentId = CE.EnvironmentId,
		@BusinessFlow = isnull(CL.BusinessFlow,''),
		@ExecPath = isnull(CL.ExecPath,''),
		@CheckDateToUse = CL.CheckDateToUse
	FROM [Payments].[tbl_CentralConfigurationEnvironments] CE
	INNER JOIN #t_CloseFBEnvironments CL ON CL.EnvironmentId = CE.EnvironmentId
	WHERE CL.ENV_ID = @LOOP_COUNT
	
	--GET the environment settings
	SELECT 
		@CompleteInvoiceOnly = CompleteInvoiceOnly,
		@GenerateRemitAdv = GenerateRemitAdv,
		@NoOfDaysPrior = NoOfDaysPrior
	FROM [Payments].[tbl_CentralConfiguration_CloseFB_Visibility] CV
	INNER JOIN #t_CloseFBEnvironments CL ON CL.EnvironmentId = CV.EnvironmentId
	WHERE CL.ENV_ID = @LOOP_COUNT
	and isnull(cl.BusinessFlow,'') = @BusinessFlow
	and isnull(cl.ExecPath,'')	= @ExecPath
	
	BEGIN TRY
      --Deploy the necessary items on the Remote DB
      EXEC [Payments].[usp_Remote_Deployment] @ProcessName, @EnvironmentId  

	  ----Try to execute the CloseFBDenied SPROC
	  SELECT @Query = 
	  ' 
	  [' + @DNS + '].[' + @DB + '].[Payments].[usp_Payment_CloseFB] ''' + CAST(@EnvironmentId AS NVARCHAR(100)) + ''', ''' + CAST(@BusinessFlow AS NVARCHAR(100)) + ''', ''' + CAST(@ExecPath AS NVARCHAR(100)) + ''', ' + CAST(@CompleteInvoiceOnly AS VARCHAR(1)) + ', ' + CAST(@GenerateRemitAdv AS VARCHAR(1)) + ', ' + CAST(@NoOfDaysPrior AS VARCHAR(1)) + ', ''' + CAST(@CheckDateToUse AS NVARCHAR(100)) + '''
	  '

	  select @QUERY

	  EXEC sp_executesql @QUERY
	END TRY
	
	BEGIN CATCH
	  --Update Error Message
	  UPDATE #t_CloseFBEnvironments
	  SET 
	    Error = 1,
		ErrorMsg = ERROR_MESSAGE()
	  WHERE ENV_ID = @LOOP_COUNT
	END CATCH
	
	--Update the Executed Status
	UPDATE #t_CloseFBEnvironments
	SET 
	  Executed = 1,
	  EndDate = GETDATE()
	WHERE ENV_ID = @LOOP_COUNT
	
	--select top 100 * from [Payments].[tbl_Payments_Logs] 
	--where processname='CloseFBVisibility'
	--order by startDate desc

	INSERT INTO [Payments].[tbl_Payments_Logs]
	  SELECT
	  @ProcessId,
	  @ProcessName,
      StartDate,
      EndDate,
	  EnvironmentId,
	  EnvironmentName,
	  Executed,
	  Error,
	  ErrorMsg
	FROM #t_CloseFBEnvironments
	WHERE ENV_ID = @LOOP_COUNT
	
	--Increase Index
	SET @LOOP_COUNT = @LOOP_COUNT + 1
	
END

UPDATE [Payments].[tbl_Payments_Processes]
SET ProcessId = @ProcessId + 1
WHERE ProcessName = @ProcessName