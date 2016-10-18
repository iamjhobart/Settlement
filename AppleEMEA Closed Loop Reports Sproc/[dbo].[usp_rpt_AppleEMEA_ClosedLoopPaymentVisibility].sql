ALTER PROCEDURE [dbo].[usp_rpt_AppleEMEA_ClosedLoopPaymentVisibility]
AS

/********************************************************************************
** Cebu Engineering
** Project Development
** Trax Holdings, Inc.
** 
** Version:	1.0	
** Author: 	Regie Langomes
** Customer: TRAX
**
** Overview:  	
** Revision History:
** When:    	Who:			What:
** 10.17.2012	Regie Langomes	Creation of Stored Procedure. SQL Code created by Alaine.
** 
** Input Parameter Descriptions:
** Output Parameter Descriptions:
**
** Requested by: 
**
********************************************************************************/

SET NOCOUNT ON 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


DECLARE @Today AS DATE
SET @Today = CONVERT (DATE, GETDATE(), 110)

/*****************************************************************************************************/
/** PREPARE TABLES																					**/
/*****************************************************************************************************/

/** FORWARDED PAST DUE OPEN																			**/
IF OBJECT_ID('tempdb..#t_ForwardedPastDueOpenDetails') IS NOT NULL DROP TABLE #t_ForwardedPastDueOpenDetails
CREATE table #t_ForwardedPastDueOpenDetails(
	[VEND_LABL]					[varchar](250) NOT NULL,
	[ISSUE_CATEGORY] 			[varchar](250) NULL,
	[ISSUE_OWNER]				[varchar](250) NULL,
	[ISSUE_DETAILS]				[varchar](500) NULL,
	[DAYS_PAST_DUE]				int NULL,
	[DAYS_SINCE_FORWARDED]		int NULL,
	[INV_ID] 					[varchar](23) NOT NULL,
	[INV_KEY] 					[varchar](50) NULL,
	[INV_APP_AMT]				[money] NULL,
	[CURRENCY]					[varchar](50) NULL,
	[PAYMENT_FORWARDED_INV_ID]	[varchar](23) NOT NULL,
	[PAYMENT_STATE]				[varchar](250) NULL,
	[INV_DUE_DTM]				[date] NULL,
	[PAYMENT_FORWARDED_DATE]	[date] NULL,
	[PAYMENT_FORWARDED_INFO]	[varchar](250) NULL,
	[824A_RECEIVED]				[varchar](50) NULL,
	[824A_RECEIVED_DATE]		[date] NULL,
	[824A_ACTION]				[varchar](250) NULL,
	[824A_RECEIVED_INFO]		[varchar](250) NULL,
	[824B_RECEIVED]				[varchar](250) NULL,
	[824B_CHECKNUM]				[varchar](100) NULL,
	[824B_DATE]					[date] NULL,
	[824B_INFO]					[varchar](250) NULL,
	[BU]						[varchar](100) NULL,
	[COMPANY_CODE]				[varchar](255) NULL,
	[ACCT_NUM_VEND_BLNG]		[varchar](255) NULL,
	[VENDOR_NUMBER]				[varchar](255) NULL,
	[BOOKING_KEY]				[varchar](100) NULL,
	[REVIEW_REMARKS]			[varchar](500) NULL,
	[REVIEWED_BY]				[varchar](255) NULL,
	[REVIEWED_ON]				datetime NULL
) ON [PRIMARY]

create clustered index [#t_ForwardedPastDueOpenDetails:inv_id] on #t_ForwardedPastDueOpenDetails(inv_id)

/*****************************************************************************************************/
/** GET LATEST 810 INFO	FOR FORWARDED PAST DUE OPEN INVOICES										**/
/*****************************************************************************************************/
IF OBJECT_ID('tempdb..#Latest810ID') IS NOT NULL DROP TABLE #Latest810ID
SELECT MAX(DataFeedDetail_PaymentRequest_EDIId) as DataFeedDetail_PaymentRequest_EDIId
INTO #Latest810ID
FROM Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_PaymentRequest_EDI with (nolock)
GROUP BY TransactionID
CREATE NONCLUSTERED INDEX ByPaymentRequestId on #Latest810ID(DataFeedDetail_PaymentRequest_EDIId)

IF OBJECT_ID('tempdb..#Final810InfoReference') IS NOT NULL DROP TABLE #Final810InfoReference
SELECT DISTINCT PR.TransactionID, PR.DataFeedFileName, PR.DataFeedFileCreationDTMUTC, PR.DataFeedISAControlNum,
	PR.TransactionKey, PR.ReqCurrency, I.VEND_LABL, I.INV_DUE_DTM, I.INV_KEY, I.INV_APP_AMT, I.INV_CURRENCY_QUAL
INTO #Final810InfoReference
FROM #Latest810ID ID
INNER JOIN Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_PaymentRequest_EDI as PR with (nolock) 
	ON PR.DataFeedDetail_PaymentRequest_EDIId = ID.DataFeedDetail_PaymentRequest_EDIId
INNER JOIN Aeur_Prod_02M5.dbo.INVOICE I with (nolock) 
	ON (PR.TransactionID = I.INV_ID
	AND I.INV_STAT = 'Open'
	AND I.INV_DUE_DTM < @Today)

CREATE NONCLUSTERED INDEX ByTransactionID on #Final810InfoReference(TransactionID)

DROP TABLE #Latest810ID
/*****************************************************************************************************/
/** GET LATEST 824A INFORMATION																		**/
/*****************************************************************************************************/
-- GET INVOICE ID and latest 824A EDI ID
IF OBJECT_ID('tempdb..#Latest824AID') IS NOT NULL DROP TABLE #Latest824AID
SELECT PC.TransactionID, PC.PaymentPostResultType, MAX(DataFeedDetail_PostConfirmation_EDIId) as DataFeedDetail_PostConfirmation_EDIId
INTO #Latest824AID
FROM Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_PostConfirmation_EDI PC with (nolock)
INNER JOIN #Final810InfoReference A on PC.TransactionID = A.TransactionID
GROUP BY PC.TransactionID, PC.PaymentPostResultType
CREATE NONCLUSTERED INDEX ByTransactionID on #Latest824AID(TransactionID)
CREATE NONCLUSTERED INDEX ByPostConfirmationID on #Latest824AID(DataFeedDetail_PostConfirmation_EDIId)

-- For Invoices with both 'Accept' and 'Reject', delete the 'Reject' Records
IF OBJECT_ID('tempdb..#Dual824A') IS NOT NULL DROP TABLE #Dual824A
SELECT TransactionID
INTO #Dual824A
FROM #Latest824AID
GROUP BY TransactionID
HAVING COUNT(TransactionID) > 1
CREATE NONCLUSTERED INDEX ByTransactionID on #Dual824A(TransactionID)

-- Need to change query. What if Count > 1 and all are 'Reject'?
DELETE FROM #Latest824AID
FROM #Latest824AID A
INNER JOIN #Dual824A B on A.TransactionID = B.TransactionID
WHERE A.PaymentPostResultType = 'Reject' 

-- GET 824A Details of the INVOICES
IF OBJECT_ID('tempdb..#Final824AInfoReference') IS NOT NULL DROP TABLE #Final824AInfoReference
SELECT DISTINCT
PC.TransactionID, PC.DataFeedFileName, PC.DataFeedFileCreationDTMUTC, PC.DataFeedISAControlNum,
PC.PaymentPostResultType, PC.ErrorMessage
INTO #Final824AInfoReference
FROM #Latest824AID A with (nolock)
INNER JOIN Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_PostConfirmation_EDI PC with (nolock)
	on (A.DataFeedDetail_PostConfirmation_EDIId = PC.DataFeedDetail_PostConfirmation_EDIId
	AND A.TransactionID = PC.TransactionID)

CREATE NONCLUSTERED INDEX ByTransactionID on #Final824AInfoReference(TransactionID)

DROP TABLE #Latest824AID
DROP TABLE #Dual824A
/*****************************************************************************************************/
/** GET LATEST 824B INFORMATION																		**/
/*****************************************************************************************************/
-- GET INVOICE ID and latest 824B EDI ID
IF OBJECT_ID('tempdb..#Latest824BID') IS NOT NULL DROP TABLE #Latest824BID
SELECT CC.TransactionID, A.INV_APP_AMT, CC.CustomerCheckNum, MAX(DataFeedDetail_CloseConfirmation_EDIId) as DataFeedDetail_CloseConfirmation_EDIId
INTO #Latest824BID
FROM #Final810InfoReference A
INNER JOIN Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_CloseConfirmation_EDI CC with (nolock) ON A.TransactionID = CC.TransactionID
GROUP BY CC.TransactionID, A.INV_APP_AMT, CC.CustomerCheckNum
CREATE NONCLUSTERED INDEX ByTransactionID on #Latest824BID(TransactionID)
CREATE NONCLUSTERED INDEX ByCloseConfirmationID on #Latest824BID(DataFeedDetail_CloseConfirmation_EDIId)

IF OBJECT_ID('tempdb..#Final824BInfoReference') IS NOT NULL DROP TABLE #Final824BInfoReference
SELECT DISTINCT
A.TransactionID, A.INV_APP_AMT, CC.DataFeedFileName, CC.DataFeedFileCreationDTMUTC, CC.DataFeedISAControlNum, 
A.CustomerCheckNum, CC.PaidAmt, CC.PaidCurrency, PTE.ErrorMessage
INTO #Final824BInfoReference
FROM #Latest824BID A
INNER JOIN Aeur_Prod_02M5.dbo.viewflatDataFeedDetail_CloseConfirmation_EDI CC with (nolock)
	ON (A.TransactionID = CC.TransactionID
	AND A.DataFeedDetail_CloseConfirmation_EDIId = CC.DataFeedDetail_CloseConfirmation_EDIId)
LEFT JOIN Aeur_Prod_02M5.dbo.viewflatPaymentTaskError PTE
	ON (CC.TransactionID = PTE.TransactionID AND CC.DataFeedFileName = PTE.MetricFileName)

CREATE NONCLUSTERED INDEX ByTransactionID on #Final824BInfoReference(TransactionID)

DROP TABLE #Latest824BID
/*****************************************************************************************************/
/** GET ALL DETAILS TOGETHER																		**/
/*****************************************************************************************************/
DECLARE @OWNER_NAME AS VARCHAR(20)
SELECT @OWNER_NAME = OWNER_NAME FROM Aeur_Prod_02M5.dbo.OWNERS WHERE OWNER_KEY like '100%'

--DECLARE @Today AS DATE
--SET @Today = CONVERT (DATE, GETDATE(), 110)

INSERT INTO #t_ForwardedPastDueOpenDetails
SELECT
	R.VEND_LABL,
	CASE
		WHEN (A.DataFeedFileName IS NULL AND B.DataFeedFileName IS NULL) THEN 'No 824A OR 824B Received'
		WHEN (A.PaymentPostResultType = 'Reject' and B.DataFeedFileName IS NULL) THEN '824A Received - Reject'
		WHEN (A.PaymentPostResultType = 'Accept' and B.DataFeedFileName IS NULL) THEN '824A Accept 824B Not Received'
		WHEN (B.DataFeedFileName IS NOT NULL and B.ErrorMessage IS NOT NULL) THEN '824B Validation Error'
		WHEN (B.DataFeedFileName IS NOT NULL and B.ErrorMessage IS NULL) THEN '824B Not Loaded In Trax'
	END AS [ISSUE_CATEGORY],
	CASE
		WHEN (B.DataFeedFileName IS NOT NULL 
			and B.ErrorMessage IS NOT NULL
			and 
				(B.ErrorMessage LIKE 'Invoice has multiple states%'
				or B.ErrorMessage LIKE 'mismatch%')
			) THEN 'Trax'
		WHEN (B.DataFeedFileName IS NOT NULL and B.ErrorMessage IS NULL) THEN 'Trax'
		ELSE 'Customer'
	END AS [ISSUE_OWNER],
	B.ErrorMessage AS [ISSUE_DETAILS],
	DATEDIFF(day, R.INV_DUE_DTM, @Today) AS [DAYS_PAST_DUE],
	DATEDIFF(day, R.DataFeedFileCreationDTMUTC, @Today) AS [DAYS_SINCE_FORWARDED] ,
	R.TransactionID,
	R.INV_KEY,
	R.INV_APP_AMT,
	R.INV_CURRENCY_QUAL,
	SUBSTRING (R.TransactionID,8,16) AS [PAYMENT_FORWARDED_INV_ID],
	CASE
		WHEN (A.DataFeedFileName IS NULL AND B.DataFeedFileName IS NULL) THEN 'Forwarded'
		WHEN (A.PaymentPostResultType = 'Reject' and B.DataFeedFileName IS NULL) THEN 'PostConfirmed-Reject'
		WHEN (A.PaymentPostResultType = 'Accept' and B.DataFeedFileName IS NULL) THEN 'PostConfirmed-Accept'
		WHEN (B.DataFeedFileName IS NOT NULL and B.ErrorMessage IS NOT NULL) THEN 'Payment Confirmed-824B Validation Error'
		WHEN (B.DataFeedFileName IS NOT NULL and B.ErrorMessage IS NULL) THEN 'Payment Confirmed-824B Not Loaded'
	END AS [PAYMENT_STATE],
	R.INV_DUE_DTM,
	R.DataFeedFileCreationDTMUTC AS [PAYMENT_FORWARDED_DATE],
	'Filename: ' + R.DataFeedFileName + ' - ISA: ' + R.DataFeedISAControlNum,
	CASE
		WHEN A.DataFeedFileName IS NOT NULL THEN 'YES'
		ELSE 'NO'
	END AS [824A_RECEIVED] ,
	A.DataFeedFileCreationDTMUTC AS [824A_RECEIVED_DATE] ,
	CASE
		WHEN (A.PaymentPostResultType = 'Reject' and B.DataFeedFileName IS NULL) 
			THEN A.PaymentPostResultType + ' - ' + A.ErrorMessage
		WHEN (A.PaymentPostResultType = 'Reject' and B.DataFeedFileName IS NOT NULL)
			THEN 'Accept'
		ELSE A.PaymentPostResultType
	END
	AS [824A_ACTION] ,
	'Filename: ' + A.DataFeedFileName + ' - ISA: ' + A.DataFeedISAControlNum AS [824A_RECEIVED_INFO] ,
	CASE
		WHEN B.DataFeedFileName IS NOT NULL THEN 'YES'
		ELSE 'NO'
	END AS [824B_RECEIVED] ,
	B.CustomerCheckNum AS [824B_CHECKNUM],
	B.DataFeedFileCreationDTMUTC AS [824B_RECEIVED_DATE] ,
	'Filename: ' + B.DataFeedFileName + ' - ISA: ' + A.DataFeedISAControlNum AS [824B_RECEIVED_INFO],
	'' as [BU],
	'' as [COMPANY_CODE],
	'' as [ACCT_NUM_VEND_BLNG],
	'' as [VENDOR_NUMBER],
	'' as [BOOKING_KEY],
	'' as [REVIEW_REMARKS],
	'' as [REVIEWED_BY],
	NULL as [REVIEWED_ON]
FROM #Final810InfoReference R with (nolock)
LEFT JOIN #Final824AInfoReference A with (nolock) on R.TransactionID = A.TransactionID
LEFT JOIN #Final824BInfoReference B with (nolock) on R.TransactionID = B.TransactionID

-- Reassigned to Trax for internal review prior to handing off to Apple.
update #t_ForwardedPastDueOpenDetails
set [ISSUE_OWNER] = 'Trax'
where [ISSUE_CATEGORY] = '824A Accept 824B Not Received'

---------------------
-- Override
---------------------

-- get latest override
if object_id('tempdb..#LatestOverride') is not null
drop table #LatestOverride

select inv_id, issue_category, issue_owner_override, review_remarks, reviewed_by, reviewed_on
into #LatestOverride
from ( select row_number()over(partition by ovr.inv_id order by ovr.reviewed_on desc) as Id, *
  from dbo.tbl_CloseLoopPayment_Overrides ovr
  where exists (select * 
    from #t_ForwardedPastDueOpenDetails src
    where src.inv_id = ovr.inv_id
    and src.issue_category = ovr.issue_category)) as tbl
where tbl.id = 1


-- update our list
update src
set src.issue_owner = ovr.issue_owner_override
  , src.review_remarks = ovr.review_remarks
  , src.reviewed_by = ovr.reviewed_by
  , src.reviewed_on = ovr.reviewed_on
from #t_ForwardedPastDueOpenDetails src
inner join #LatestOverride ovr on ovr.inv_id = src.inv_id
  and ovr.issue_category = src.issue_category


DROP TABLE #Final810InfoReference
DROP TABLE #Final824AInfoReference
DROP TABLE #Final824BInfoReference


/************************************/
/**REMOVE NON-DUE 824B Author: Jomar Colao**/
/*************************************/
DELETE FROM #t_ForwardedPastDueOpenDetails
WHERE [DAYS_PAST_DUE] < 2 AND [ISSUE_CATEGORY] IN ('824A Accept 824B Not Received','824B Validation Error','824B Not Loaded In Trax')


/*****************************************************************************************************/
/** SET BOOKING INFO, COMPANY CODE AND VENDOR_NUMBER					**/
/*****************************************************************************************************/

update f
  set f.BOOKING_KEY = case 
    when  charindex('Accept|', isnull(d.[%T005], '')) > 0 then replace(d.[%T005], 'Accept|','')
    else '' end
  , BU = fb.[%t004]
  , ACCT_NUM_VEND_BLNG = isnull(fb.[ACCT_NUM_VEND_BLNG], '')
  , VENDOR_NUMBER = isnull(fb.[%t012], '') 
  , company_code = isnull(ca.ca_elem_1, '')
from #t_ForwardedPastDueOpenDetails f (nolock)
inner join Aeur_Prod_02M5.dbo.Invoice_Ext ie (nolock) on f.INV_ID = ie.INV_ID
inner join Aeur_Prod_02M5.dbo.viewInvoiceMaster im (nolock) on ie.InvNId = im.MasterInvNId
inner join Aeur_Prod_02M5.dbo.Invoice_Ext ie2 (nolock) on im.EnvType = 'Payment' and im.InvNId = ie2.InvnId
inner join Aeur_Prod_02M5.dbo.PAYR_DTL d (nolock) on ie2.inv_id = d.inv_id
inner join Aeur_Prod_02M5.dbo.FRGHT_BL fb (nolock) on fb.fb_ID = d.fb_ID
inner join Aeur_Prod_02M5.dbo.CA_ELEM ca (nolock) on ca.unt_id = fb.fb_id

/*****************************************************************************************************/
/** CREATE SUMMARY 2																				**/
/*****************************************************************************************************/
IF OBJECT_ID('dbo.tbl_rpt_ForwardedPastDueOpenDetails_Summary_AppleEMEIA') IS NOT NULL DROP TABLE dbo.tbl_rpt_ForwardedPastDueOpenDetails_Summary_AppleEMEIA
SELECT
	VEND_LABL AS [SCAC],
	[CURRENCY],
	[ISSUE_CATEGORY] as [ISSUE CATEGORY],
	[ISSUE_OWNER] as [ISSUE OWNER],
	COUNT (INV_ID) AS [Invoice Cnt],
	SUM ([INV_APP_AMT]) AS [Invoice Amt]
INTO dbo.tbl_rpt_ForwardedPastDueOpenDetails_Summary_AppleEMEIA
FROM #t_ForwardedPastDueOpenDetails
WHERE [DAYS_SINCE_FORWARDED] > 5
GROUP BY VEND_LABL, [CURRENCY], [ISSUE_CATEGORY], [ISSUE_OWNER]
--ORDER BY VEND_LABL, [CURRENCY], [ISSUE_CATEGORY], [ISSUE_OWNER]
ORDER BY (COUNT (INV_ID)) DESC

/*****************************************************************************************************/
/** CREATE SUMMARY 2 - DETAILS																		**/
/*****************************************************************************************************/
-- ISSUE OWNER: CUSTOMER
IF OBJECT_ID('dbo.tbl_rpt_ForwardedPastDueOpenDetails_Customer_AppleEMEIA') IS NOT NULL DROP TABLE dbo.tbl_rpt_ForwardedPastDueOpenDetails_Customer_AppleEMEIA
SELECT
	[VEND_LABL] AS [SCAC],
	[CURRENCY] AS [PAYMT CURRENCY],
	[ISSUE_CATEGORY] AS [ISSUE CATEGORY],
	[ISSUE_DETAILS] AS [ISSUE DETAILS],
	[DAYS_PAST_DUE] AS [DAYS (PAST DUE)] ,
	[DAYS_SINCE_FORWARDED] AS [DAYS (FORWARDED)],
	[PAYMENT_FORWARDED_INV_ID] AS [INV_ID (810)],
	[INV_KEY] AS [INV_KEY (CARRIER)],
	[BU] AS [INV BUSINESS UNIT],
	[ACCT_NUM_VEND_BLNG] AS [ACCOUNT NUMBER],
	[VENDOR_NUMBER] AS [VENDOR NUMBER],
	[COMPANY_CODE] AS [COMPANY CODE],
	[INV_APP_AMT] AS [PAYMT REQ AMT],	
	CONVERT (VARCHAR(25), [INV_DUE_DTM], 110) AS [INV DUE DATE],
	CONVERT (VARCHAR(25), [PAYMENT_FORWARDED_DATE], 110) AS [PAYMT FORWARDED DATE],
	[PAYMENT_FORWARDED_INFO] AS [810 FILE INFO],
	[824A_RECEIVED] AS [RECVD 824A?],
	CONVERT (VARCHAR(25), [824A_RECEIVED_DATE], 110) AS [824A RECVD DATE],
	[824A_ACTION] AS [824A ACT CODE],
	[BOOKING_KEY] AS [824A BOOKING KEY],
	[824A_RECEIVED_INFO] AS [824A FILE INFO],
	[824B_RECEIVED] AS [RECVD 824B?],
	[824B_CHECKNUM] AS [Check# In 824B],
	CONVERT (VARCHAR(25), [824B_DATE], 110) AS [824B RECVD DATE],
	[824B_INFO] AS [824B FILE INFO],
	[INV_ID] AS [INV_ID (TRAX)],
	[PAYMENT_STATE] AS [PAYMT STATUS],
	[REVIEW_REMARKS] AS [REVIEW REMARKS],
	[REVIEWED_BY] AS [REVIEWED BY],
	[REVIEWED_ON] AS [REVIEWED ON]
INTO dbo.tbl_rpt_ForwardedPastDueOpenDetails_Customer_AppleEMEIA
FROM #t_ForwardedPastDueOpenDetails
WHERE [DAYS_SINCE_FORWARDED] > 5
AND [ISSUE_OWNER] = 'Customer'
ORDER BY [VEND_LABL] ASC, [DAYS_PAST_DUE] DESC


if (@@rowcount = 0)
begin
insert into dbo.tbl_rpt_ForwardedPastDueOpenDetails_Customer_AppleEMEIA (SCAC,[INV_ID (810)],[INV_ID (TRAX)]) 
values('No Records','','')
end

-- ISSUE OWNER: TRAX
IF OBJECT_ID('dbo.tbl_rpt_ForwardedPastDueOpenDetails_Trax_AppleEMEIA') IS NOT NULL DROP TABLE dbo.tbl_rpt_ForwardedPastDueOpenDetails_Trax_AppleEMEIA
SELECT
	[VEND_LABL] AS [SCAC],
	[CURRENCY] AS [PAYMT CURRENCY],
	[ISSUE_CATEGORY] AS [ISSUE CATEGORY],
	[ISSUE_DETAILS] AS [ISSUE DETAILS],
	[DAYS_PAST_DUE] AS [DAYS (PAST DUE)] ,
	[DAYS_SINCE_FORWARDED] AS [DAYS (FORWARDED)],
	[PAYMENT_FORWARDED_INV_ID] AS [INV_ID (810)],
	[INV_KEY] AS [INV_KEY (CARRIER)],
	[BU] AS [INV BUSINESS UNIT],
	[ACCT_NUM_VEND_BLNG] AS [ACCOUNT NUMBER],
	[VENDOR_NUMBER] AS [VENDOR NUMBER],
	[COMPANY_CODE] AS [COMPANY CODE],
	[INV_APP_AMT] AS [PAYMT REQ AMT],	
	CONVERT (VARCHAR(25), [INV_DUE_DTM], 110) AS [INV DUE DATE],
	CONVERT (VARCHAR(25), [PAYMENT_FORWARDED_DATE], 110) AS [PAYMT FORWARDED DATE],
	[PAYMENT_FORWARDED_INFO] AS [810 FILE INFO],
	[824A_RECEIVED] AS [RECVD 824A?],
	CONVERT (VARCHAR(25), [824A_RECEIVED_DATE], 110) AS [824A RECVD DATE],
	[824A_ACTION] AS [824A ACT CODE],
	[BOOKING_KEY] AS [824A BOOKING KEY],
	[824A_RECEIVED_INFO] AS [824A FILE INFO],
	[824B_RECEIVED] AS [RECVD 824B?],
	[824B_CHECKNUM] AS [Check# In 824B],
	CONVERT (VARCHAR(25), [824B_DATE], 110) AS [824B RECVD DATE],
	[824B_INFO] AS [824B FILE INFO],
	[INV_ID] AS [INV_ID (TRAX)],
	[PAYMENT_STATE] AS [PAYMT STATUS],
	[REVIEW_REMARKS] AS [REVIEW REMARKS],
	[REVIEWED_BY] AS [REVIEWED BY],
	[REVIEWED_ON] AS [REVIEWED ON]
INTO dbo.tbl_rpt_ForwardedPastDueOpenDetails_Trax_AppleEMEIA
FROM #t_ForwardedPastDueOpenDetails
WHERE [DAYS_SINCE_FORWARDED] > 5
AND [ISSUE_OWNER] = 'Trax'
ORDER BY [VEND_LABL] ASC, [DAYS_PAST_DUE] DESC


if (@@rowcount = 0)
begin
insert into dbo.tbl_rpt_ForwardedPastDueOpenDetails_Trax_AppleEMEIA (SCAC,[INV_ID (810)],[INV_ID (TRAX)]) 
values('No Records','','')
end

--DROP TABLE #t_ForwardedPastDueOpenDetails


------- testing area

--select top 10 f.*, replace(d.[%T005], 'Accept|','') as [Booking_key]

--Update f
--set [BOOKING_KEY] = replace(d.[%T005], 'Accept|','')
--from #t_ForwardedPastDueOpenDetails f (nolock)
--inner join Aeur_Prod_02M5.dbo.Invoice_Ext ie (nolock) on f.INV_ID = ie.INV_ID and f.[824A_ACTION] = 'Accept'
--inner join Aeur_Prod_02M5.dbo.viewInvoiceMaster im (nolock) on ie.InvNId = im.MasterInvNId
--inner join Aeur_Prod_02M5.dbo.Invoice_Ext ie2 (nolock) on im.EnvType = 'Payment' and im.InvNId = ie2.InvnId
--inner join Aeur_Prod_02M5.dbo.PAYR_DTL d (nolock) on ie2.INV_ID = d.INV_ID


--select * from #t_ForwardedPastDueOpenDetails

-- select * from dbo.tbl_rpt_ForwardedPastDueOpenDetails_Summary_AppleEMEIA
--select * from dbo.tbl_rpt_ForwardedPastDueOpenDetails_Trax_AppleEMEIA
--select * from dbo.tbl_rpt_ForwardedPastDueOpenDetails_Customer_AppleEMEIA