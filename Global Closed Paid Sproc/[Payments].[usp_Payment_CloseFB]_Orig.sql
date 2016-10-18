/*

exec [Payments].[usp_Payment_CloseFB] '33F0CFCB-46B1-4278-9D88-A69CF6B0DDE3','Stryker.EMEA.','Visibility',1,0,0

select count(*) from Payments.CloseVisilityTestFBInfoBegin
select count(*) from Payments.CloseVisilityTestFBInfoEnd

--select count(*) from Payments.CloseVisilityTestFBInfoMulti
select * from Payments.CloseVisilityTestFBInfoBegin

drop table  Payments.CloseVisilityTestFBInfoBegin
drop table Payments.CloseVisilityTestFBInfoMulti
drop table  Payments.CloseVisilityTestFBInfoEnd

select top 100  * from dbo.[funds_app] order by app_dtm desc
  Payments.CloseVisilityTestFBInfoEnd


select p.funds_app_temp_id,p.paymt_req_amt,f.fb_app_amt
from dbo.payr_Dtl as p
inner join dbo.outfrght_bl f on f.fb_id=p.fb_id 
inner join dbo.Funds_ap as a on a.FUNDS_APP_TEMP_ID=p.funds_app_temp_id
*/

CREATE PROCEDURE [Payments].[usp_Payment_CloseFB]  
@EnvironmentId 			AS UNIQUEIDENTIFIER, 
@BusinessFlow  			AS [nvarchar](100),
@ExecPath	   			AS [nvarchar](100),
@CompletedInvoiceOnly  	AS Bit = 1,      
@GenerateRemitedAdvises AS Bit = 0,  
@NoOfDaysPrior 			AS Int = 2  
AS      
      
      
/*******************************************************************************      
** Software Development      
** Trax Holdings, Inc.      
**       
** Version: 4.0       
** Author:        
** Customer:        
**       
** Overview:      
**       
** Revision History:      
** When:     Who:      What:       
** 08/12/2008 Jenniffer Valverde  Creation      
** 08/08/2011 Gaby Vega   			Changes to make sproc generic      
** 08/09/2011 Jenniffer Valverde  	Standarization       
** 09/06/2011 Jenniffer Valverde  	Adding the parameters    
** 23/02/2012 Jessica Gonzalez    	Adding code for coorstek     
** 10/25/2013 Randy Argonillo   	Added @NoOfDaysPrior. This is to set date filter in gathering the fbs to be included for closing.  
** 10/31/2013 Randy Argonillo   	Added the section specific for Pfizer LA
** 06/27/2016 Jenniffer Valverde 	Add the EnvironmentId, Flow and Execution Paths.
**       
** Input Parameter Descriptions:      
**       
** Output Parameter Descriptions:      
**       
********************************************************************************/      
      
SET NOCOUNT ON      
      
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED      
      
/* ---------------------------------------------------------------------------------------- */      
/*     LOG SECTION         */      
/* -----------------------------------------------------------------------------------------*/      
      
DECLARE @s_db_name VARCHAR(250), @s_object_name VARCHAR(250), @n_exec_id INT      
      
SELECT @s_db_name = DB_NAME(), @s_object_name = OBJECT_NAME(@@PROCID)      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_start @s_db_name, @s_object_name, @n_exec_id = @n_exec_id OUTPUT      
      
/* ---------------------------------------------------------------------------------------- */      
/* DECLARATION SECTION             */      
/* -----------------------------------------------------------------------------------------*/      
DECLARE @OwnerName   				AS VARCHAR(50)  --Use in the Try Catch    
DECLARE @FixOwnerKey  				AS VARCHAR(7)
DECLARE @RowCount   				AS INT      
DECLARE @s_db_program_name   		AS VARCHAR(250)      
DECLARE @s_db_loginame    			AS VARCHAR(250)      
DECLARE @ERROR_MESSAGE    			AS VARCHAR(250)      
DECLARE @GenericKey   				AS VARCHAR(8)      
DECLARE @SUPPRESS_DATA_FEED_FLAG	AS INT      
  
      
SET @SUPPRESS_DATA_FEED_FLAG = 0     
SET @NoOfDaysPrior = @NoOfDaysPrior * -1   -- Randy.Argonillo 10/24/2013 Allow us to negate the value.  
  

DECLARE @EnvType	AS VARCHAR(20)
DECLARE @OwnerKey	AS VARCHAR(8)

SELECT @OwnerKey = CONVERT (VARCHAR(8),OWNER_KEY)
FROM OWNER_CONFIG
WHERE [TEXT]=CONVERT(VARCHAR(50),@EnvironmentId)
AND [CONFIG_LABL]='ENV_ID'



--select CONVERT (VARCHAR(8),c.OWNER_KEY),c.[TEXT],c.[CONFIG_LABL],o.Owner_name
--FROM OWNER_CONFIG as c
--INNER JOIN OWNERS as o on c.owner_key=o.owner_key
--WHERE [CONFIG_LABL]='ENV_ID'



SELECT @EnvType=[TEXT] 
FROM  OWNER_CONFIG 
WHERE [CONFIG_LABL]='ENV_TYPE'
AND OWNER_KEY=@OwnerKey

IF  @EnvType='MultiClient'
BEGIN
 
	SET  @FixOwnerKey = REPLACE(@OwnerKey,'-','')
  
    SET @OwnerName = (SELECT OWNER_NAME 
    FROM OWNERS 
    WHERE OWNER_KEY = @OwnerKey)  

END
ELSE
BEGIN 

	SET  @FixOwnerKey = (SELECT  REPLACE(OWNER_KEY,'-','')   
	FROM  OWNER_CONFIG   
	WHERE CONFIG_LABL = 'ENV_TYPE'   
	  AND TEXT = 'Payment')  
  
	SET  @OwnerKey = (SELECT OWNER_KEY   
	FROM  OWNER_CONFIG   
	WHERE CONFIG_LABL = 'ENV_TYPE'   
	  AND TEXT = 'Payment')    
  
	SET @OwnerName = (SELECT OWNER_NAME 
	FROM OWNERS 
	WHERE OWNER_KEY IN (SELECT OWNER_KEY 
					FROM OWNER_CONFIG 
					WHERE CONFIG_LABL = 'ENV_TYPE' 
					AND [TEXT] = 'MASTER'))  

END  

         
SELECT @s_db_program_name = program_name,       
 @s_db_loginame =loginame       
FROM master.dbo.sysprocesses (NOLOCK)       
WHERE spid = @@SPID      
      
SELECT @GenericKey = CAST(DatePart(yyyy,GetDate()) AS VARCHAR(4))      
  + CASE WHEN DatePart(mm,GetDate())<10 THEN '0' ELSE '' END      
  + CAST(DatePart(mm,GetDate())AS VARCHAR(2))       
  + CASE WHEN DatePart(dd,GetDate())<10 THEN '0' ELSE '' END      
  + CAST(DatePart(dd,GetDate()) AS VARCHAR(2))      
 
      
/* ---------------------------------------------------------------------------------------- */      
/*     EXECUTABLE SECTION              */      
/* -----------------------------------------------------------------------------------------*/      
-------------------------------      
-- Phase No.00 REGISTER SP   --      
-------------------------------      
      
IF NOT EXISTS (SELECT * FROM  [FilexTools].db.tbl_objects       
  WHERE server_name  = @@SERVERNAME       
    AND db_name   = @s_db_name      
    AND object_name = @s_object_name)       
BEGIN      
 EXEC [FilexTools].dbo.dba_FILEXTools_register_object       
 @s_db_name   = @s_db_name,       
 @s_object_schema  = 'dbo',       
 @s_object_name   = @s_object_name,       
 @s_object_operator  = 'FILEX\',      
 @s_object_desc   = 'Payment Invoice Auto Close ',      
 @s_object_source  = 'dbo.PAYR_DTL',      
 @s_object_type   = 'S', -- 'S' Stored Procedure, 'V' View, 'F' Function      
 @d_days_to_store  = -1, -- OR the amount of days to keep it (-1 means never)      
 @s_lock_object   = 'Y' --Y OR N. if Y object cannot be deleted even if is out-of-date      
END      
      
----------------------------------------------------------------------------------------------      
-- Phase No.00 Create and register the tables            --      
----------------------------------------------------------------------------------------------      
BEGIN TRY      
-------------------------------------------      
-- Phase No.01 Find the FB Info from FPS --      
-------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 1, 'Phase No.01 Selecting the #t_FBInfo'      
      
IF OBJECT_ID('tempdb..#t_FBInfo') IS NOT NULL DROP TABLE #t_FBInfo      
      
CREATE TABLE #t_FBInfo(      
 [FB_ID]   [varchar](23)  NOT NULL,      
 [INV_ID]   [varchar](23)  NOT NULL,  
 [InvNId]   [BIGINT]  NOT NULL,  
 [BAT_ID]   [varchar](23)  NULL,      
 [BAT_KEY]   [varchar](8)  NULL,      
 [Action_Code]   [varchar](7)  NOT NULL,      
 [PAYMT_REQ_APP_AMT]  [money]  NULL,      
 [ACT_REASON]   [varchar](20)  NULL,      
 [ACT_REASON_DESC]  [varchar](20)  NULL,      
 [OWNER_KEY]   [varchar](8)  NULL,      
 [VEND_LABL]   [varchar](50)  NULL,      
 [PAYMT_TYPE]   [varchar](50)  NULL,      
 [FB_APP_AMT]  [money]  NULL,      
 [CURRENCYCODE]   [varchar](50)  NOT NULL
) ON [PRIMARY]      

CREATE CLUSTERED INDEX [#t_FBInfo:ByFB_ID] ON #t_FBInfo(FB_ID)            
CREATE INDEX [#t_FBInfo:ByINV_ID] ON #t_FBInfo(INV_ID)      

IF  @EnvType='MultiClient'
BEGIN

      
	INSERT INTO #t_FBInfo      
	SELECT         
 		outFRGHT_BL.FB_ID,      
		outFRGHT_BL.INV_ID, 
		INVOICE_EXT.InvNId,     
		PAYR_DTL.BAT_ID,      
		outFRGHT_BL.BAT_KEY,      
		'Approve'   AS Action_Code,      
		outFRGHT_BL.FB_APP_AMT  AS PAYMT_REQ_APP_AMT,      
		CAST(NULL  AS [VARCHAR](20)) AS ACT_REASON,      
		CAST(NULL  AS [VARCHAR](20)) AS ACT_REASON_DESC,      
		outFRGHT_BL.OWNER_KEY,      
		outFRGHT_BL.VEND_LABL,      
		VENDOR_REMIT.PAYMT_TYPE,      
		outFRGHT_BL.FB_APP_AMT,      
		ISNULL(PAYR_DTL.PAYMT_REQ_CURRENCY_QUAL,'USD') AS CURRENCYCODE
      
	FROM  dbo.outFRGHT_BL WITH (NOLOCK)      
         
	INNER JOIN  dbo.PAYR_DTL WITH (NOLOCK)       
 		ON outFRGHT_BL.FB_ID=PAYR_DTL.FB_ID      
     
    INNER JOIN dbo.INVOICE_EXT WITH (NOLOCK)
    	ON PAYR_DTL.INV_ID=INVOICE_EXT.INV_ID
            
	INNER JOIN  dbo.VENDOR_REMIT WITH (NOLOCK)      
  		ON PAYR_DTL.OWNER_KEY = VENDOR_REMIT.OWNER_KEY       
  		AND outFRGHT_BL.VEND_LABL = VENDOR_REMIT.VEND_LABL       
  		AND PAYR_DTL.PAYMT_REQ_CURRENCY_QUAL = VENDOR_REMIT.CURRENCY_QUAL       
        
	WHERE    
	    dbo.outFRGHT_BL.FB_ID LIKE 'FBLL' + @FixOwnerKey + '%'     
       	AND (outFRGHT_BL.FB_STAT='Denied' or outFRGHT_BL.FB_STAT='Open')      
 		AND PAYR_DTL.RCRD_CREAT_DTM<=DATEADD(dd, @NoOfDaysPrior, GETDATE())      
 		AND PAYR_DTL.FUNDS_APP_TEMP_ID IS NULL      
 
		SET @RowCount=@@ROWCOUNT
END 
ELSE      
BEGIN      
	INSERT INTO #t_FBInfo      
	SELECT         
 		FRGHT_BL.FB_ID,      
		FRGHT_BL.INV_ID,
		INVOICE_EXT.InvNId,       
		PAYR_DTL.BAT_ID,      
		FRGHT_BL.BAT_KEY,      
		'Approve'   AS Action_Code,      
		FRGHT_BL.FB_APP_AMT  AS PAYMT_REQ_APP_AMT,      
		CAST(NULL  AS [VARCHAR](20)) AS ACT_REASON,      
		CAST(NULL  AS [VARCHAR](20)) AS ACT_REASON_DESC,      
		FRGHT_BL.OWNER_KEY,      
		FRGHT_BL.VEND_LABL,      
		VENDOR_REMIT.PAYMT_TYPE,      
		FRGHT_BL.FB_APP_AMT,      
		ISNULL(PAYR_DTL.PAYMT_REQ_CURRENCY_QUAL,'USD') AS CURRENCYCODE
      
	FROM  dbo.FRGHT_BL WITH (NOLOCK)      
      
	INNER JOIN  dbo.PAYR_DTL WITH (NOLOCK)            
  		ON FRGHT_BL.FB_ID=PAYR_DTL.FB_ID      

    INNER JOIN dbo.INVOICE_EXT WITH (NOLOCK)
    	ON PAYR_DTL.INV_ID=INVOICE_EXT.INV_ID
		        
	INNER JOIN  dbo.VENDOR_REMIT WITH (NOLOCK)      
  		ON PAYR_DTL.OWNER_KEY = VENDOR_REMIT.OWNER_KEY       
  		AND FRGHT_BL.VEND_LABL = VENDOR_REMIT.VEND_LABL       
  		AND PAYR_DTL.PAYMT_REQ_CURRENCY_QUAL = VENDOR_REMIT.CURRENCY_QUAL       
        
	WHERE
		dbo.FRGHT_BL.FB_ID LIKE 'FBLL' + @FixOwnerKey + '%'     
       	AND (FRGHT_BL.FB_STAT='Denied' or FRGHT_BL.FB_STAT='Open')      
 		AND PAYR_DTL.RCRD_CREAT_DTM<=DATEADD(dd, @NoOfDaysPrior, GETDATE())      
 		AND PAYR_DTL.FUNDS_APP_TEMP_ID IS NULL      
 
		SET @RowCount=@@ROWCOUNT      

END
       
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 1,  'Rowcount #t_FBInfo', @RowCount      
      
      
IF @RowCount > 0      
BEGIN      
      
     
-----------------------------------------------------------------------------------      
-- Phase No.02 Code added for SigmaAldrich close only the FB mark as visibility  --      
-----------------------------------------------------------------------------------      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 2, 'Phase No.02 Check the Visibility for SigmaAldrich '      
      
IF @OwnerKey = '005-0136'       
BEGIN      
      
 IF OBJECT_ID('tempdb..#t_Visibility') IS NOT NULL DROP TABLE #t_Visibility      
      
 CREATE TABLE #t_Visibility(      
  [FB_ID]   [varchar](23)  NOT NULL,      
  [%T002]   [varchar](255) NULL      
 ) ON [PRIMARY]      
       
       
 INSERT INTO #t_Visibility      
 SELECT F.FB_ID,      
  P.[%T002]      
 FROM  #t_FBInfo AS F      
 INNER JOIN PAYR_DTL AS P WITH(NOLOCK) ON P.FB_ID=F.FB_ID      
 WHERE P.[%T002] <> 'Visibility'      
       
 DELETE FROM #t_FBInfo       
 FROM #t_FBInfo  AS F      
 INNER JOIN #t_Visibility as V on V.FB_ID=F.FB_ID      
       
END      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 2,  'Rowcount #t_Visibility', @@ROWCOUNT      
      
----------------------------------------------------------------------------------------      
-- Phase No.03 Code added for CoorstekNAProd close only the FB mark as PostAuditFlow  --      
----------------------------------------------------------------------------------------      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 3, 'Phase No.03 Check the Payment Flow for Coorstek'      
      
IF @OwnerKey = '006-0462'       
BEGIN      
      
 IF OBJECT_ID('tempdb..#t_FlowId') IS NOT NULL DROP TABLE #t_FlowId      
      
 CREATE TABLE #t_FlowId(      
  [FB_ID]   [varchar](23)  NOT NULL,      
  [%t004]   [varchar](255) NULL      
 ) ON [PRIMARY]      
       
       
 INSERT INTO #t_FlowId      
 SELECT F.FB_ID,      
  P.[%t004]    
 FROM  #t_FBInfo AS F      
 INNER JOIN PAYR_DTL AS P WITH(NOLOCK) ON P.FB_ID=F.FB_ID      
 WHERE (P.[%t004] <> 'Prepaid' OR P.[%t004] IS NULL)    
       
 DELETE FROM #t_FBInfo       
 FROM #t_FBInfo  AS F      
 INNER JOIN #t_FlowId as V on V.FB_ID=F.FB_ID      
       
END      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M',3,  'Rowcount #t_Visibility', @@ROWCOUNT       
     
  
----------------------------------------------------------------------------------------      
-- Phase No.04 Code added for "TexasInstrumentsAmrProd" close only the FB mark as NoPayment  --      
----------------------------------------------------------------------------------------      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 4, 'Phase No.04 Check the Payment Flow for Coorstek'      
    
IF @OwnerKey = '006-0416'       
BEGIN    
      
 IF OBJECT_ID('tempdb..#t_TI_NoPayment') IS NOT NULL DROP TABLE #t_TI_NoPayment    
      
create table #t_TI_NoPayment(      
   [FB_ID] [varchar](23)  NOT NULL,    
   [FlowName] [varchar](255) NOT NULL    
 ) ON [PRIMARY]      
    
insert into #t_TI_NoPayment      
  select F.FB_ID,      
   P.GRP_KEY    
    from #t_FBInfo as f    
 inner join PAYR_DTL as p with(nolock)    
   on P.FB_ID=F.FB_ID      
   where P.GRP_KEY <> 'NoPayment'    
    
  delete F    
    from #t_FBInfo  AS F    
 inner join #t_TI_NoPayment as V    
   on V.FB_ID = F.FB_ID      
       
END      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M',4,  'Rowcount #t_TI_NoPayment', @@ROWCOUNT        
  
----------------------------------------------------------------------------------------        
-- Phase No.05 Code added for "PfizerLAProd" close only the FB mark as VE,CO or AR  --        
----------------------------------------------------------------------------------------        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 5, 'Phase No.05 Check the Payment Flow for PfizerLA'        
     
IF @OwnerKey = '005-0200'         
BEGIN        
    
 IF OBJECT_ID('tempdb..#t_PfizerLA') IS NOT NULL DROP TABLE #t_PfizerLA        
        
 CREATE TABLE #t_PfizerLA(        
  [INV_ID]   [varchar](23)  NOT NULL,        
  [INV_CREAT_DTM]   [datetime] NULL        
 ) ON [PRIMARY]        
         
         
 INSERT INTO #t_PfizerLA        
 SELECT F.INV_ID,    
 I.INV_CREAT_DTM    
 FROM  #t_FBInfo AS F        
 INNER JOIN PAYR_DTL AS P WITH(NOLOCK)     
 ON P.INV_ID=F.INV_ID      
 INNER JOIN INVOICE AS I WITH(NOLOCK)     
 ON I.INV_ID=F.INV_ID           WHERE ((F.VEND_LABL IN ('PALAAR','PALACO','PALAVE','PANLAR','PANLCO','PANLVE','JORI')  AND I.INV_CREAT_DTM < '2013-04-01'))    
 OR ((F.VEND_LABL IN ('LUXA') AND I.INV_CREAT_DTM < '2013-07-01'))      
    
    
 DELETE FROM #t_FBInfo      
 FROM #t_FBInfo  AS F        
 LEFT JOIN #t_PfizerLA as V    
 on V.INV_ID=F.INV_ID        
 WHERE v.INV_ID IS NULL    

         
END        
        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M',5,  'Rowcount #t_Visibility', @@ROWCOUNT   

--------------------------------------------------------------------------------------------        
-- Phase No.06 Code added for Eli Lilly EMEA. Close only invoices in "PostAudit" flow  --        
--------------------------------------------------------------------------------------------        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 6, 'Phase No.06 Check the Payment Flow for Eli Lilly EMEA'        
        
IF @OwnerKey = '005-0250' -- OWNER KEY for Eli Lilly EMEA        
BEGIN        
    
	IF OBJECT_ID('tempdb..#t_EliLillyEMEA') IS NOT NULL DROP TABLE #t_EliLillyEMEA        
	    
	CREATE TABLE #t_EliLillyEMEA(        
		[FB_ID]   [varchar](23)  NOT NULL,        
		[GRP_KEY]   [varchar](200) NULL        
	) ON [PRIMARY]
	   
	INSERT INTO #t_EliLillyEMEA -- BILLS TO BE EXCLUDED FROM THE AUTO-CLOSE PROCESS    
		SELECT 
			F.FB_ID,    
			P.GRP_KEY    
		FROM  #t_FBInfo AS F        
		INNER JOIN PAYR_DTL AS P WITH(NOLOCK)     
			ON P.FB_ID=F.FB_ID
		WHERE 
			ISNULL(P.GRP_KEY,'') NOT IN ('PostAudit','Default')    -- PreAudit, etc
    
	DELETE FROM #t_FBInfo      
	FROM #t_FBInfo  AS F        
	INNER JOIN #t_EliLillyEMEA as V    
		ON F.FB_ID = V.FB_ID
	
END        
        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M',6,  'Rowcount #t_EliLillyEMEA', @@ROWCOUNT   
  
--------------------------------------------------------------------------------------------        
-- Phase No.07 Code added for MULTICLIENT   Close only invoices sent in payment with the duedate of the current date--        
--------------------------------------------------------------------------------------------        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 7, 'Phase No.07 Check the Payment Flow for EMERSON'        
   
IF  @EnvType='MultiClient'
BEGIN        

	IF OBJECT_ID('tempdb..#t_Multiclient') IS NOT NULL DROP TABLE #t_Multiclient        
	    
	CREATE TABLE #t_Multiclient(        
		[FB_ID]   [varchar](23)  NOT NULL,        
	) ON [PRIMARY]
	   
	INSERT INTO #t_Multiclient -- BILLS TO BE EXCLUDED FROM THE AUTO-CLOSE PROCESS    
		SELECT 
			F.FB_ID  
		FROM  #t_FBInfo AS F        
		INNER JOIN BNorm.FbNormFactLayer AS flow WITH(NOLOCK)     
			ON flow.FB_ID=F.FB_ID
		INNER JOIN PAYR_DTL AS P WITH(NOLOCK)     
			ON P.FB_ID=F.FB_ID		
		WHERE 		
			   NOT flow.BusinessFlow LIKE '%' + ISNULL(@BusinessFlow,'') + '%'     
			OR NOT flow.ExecPath LIKE '%' + ISNULL(@ExecPath,'') + '%' 
			OR (P.FWRD_KEY IS NULL AND P.FUNDS_REQ_KEY IS NULL)
			OR ((NOT P.FWRD_KEY IS NULL OR NOT P.FUNDS_REQ_KEY IS NULL) AND P.fb_due_dtm >= getdate())  

	DELETE FROM #t_FBInfo      
	FROM #t_FBInfo  AS F        
	INNER JOIN #t_Multiclient as V    
		ON F.FB_ID = V.FB_ID


END        
        
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M',7,  'Rowcount #t_Multiclient', @@ROWCOUNT  


--------------------------------------------------------      
-- Phase No.08 Check the Complete Invoices --      
--------------------------------------------------------      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 8, 'Phase No.08 Check the CompletedInvoiceOnly '      
      
IF @CompletedInvoiceOnly=1      
BEGIN      
      
    
 IF OBJECT_ID('tempdb..#t_InvoiceInfo') IS NOT NULL DROP TABLE #t_InvoiceInfo      
      
 CREATE TABLE #t_InvoiceInfo(      
  [INV_ID]   [varchar](23)  NOT NULL,      
  [FB_CNT]   [int]  NULL      
 ) ON [PRIMARY]      
      
 CREATE CLUSTERED INDEX [#t_InvoiceInfo:ByINV_ID] ON #t_InvoiceInfo(INV_ID)      
      
 INSERT INTO #t_InvoiceInfo      
 SELECT DISTINCT INV_ID,      
  COUNT(DISTINCT FB_ID) AS FB_CNT      
 FROM  #t_FBInfo      
 GROUP BY INV_ID      
    
 IF OBJECT_ID('tempdb..#t_InvoiceInfoRemit') IS NOT NULL DROP TABLE #t_InvoiceInfoRemit      
 CREATE TABLE #t_InvoiceInfoRemit(      
  [INV_ID]   [varchar](23)  NOT NULL,      
  [FB_CNT]   [int]  NULL      
 ) ON [PRIMARY]      
     
 INSERT INTO    #t_InvoiceInfoRemit     
 SELECT DISTINCT R.INV_ID,    
  COUNT (DISTINCT R.FB_ID) AS FB_CNT    
 FROM  #t_InvoiceInfo AS I    
 INNER JOIN REMITDTL AS R WITH (NOLOCK) ON  R.INV_ID=I.INV_ID    
 GROUP BY R.INV_ID    
    
 UPDATE I    
 SET I.[FB_CNT]=I.[FB_CNT]+R.FB_CNT    
 FROM #t_InvoiceInfo AS I    
 INNER JOIN #t_InvoiceInfoRemit AS R ON R.INV_ID=I.INV_ID    
      
 DELETE FROM #t_FBInfo       
 FROM #t_FBInfo  AS f      
 INNER JOIN #t_InvoiceInfo as a on a.inv_id=f.inv_id      
 INNER JOIN INVOICE i on a.INV_ID = i.INV_ID       
 WHERE a.FB_CNT <> i.INV_FB_CNT      
      
END      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 8,  'Rowcount #t_FBInfo', @@ROWCOUNT      
    
---------------------------------------------------------      
-- Phase No.09 Check the InvRoot for Complete Invoices --      
---------------------------------------------------------      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 9, 'Phase No.09 Check CompletedInvoiceOnly by InvRoot '      
      
IF @CompletedInvoiceOnly=1      
BEGIN      

	if object_id('tempdb..#Initial') is not null drop table #Initial
	create table #Initial(
		PaymentInvNId bigint,
		PaymentInvId varchar(23),
		MasterInvNId bigint,
		RootInvNId bigint
	)

	if object_id('tempdb..#InvsInRemitDtl') is not null drop table #InvsInRemitDtl
	create table #InvsInRemitDtl(
		InvNId bigint,
		INV_ID varchar(23),
		MasterInvNId bigint,
		RootInvCnt bigint
	)

	if object_id('tempdb..#RootInvCnt') is not null drop table #RootInvCnt
	create table #RootInvCnt(
		RootInvNId bigint,
		InvPartCnt int
	)

	if object_id('tempdb..#InvsWithPartCnt') is not null drop table #InvsWithPartCnt
	create table #InvsWithPartCnt(
		PaymentInvNId bigint,
		PaymentInvId varchar(23),
		MasterInvNId bigint,
		RootInvNId bigint,
		InvPartCnt int
	)

	if object_id('tempdb..#RootInvsExpectedCnt') is not null drop table #RootInvsExpectedCnt
	create table #RootInvsExpectedCnt(
		RootInvNId bigint,
		InvPartCnt int
	)

	if object_id('tempdb..#ActualCntVsExpectedCnt') is not null drop table #ActualCntVsExpectedCnt
	create table #ActualCntVsExpectedCnt(
		PaymentInvNId bigint,
		PaymentInvId varchar(23),
		MasterInvNId bigint,
		RootInvNId bigint,
		PayrDtlInvCnt int,
		RootInvCnt int
	)


	IF  @EnvType='MultiClient'
	BEGIN 
	
		insert into #Initial
		select distinct src.invnid as PaymentInvNId
			, src.inv_id as PaymentInvId
			, rt.invnid as MasterInvNId
			, rt.rootinvnid as RootInvNId
		from #t_FBInfo src
		inner join invoice_ext ext on ext.invnid = src.invnid
		inner join invroot rt on rt.invnid = ext.invnid


		insert into #InvsInRemitDtl
		select distinct ext.invnid, inv.inv_id, ext.invnid, rt.rootinvnid
		from (
			select distinct rootinvnid
			from #Initial rootInvs
			group by rootinvnid) roots
		inner join invroot rt on rt.rootinvnid = roots.rootinvnid
		inner join invoice_ext ext on ext.invnid = rt.invnid
		inner join invoice inv on inv.inv_id = ext.inv_id
		inner join frght_bl fb on fb.inv_id = inv.inv_id
		inner join remitdtl rm on rm.fb_id = fb.fb_id
		group by ext.invnid, inv.inv_id,  rt.invnid, rt.rootinvnid

		-- Add the remitdtl invoices to the src table.
		-- Exclude those that are already included in the src table. 
		-- This will happens when an invoice is partially close which means
		-- invoices appears both in Payr_Dtl and RemitDtl
		insert into #Initial
		select * 
		from #InvsInRemitDtl rem
		where not exists(select * from #initial src where src.paymentinvid = rem.inv_id)

		insert into #RootInvCnt
		select rootinvnid, count(rootinvnid) as InvPartCnt
		from #Initial t2 
		group by rootinvnid

		insert into #InvsWithPartCnt
		select t1.*, t2.InvPartCnt
		from #Initial t1
		inner join #RootInvCnt t2 on t1.rootinvnid = t2.rootinvnid
	
		insert into #RootInvsExpectedCnt
		select rt.rootinvnid, count(*) as InvPartCnt
		from invroot rt
		inner join #RootInvCnt t2 on rt.rootinvnid = t2.rootinvnid
		group by rt.rootinvnid

		
		insert into #ActualCntVsExpectedCnt
		select t1.PaymentInvNId, t1.PaymentInvId, t1.MasterInvNId, t1.RootInvnId, t1.InvPartCnt as PayrDtlInvCnt, t2.InvPartCnt as RootInvCnt
		from #InvsWithPartCnt t1
		inner join #RootInvsExpectedCnt t2 on t2.rootinvnid = t1.rootinvnid

 		DELETE FROM #t_FBInfo  
		FROM #t_FBInfo  AS f      
 		INNER JOIN #ActualCntVsExpectedCnt as a on a.paymentinvid=f.inv_id     
 		WHERE a.PayrDtlInvCnt <> RootInvCnt    
  	END
  	ELSE
	BEGIN

		insert into #Initial
		select DISTINCT src.invnid as PaymentInvNId
			, src.inv_id as PaymentInvId
			, rt.invnid as MasterInvNId
			, rt.rootinvnid as RootInvNId
		from #t_FBInfo src
		inner join invoice_ext ext on ext.invnid = src.invnid
		inner join invoicemaster mas on mas.invnid = ext.invnid
		inner join invroot rt on rt.invnid = mas.masterinvnid

		
		insert into #InvsInRemitDtl
		select distinct ext.invnid, inv.inv_id, mas.masterinvnid, rt.rootinvnid
		from (
			select rootinvnid
			from #Initial rootInvs
			group by rootinvnid) roots
		inner join invroot rt on rt.rootinvnid = roots.rootinvnid
		inner join viewinvoicemaster mas on mas.masterinvnid = rt.invnid and mas.envtype = 'payment'
		inner join invoice_ext ext on ext.invnid = mas.invnid
		inner join invoice inv on inv.inv_id = ext.inv_id
		inner join frght_bl fb on fb.inv_id = inv.inv_id
		inner join remitdtl rm on rm.fb_id = fb.fb_id
		group by ext.invnid, inv.inv_id, mas.masterinvnid, rt.rootinvnid

		-- Add the remitdtl invoices to the src table.
		-- Exclude those that are already included in the src table. 
		-- This will happens when an invoice is partially close which means
		-- invoices appears both in Payr_Dtl and RemitDtl
		insert into #Initial
		select * 
		from #InvsInRemitDtl rem
		where not exists(select * from #initial src where src.paymentinvid = rem.inv_id)


		-- get the invs cnt for root
		insert into #RootInvCnt
		select rootinvnid, count(rootinvnid) as InvPartCnt
		from #Initial t2 
		group by rootinvnid

		insert into #InvsWithPartCnt
		select t1.*, t2.InvPartCnt
		from #Initial t1
		inner join #RootInvCnt t2 on t1.rootinvnid = t2.rootinvnid


		insert into #RootInvsExpectedCnt
		select rt.rootinvnid, count(*) as InvPartCnt
		from invroot rt
		inner join #RootInvCnt t2 on rt.rootinvnid = t2.rootinvnid
		group by rt.rootinvnid

		insert into #ActualCntVsExpectedCnt
		select t1.PaymentInvNId, t1.PaymentInvId, t1.MasterInvNId, t1.RootInvnId, t1.InvPartCnt as PayrDtlInvCnt, t2.InvPartCnt as RootInvCnt
		from #InvsWithPartCnt t1
		inner join #RootInvsExpectedCnt t2 on t2.rootinvnid = t1.rootinvnid
 		
 		DELETE FROM #t_FBInfo       
 		FROM #t_FBInfo  AS f      
 		INNER JOIN #ActualCntVsExpectedCnt as a on a.paymentinvid=f.inv_id     
 		WHERE a.PayrDtlInvCnt <> RootInvCnt    


  	END    
END      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 9,  'Rowcount #t_FBInfo', @@ROWCOUNT         
--------------------------------------------------------      
-- Phase No.10 Insert the t_PaymentHeader information --      
--------------------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 10, 'Phase No.10 Insert #t_PaymentHeader'      
      
IF OBJECT_ID('tempdb..#t_PaymentHeader') IS NOT NULL DROP TABLE #t_PaymentHeader      
CREATE TABLE #t_PaymentHeader(      
 [PAYMENT_ID]  [bigint] NULL,      
 [OWNER_KEY]  [varchar](8) NULL,      
 [VEND_LABL]  [varchar](50) NULL,      
 [BILLS]  [int] NULL,      
 [AMOUNT]  [money] NULL,      
 [CURRENCYCODE]  [varchar](50) NOT NULL,      
 [PAYMT_TYPE]  [varchar](50) NULL      
) ON [PRIMARY]      
      
INSERT INTO #t_PaymentHeader      
SELECT  Row_Number() OVER (ORDER BY OWNER_KEY,  VEND_LABL, CURRENCYCODE, PAYMT_TYPE) as PAYMENT_ID,      
  OWNER_KEY,      
  VEND_LABL,       
  COUNT(FB_ID)  AS BILLS,      
  SUM(PAYMT_REQ_APP_AMT) AS AMOUNT,      
  CURRENCYCODE,      
  PAYMT_TYPE      
        
FROM  #t_FBInfo AS F      
      
GROUP BY OWNER_KEY,      
  VEND_LABL,       
  CURRENCYCODE,      
  PAYMT_TYPE      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 10,  'Rowcount #t_PaymentHeader', @@ROWCOUNT      
      
--------------------------------------------------------      
-- Phase No.11 Insert the t_PaymentDetail information --      
--------------------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 11, 'Phase No.11 Insert #t_PaymentDetail'      
      
IF OBJECT_ID('tempdb..#t_PaymentDetail') IS NOT NULL DROP TABLE #t_PaymentDetail       
CREATE TABLE #t_PaymentDetail(      
 [PAYMENT_ID]   [bigint] NULL,      
 [FB_ID]   [varchar](23) NOT NULL,      
 [PAYMT_REQ_KEY]  [varchar](50) NULL,      
 [Action_Code]   [varchar](7) NOT NULL,      
 [PAYMT_REQ_APP_AMT]  [money] NULL,      
 [ACT_REASON]   [varchar](50) NULL,      
 [ACT_REASON_DESC]  [varchar](255) NULL,      
 [OWNER_KEY]   [varchar](8) NULL,      
 [VEND_LABL]   [varchar](50) NULL,      
 [PAYMT_TYPE]   [varchar](50) NULL,      
 [FB_APP_AMT]   [money] NULL,      
 [CURRENCYCODE]   [varchar](50) NOT NULL      
) ON [PRIMARY]      
      
INSERT INTO  #t_PaymentDetail      
SELECT   Header.PAYMENT_ID,      
  Detail.FB_ID,      
  CAST(Detail.BAT_KEY AS VARCHAR(50))  AS PAYMT_REQ_KEY,      
  Detail.Action_Code,      
  Detail.PAYMT_REQ_APP_AMT,      
  Detail.ACT_REASON,      
  Detail.ACT_REASON_DESC,      
  Detail.OWNER_KEY,      
  Detail.VEND_LABL,      
  Detail.PAYMT_TYPE,      
  Detail.FB_APP_AMT,      
  Header.CURRENCYCODE       
        
        
FROM  #t_FBInfo   AS Detail      
INNER JOIN  #t_PaymentHeader AS Header       
 ON  Header.VEND_LABL =Detail.VEND_LABL      
 AND Header.CURRENCYCODE =Detail.CURRENCYCODE      
      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 11,  'Rowcount #t_PaymentDetail', @@ROWCOUNT      
      
-------------------------------------------      
-- Phase No.12 Preparing the #t_FUNDS_AP --      
-------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 12, 'Phase No.12 Insert #t_FUNDS_AP'      
      
IF OBJECT_ID('tempdb..#t_FUNDS_AP') IS NOT NULL DROP TABLE #t_FUNDS_AP       
CREATE TABLE #t_FUNDS_AP(      
 [OWNER_KEY]    [varchar](8) NULL,      
 [FUNDS_APP_TEMP_ID]   [varchar](23) NULL,      
 [FUNDS_APP_NAME]   [varchar](11) NULL,      
 [VEND_LABL]    [varchar](10) NULL,      
 [FUNDS_APP_KEY]   [varchar](50) NULL,      
 [FUNDS_APP_REF_KEY]   [varchar](50) NULL,      
 [FUNDS_APP_KEY_ORIG]   [varchar](50) NULL,      
 [FUNDS_APP_TYPE]   [varchar](50) NULL,      
 [CREAT_DTM]    [datetime] NOT NULL,      
 [SENT_TO_KEY]    [varchar](50) NOT NULL,      
 [FUNDS_APP_AMT]   [money] NULL,      
 [FUNDS_APP_CURRENCY_QUAL]  [varchar](50) NOT NULL,      
 [SENT_DTM]    [datetime] NULL,      
 [FUNDS_APP_STAT]   [varchar](50) NOT NULL,      
 [FUNDS_APP_MEMO]   [varchar](255) NOT NULL,      
 [SUPPRESS_DATA_FEED_FLAG]  [int] NOT NULL,      
 [RCRD_USER_LOGON_KEY]   [varchar](20) NULL,      
 [RCRD_CREAT_DTM]   [datetime] NOT NULL,      
 [RCRD_WKS_CODE]   [varchar](4) NOT NULL,      
 [MSG_GRP_NUM]    [varchar](8) NOT NULL    
) ON [PRIMARY]      
      
      
IF @GenerateRemitedAdvises=0      
BEGIN      
 SET @SUPPRESS_DATA_FEED_FLAG=1      
END       
      
INSERT INTO #t_FUNDS_AP      
      
SELECT   Header.OWNER_KEY,      
  Payments.fn_getNextFundsApTempId(@OwnerKey,Header.PAYMENT_ID)  AS FUNDS_APP_TEMP_ID,      
  @GenericKey        AS FUNDS_APP_NAME,      
  SUBSTRING(Header.VEND_LABL,1,10)     AS VEND_LABL,      
  CAST(NULL AS VARCHAR(25))      AS FUNDS_APP_KEY,      
  CAST(NULL AS VARCHAR(25))      AS FUNDS_APP_REF_KEY,      
  CAST(NULL AS VARCHAR(25))      AS FUNDS_APP_KEY_ORIG,      
  SUBSTRING(Header.PAYMT_TYPE,1,50)     AS FUNDS_APP_TYPE,      
  GetDate()        AS CREAT_DTM,      
  ''         AS SENT_TO_KEY,      
  Header.AMOUNT        AS FUNDS_APP_AMT,      
  Header.CURRENCYCODE       AS FUNDS_APP_CURRENCY_QUAL,      
  NULL         AS SENT_DTM,      
  'In Process'        AS FUNDS_APP_STAT, --New      
  'Auto Close Visibility' AS FUNDS_APP_MEMO,      
  @SUPPRESS_DATA_FEED_FLAG      AS SUPPRESS_DATA_FEED_FLAG,      
  SUBSTRING(@s_db_loginame,1,20)      AS RCRD_USER_LOGON_KEY,      
  GetDate()        AS RCRD_CREAT_DTM,      
  ''         AS RCRD_WKS_CODE,      
  'TA Creat'        AS MSG_GRP_NUM --AutoDeny      
      
FROM  #t_PaymentHeader   AS Header      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 11,  'Rowcount #t_FUNDS_AP', @@ROWCOUNT      
      
UPDATE #t_FUNDS_AP      
SET   FUNDS_APP_KEY  = 'VIS-' + @GenericKey + SUBSTRING(FUNDS_APP_TEMP_ID, LEN([FUNDS_APP_TEMP_ID] )-4, 5),      
  FUNDS_APP_REF_KEY  = 'VIS-' + @GenericKey + SUBSTRING(FUNDS_APP_TEMP_ID, LEN([FUNDS_APP_TEMP_ID] )-4, 5),      
  FUNDS_APP_KEY_ORIG = 'VIS-' + @GenericKey + SUBSTRING(FUNDS_APP_TEMP_ID, LEN([FUNDS_APP_TEMP_ID] )-4, 5)      
FROM #t_FUNDS_AP      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 12,  'Rowcount #t_FUNDS_AP', @@ROWCOUNT      
      
--------------------------------------------------      
-- Phase No.13 Preparing the #t_PAYR_DTL_Update --      
--------------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 13, 'Phase No.13 Insert #t_PAYR_DTL_Update'      
      
IF OBJECT_ID('tempdb..#t_PAYR_DTL_Update') IS NOT NULL DROP TABLE #t_PAYR_DTL_Update      
CREATE TABLE #t_PAYR_DTL_Update(      
 [OWNER_KEY]   [varchar](8) NULL,      
 [PAYMT_REQ_KEY]  [varchar](50) NULL,      
 [FB_ID]   [varchar](23) NOT NULL,      
 [PAYMT_REQ_APP_AMT]  [money] NULL,      
 [REQ_ACT_CODE]   [varchar](50) NOT NULL,      
 [ACT_REASON]   [varchar](50) NULL,      
 [ACT_REASON_DESC]  [varchar](255) NULL,      
 [FUNDS_APP_TEMP_ID]  [varchar](23) NULL,      
 [RCRD_USER_LOGON_KEY]  [varchar](20) NULL,      
 [RCRD_CREAT_DTM]  [datetime] NOT NULL,      
 [RCRD_WKS_CODE]  [varchar](4) NOT NULL,      
 [MSG_GRP_NUM]   [varchar](8) NOT NULL      
) ON [PRIMARY]      
      
      
INSERT INTO  #t_PAYR_DTL_Update      
SELECT        
  #t_PaymentDetail.OWNER_KEY,      
  #t_PaymentDetail.[PAYMT_REQ_KEY],      
  #t_PaymentDetail.[FB_ID],      
  #t_PaymentDetail.[PAYMT_REQ_APP_AMT],      
  #t_PaymentDetail.Action_Code AS [REQ_ACT_CODE],      
  #t_PaymentDetail.[ACT_REASON],      
  #t_PaymentDetail.[ACT_REASON_DESC],      
  #t_FUNDS_AP.[FUNDS_APP_TEMP_ID],      
  #t_FUNDS_AP.RCRD_USER_LOGON_KEY,      
  #t_FUNDS_AP.RCRD_CREAT_DTM,      
  #t_FUNDS_AP.RCRD_WKS_CODE,      
  [MSG_GRP_NUM]      
      
FROM  #t_PaymentDetail      
INNER JOIN #t_FUNDS_AP  ON  #t_PaymentDetail.CURRENCYCODE=#t_FUNDS_AP.FUNDS_APP_CURRENCY_QUAL      
     AND  #t_PaymentDetail.VEND_LABL=#t_FUNDS_AP.VEND_LABL      
     AND  #t_PaymentDetail.PAYMT_TYPE=#t_FUNDS_AP.FUNDS_APP_TYPE      
     AND  #t_PaymentDetail.OWNER_KEY=#t_FUNDS_AP.OWNER_KEY      
--select * from #t_PAYR_DTL_Update      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 13,  'Rowcount #t_PAYR_BAT_Update', @@ROWCOUNT      
      
-------------------------------------------      
-- Phase No.14 Preparing the #t_FA_TO_PB --      
-------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 14, 'Phase No.14 Insert #t_FA_TO_PB'      
      
IF OBJECT_ID('tempdb..#t_FA_TO_PB') IS NOT NULL DROP TABLE #t_FA_TO_PB      
      
CREATE TABLE #t_FA_TO_PB(      
 [OWNER_KEY]    [varchar](8) NULL,      
 [FUNDS_APP_TEMP_ID]   [varchar](23) NULL,      
 [PAYMT_REQ_KEY]   [varchar](50) NULL,      
 [FA_TO_PB_APP_AMT]   [money] NULL,      
 [FA_TO_PB_CURRENCY_QUAL]  [varchar](50) NOT NULL,      
 [RCRD_USER_LOGON_KEY]   [varchar](20) NULL,      
 [RCRD_CREAT_DTM]   [datetime] NOT NULL,      
 [RCRD_WKS_CODE]   [varchar](4) NOT NULL,      
 [CRD_MSG_STAT_FLAG]   [int] NULL      
) ON [PRIMARY]      
      
      
INSERT INTO #t_FA_TO_PB      
SELECT        
  #t_PaymentDetail.OWNER_KEY,       
  #t_FUNDS_AP.FUNDS_APP_TEMP_ID,      
  #t_PaymentDetail.PAYMT_REQ_KEY,      
  SUM(#t_PaymentDetail.PAYMT_REQ_APP_AMT) AS FA_TO_PB_APP_AMT,      
  #t_PaymentDetail.CURRENCYCODE  AS FA_TO_PB_CURRENCY_QUAL,      
  #t_FUNDS_AP.RCRD_USER_LOGON_KEY,      
  #t_FUNDS_AP.RCRD_CREAT_DTM,      
  #t_FUNDS_AP.RCRD_WKS_CODE,      
  NULL     AS CRD_MSG_STAT_FLAG      
        
      
FROM  #t_PaymentDetail         
INNER JOIN #t_FUNDS_AP  ON  #t_PaymentDetail.CURRENCYCODE=#t_FUNDS_AP.FUNDS_APP_CURRENCY_QUAL      
     AND  #t_PaymentDetail.VEND_LABL=#t_FUNDS_AP.VEND_LABL      
     AND  #t_PaymentDetail.PAYMT_TYPE=#t_FUNDS_AP.FUNDS_APP_TYPE      
     AND  #t_PaymentDetail.OWNER_KEY=#t_FUNDS_AP.OWNER_KEY     
           
GROUP BY  #t_PaymentDetail.OWNER_KEY,       
  #t_FUNDS_AP.FUNDS_APP_TEMP_ID,      
  #t_PaymentDetail.PAYMT_REQ_KEY,      
  #t_PaymentDetail.CURRENCYCODE,      
  #t_FUNDS_AP.RCRD_USER_LOGON_KEY,      
  #t_FUNDS_AP.RCRD_CREAT_DTM,      
  #t_FUNDS_AP.RCRD_WKS_CODE      
        
--select * from #t_FA_TO_PB      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 14,  'Rowcount #t_FA_TO_PB', @@ROWCOUNT      
      
--------------------------------------------------      
-- Phase No.15 Preparing the #t_PAYR_BAT_Update --      
--------------------------------------------------      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 15, 'Phase No.15 Insert #t_PAYR_BAT_Update'      
      
IF OBJECT_ID('tempdb..#t_PAYR_BAT_Update') IS NOT NULL DROP TABLE #t_PAYR_BAT_Update      
      
CREATE TABLE #t_PAYR_BAT_Update(      
 [OWNER_KEY]   [varchar](8) NULL,      
 [PAYMT_REQ_KEY]  [varchar](50) NULL,      
 [PAYMT_REQ_APP_AMT]  [money] NULL,      
 [PAYMT_REQ_STAT]  [varchar](50) NOT NULL,      
 [PAYMT_REQ_APP_STAT]  [varchar](50) NOT NULL,      
 [RCRD_USER_LOGON_KEY]  [varchar](20) NULL,      
 [RCRD_CREAT_DTM]  [datetime] NOT NULL,      
 [RCRD_WKS_CODE]  [varchar](4) NOT NULL,      
 [MSG_GRP_NUM]   [varchar](8) NOT NULL      
) ON [PRIMARY]      
      
      
INSERT INTO #t_PAYR_BAT_Update      
      
SELECT DISTINCT  #t_PaymentDetail.OWNER_KEY,      
  #t_PaymentDetail.PAYMT_REQ_KEY,      
  SUM(#t_PaymentDetail.PAYMT_REQ_APP_AMT) AS PAYMT_REQ_APP_AMT,      
  'Received'    AS PAYMT_REQ_STAT,      
  'Partial'    AS PAYMT_REQ_APP_STAT,      
  SUBSTRING(@s_db_loginame,1,20)  AS RCRD_USER_LOGON_KEY,      
  GetDate()    AS RCRD_CREAT_DTM,      
  ''     AS RCRD_WKS_CODE,      
  'TA Creat'    AS MSG_GRP_NUM --AutoDeny      
        
      
FROM  #t_PaymentDetail         
      
       
GROUP BY  #t_PaymentDetail.OWNER_KEY,       
  #t_PaymentDetail.PAYMT_REQ_KEY,      
  #t_PaymentDetail.CURRENCYCODE      
        
--SELECT  * INTO Payments.t_PAYR_DTL_Update FROM #t_PAYR_DTL_Update  

/*SELECT  p.* 
from Payments.t_PAYR_DTL_Update as u
inner join payr_Dtl as p on p.fb_id=u.fb_id
*/      
    
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 15,  'Rowcount #t_PAYR_BAT_Update', @@ROWCOUNT      
/*****************************************************************************************************************************/      
/*****************************************************************************************************************************/      
/*****************************************************************************************************************************/      
/*****************************************************************************************************************************/      
      
EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 16, 'Phase No.16 Updating PAYR_DTL'      

      
IF @@ERROR = 0      
BEGIN      
 BEGIN TRAN      
      
      
UPDATE  dbo.PAYR_DTL       
SET       
 PAYR_DTL.PAYMT_REQ_APP_AMT  = #t_PAYR_DTL_Update.PAYMT_REQ_APP_AMT,       
 PAYR_DTL.REQ_ACT_CODE   = #t_PAYR_DTL_Update.REQ_ACT_CODE,       
 PAYR_DTL.ACT_REASON   = #t_PAYR_DTL_Update.ACT_REASON,       
 PAYR_DTL.ACT_REASON_DESC  = #t_PAYR_DTL_Update.ACT_REASON_DESC,       
 PAYR_DTL.FUNDS_APP_TEMP_ID  = #t_PAYR_DTL_Update.FUNDS_APP_TEMP_ID,       
 PAYR_DTL.RCRD_USER_LOGON_KEY  = #t_PAYR_DTL_Update.RCRD_USER_LOGON_KEY,       
 PAYR_DTL.RCRD_CREAT_DTM  = #t_PAYR_DTL_Update.RCRD_CREAT_DTM,       
 PAYR_DTL.RCRD_WKS_CODE   = #t_PAYR_DTL_Update.RCRD_WKS_CODE,       
 PAYR_DTL.MSG_GRP_NUM   = #t_PAYR_DTL_Update.MSG_GRP_NUM       
FROM  #t_PAYR_DTL_Update       
WHERE  PAYR_DTL.OWNER_KEY = #t_PAYR_DTL_Update.OWNER_KEY       
AND  PAYR_DTL.PAYMT_REQ_KEY = #t_PAYR_DTL_Update.PAYMT_REQ_KEY       
AND  PAYR_DTL.FB_ID = #t_PAYR_DTL_Update.FB_ID       
          
-----------------------------------      
-- Phase No.16 Updating PAYR_BAT --      
-----------------------------------      
      
UPDATE  dbo.PAYR_BAT       
SET       
 PAYR_BAT.PAYMT_REQ_APP_AMT=#t_PAYR_BAT_Update.PAYMT_REQ_APP_AMT,      
 PAYR_BAT.PAYMT_REQ_STAT=#t_PAYR_BAT_Update.PAYMT_REQ_STAT ,       
 PAYR_BAT.PAYMT_REQ_APP_STAT=#t_PAYR_BAT_Update.PAYMT_REQ_APP_STAT,       
 PAYR_BAT.RCRD_USER_LOGON_KEY=#t_PAYR_BAT_Update.RCRD_USER_LOGON_KEY,       
 PAYR_BAT.RCRD_CREAT_DTM=#t_PAYR_BAT_Update.RCRD_CREAT_DTM,       
 PAYR_BAT.RCRD_WKS_CODE=#t_PAYR_BAT_Update.RCRD_WKS_CODE,       
 PAYR_BAT.MSG_GRP_NUM=#t_PAYR_BAT_Update.MSG_GRP_NUM      
FROM #t_PAYR_BAT_Update       
WHERE PAYR_BAT.OWNER_KEY = #t_PAYR_BAT_Update.OWNER_KEY       
AND PAYR_BAT.PAYMT_REQ_KEY = #t_PAYR_BAT_Update.PAYMT_REQ_KEY      
      
-----------------------------------      
-- Phase No.17 Updating FA_TO_PB --      
-----------------------------------      
            
INSERT INTO  dbo.FA_TO_PB       
SELECT       
[OWNER_KEY],      
[FUNDS_APP_TEMP_ID],      
[PAYMT_REQ_KEY],      
[FA_TO_PB_APP_AMT],      
[FA_TO_PB_CURRENCY_QUAL],      
[RCRD_USER_LOGON_KEY],      
[RCRD_CREAT_DTM],      
[RCRD_WKS_CODE],      
NULL AS [RCRD_MSG_STAT_FLAG]      
FROM #t_FA_TO_PB;      
         
-----------------------------------      
-- Phase No.18 Updating FUNDS_AP --      
-----------------------------------      
      
INSERT INTO  dbo.FUNDS_AP       
SELECT       
 #t_FUNDS_AP.[OWNER_KEY],      
 #t_FUNDS_AP.[FUNDS_APP_TEMP_ID],      
 #t_FUNDS_AP.[FUNDS_APP_NAME],      
 #t_FUNDS_AP.[VEND_LABL],      
 #t_FUNDS_AP.[FUNDS_APP_KEY],      
 #t_FUNDS_AP.[FUNDS_APP_REF_KEY],      
 #t_FUNDS_AP.[FUNDS_APP_KEY_ORIG],      
 #t_FUNDS_AP.[FUNDS_APP_TYPE],      
 #t_FUNDS_AP.[CREAT_DTM],      
 #t_FUNDS_AP.[SENT_TO_KEY],      
 #t_FUNDS_AP.[FUNDS_APP_AMT],      
 #t_FUNDS_AP.[FUNDS_APP_CURRENCY_QUAL],      
 #t_FUNDS_AP.[SENT_DTM],      
 #t_FUNDS_AP.[FUNDS_APP_STAT],      
 #t_FUNDS_AP.[FUNDS_APP_MEMO],      
 #t_FUNDS_AP.[SUPPRESS_DATA_FEED_FLAG],      
 #t_FUNDS_AP.[RCRD_USER_LOGON_KEY],      
 #t_FUNDS_AP.[RCRD_CREAT_DTM],      
 #t_FUNDS_AP.[RCRD_WKS_CODE],      
  #t_FUNDS_AP.[MSG_GRP_NUM]      
FROM #t_FUNDS_AP      
         
------------------------------------------------------      
-- Phase No.19 Process the Payment under the queued --      
------------------------------------------------------      
     
INSERT INTO  dbo.PROCESS_QUEUE      
SELECT       
      
 1    AS [PROCESS_PRIORITY_HIGH],      
 80    AS [PROCESS_PRIORITY_LOW],      
 'Queued'   AS [PROCESS_STAT],      
 'Submit Check: ' + #t_FUNDS_AP.VEND_LABL + '  ' + #t_FUNDS_AP.FUNDS_APP_KEY AS [PROCESS_LABL],      
 ''    AS [EXPR_COND],      
 'Submit_Funds_App_Batch(''' + #t_FUNDS_AP.OWNER_KEY + ''', ''[' + #t_FUNDS_AP.FUNDS_APP_TEMP_ID  + ']'','''')' AS [FUNC_MEMO],      
 ''    AS [LOCK_LIST],      
 ''    AS [PROCESS_NUM_PREREQ_LIST],      
 ''    AS [PROCESS_DATA],      
 'Process Payments'  AS [CREAT_PROCESS_LABL],      
 GetDate()   AS [CREAT_DTM],      
 SUBSTRING(@s_db_loginame,1,20) AS [CREAT_USER_LOGON_KEY],      
 ''    AS [CREAT_WKS_KEY],      
 0    AS [START_CNT],      
 NULL    AS [START_NUM],      
 NULL    AS [START_SESSION_NUM],      
 NULL    AS [START_DTM],      
 NULL AS [START_USER_LOGON_KEY],      
 NULL    AS [START_WKS_KEY],      
 NULL    AS [END_DTM],      
 NULL    AS [RESTART_DATA],      
 NULL    AS [FUNC_RSLT],      
 NULL    AS [PROCESS_FAIL_REASON]      
FROM #t_FUNDS_AP      
      
       
 IF @@ERROR = 0      
 BEGIN      
   EXEC FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'M', 16,  'Rowcount PROCESS_QUEUE', @@ROWCOUNT        
   COMMIT TRAN      
 END      
 ELSE      
 BEGIN      
  ROLLBACK TRAN      
   EXECUTE FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 0, 'Error executing Payment Fb Auto close'      
   EXECUTE FILEXTools.[dbo].[dba_FILEXTools_exec_track_fail] @n_exec_id      
         
   RAISERROR ('Error executing Payment Fb Auto close', 16, 1)      
   RETURN      
 END      
END      
END      
END TRY      
BEGIN CATCH      
       
       
 RAISERROR (N'Error executing the Close Fb store procedure',16, 1)      
      
 SELECT @ERROR_MESSAGE = 'Close Fb Sproc failed At: ' + ERROR_PROCEDURE() + '; ' + 'Line: ' + CAST(ERROR_LINE() AS VARCHAR(5)) + '; ' + ERROR_MESSAGE()      
      
 EXECUTE FILEXTools.dbo.dba_FILEXTools_exec_track_phase @n_exec_id, 'P', 0, @ERROR_MESSAGE      
 EXECUTE FILEXTools.[dbo].[dba_FILEXTools_exec_track_fail] @n_exec_id      
      
 DECLARE @subjectText   AS VARCHAR(250)      
 SET @subjectText = @OwnerName + '-usp_Payment_CloseFB:Close Fb Sproc failed'      
      
 EXEC msdb.dbo.sp_send_dbmail @recipients='paymentsupport@traxtech.com;databaseadministrators@traxtech.com',@subject = @subjectText,      
     @body = @ERROR_MESSAGE, @body_format = 'HTML';       
       
      
END CATCH      
EXEC [FilexTools].dbo.dba_FILEXTools_exec_track_END @n_exec_id,16