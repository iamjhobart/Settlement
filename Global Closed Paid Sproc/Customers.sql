use GlobalPaymentsProd

select top 1000 * from [Payments].[tbl_Payments_Logs]
where ProcessName = 'CloseFBVisibility' 
order by 3 desc

select top 1000 * from [Payments].[tbl_Payments_Logs]
where ProcessName = 'GlobalCloseFBDenieds' 
order by 3 desc

SELECT * FROM [Payments].[tbl_CentralConfiguration_CloseFB_Denied] CD
INNER JOIN [Payments].[tbl_CentralConfigurationEnvironments] CE ON CD.EnvironmentId = CE.EnvironmentId
ORDER BY CE.EnvironmentName

SELECT * FROM [Payments].[tbl_CentralConfiguration_CloseFB_Visibility] CV
INNER JOIN [Payments].[tbl_CentralConfigurationEnvironments] CE ON CE.EnvironmentId = CV.EnvironmentId
ORDER BY CE.EnvironmentName

/*
-- ExecutionDate

CAT Global Post Audit Prod				DONE
Coorstek NA Prod						DONE
Dupont EMEA Invoice Prod				DONE
DuPont EMEA Post Audit Match Prod		DONE
DuPont EMEA Post Audit Prod				DONE
Eli Lilly EMEA Prod						DONE
Eli Lilly LA Prod						DONE
Emerson Prod							DONE
Pfizer AMR QS Prod						DONE							
Pfizer APAC QS Prod						DONE
Pfizer LA Prod							DONE

-- InvoiceCreateDate
Texas Instruments AMR Prod

-- InvoiceDueDate
Boston Scientific Prod

-- ForwardDate
 Stryker EMEA Prod						DONE
 Stryker APAC Prod						DONE
 Merck EMEA Prod

*/


--InvoiceDueDate, InvoiceCreateDate, ForwardDate, ExecutionDate, InPaymentNew
/*
update CV
set CheckDateToUse = 'InvoiceCreateDate'
FROM [Payments].[tbl_CentralConfiguration_CloseFB_Visibility] CV
INNER JOIN [Payments].[tbl_CentralConfigurationEnvironments] CE ON CE.EnvironmentId = CV.EnvironmentId
where EnvironmentName = 'Texas Instruments AMR Prod'
*/

SELECT 
	CV.BusinessFlow as BusinessFlow,
	CV.ExecPath as ExecPath,
	NULL as StartDate,
	NULL as EndDate,
	CV.EnvironmentId,
	CE.EnvironmentName,
	0 as Executed,
	0 as Error,
	NULL as ErrorMsg
FROM [Payments].[tbl_CentralConfiguration_CloseFB_Visibility] CV
INNER JOIN [Payments].[tbl_CentralConfigurationEnvironments] CE ON CE.EnvironmentId = CV.EnvironmentId
ORDER BY CE.EnvironmentName