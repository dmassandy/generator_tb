CREATE VIEW VW_MI_CLOSED_CASE (POLICY_NO, BUSINESS, PLAN_CODE, POLICY_STATUS, PROPOSAL_SIGN_DATE, PROPOSAL_RECEIVE_DATE, UW_EFFECTIVE_DATE, ISSUE_DATE, SOURCE, FIRM_NAME, ADVISOR_NAME, OWNER, IS_CLEAN_CASE, AVIVA_TAT, SUSPEND_PERIOD, LOCATION, LIFE_ASSURED, START_TIME, UPLOAD_TIME, PROCESS_NAME, DISPATCH_DATE, PROCESS_END_DATE, TRANSACTION_NAME, DOCUMENT_TYPE, AGENT_NAME, END_TIME, PROCESS_STATUS, LAST_USER, LAST_ACTIVITY, DECISION, NO, PROPOSAL_NUMBER, PRODUCT_NAME, STATUS, SUM_ASSURED, TOTAL_PREMIUM, BILLING_FREQUENCY, ACTION_TIME, UW__USER, ISSUE_USER, AGENT_NUMBER, DISTRIBUTION_CHANNEL, PENDING_REASON, ACTION_USER, EXC_PARAM_REGION, EXC_PARAM_PERIOD) AS 
	SELECT 	p."NUMBER" AS POLICY_NO,
	w.LOB AS BUSINESS,
	'N/A' AS PLAN_CODE,
	c.CONTRACTSTATUS AS POLICY_STATUS,
	c.PROPOSALSIGNEDDATE AS PROPOSAL_SIGN_DATE,
	c.PROPOSALRECEIVEDDATE AS PROPOSAL_RECEIVE_DATE,
	'N/A' AS UW_EFFECTIVE_DATE,
	'N/A' AS ISSUE_DATE,
	w.SOURCE AS SOURCE,
	c.AGENTNAME AS FIRM_NAME,
	'N/A' AS ADVISOR_NAME,
	c.PROPOSALNAME AS OWNER,
	'' AS IS_CLEAN_CASE,
	'N/A' AS AVIVA_TAT,
	'CALCULATED_FIELD' AS SUSPEND_PERIOD,
	p.REGION_CODE AS LOCATION,
	u.LIFEGIVENNAME AS LIFE_ASSURED,
	w.DATE_CREATED AS START_TIME,
	w.DATE_CREATED AS UPLOAD_TIME,
	'' AS PROCESS_NAME,
	'N/A' AS DISPATCH_DATE,
	'N/A' AS PROCESS_END_DATE,
	w.TRANSACTION_TYPE AS TRANSACTION_NAME,
	'N/A' AS DOCUMENT_TYPE,
	c.AGENTNAME AS AGENT_NAME,
	'N/A' AS END_TIME,
	w.STATUS AS PROCESS_STATUS,
	v.MODIFIED_BY AS LAST_USER,
	v.TASK_NAME AS LAST_ACTIVITY,
	'N/A' AS DECISION,
	'N/A' AS NO,
	c1.APPLICATIONNO AS PROPOSAL_NUMBER,
	'N/A' AS PRODUCT_NAME,
	v.STATUS AS STATUS,
	u.SUMASSURED AS SUM_ASSURED,
	'N/A' AS TOTAL_PREMIUM,
	c.BILLINGFREQUENCY AS BILLING_FREQUENCY,
	v.DATE_CREATED AS ACTION_TIME,
	'N/A' AS UW__USER,
	'N/A' AS ISSUE_USER,
	c.COMMISSIONAGENTNO AS AGENT_NUMBER,
	'N/A' AS DISTRIBUTION_CHANNEL,
	'' AS PENDING_REASON,
	v.MODIFIED_BY AS ACTION_USER,
	p.REGION_CODE AS EXC_PARAM_REGION,
	v.DATE_CREATED AS EXC_PARAM_PERIOD
	FROM CMDEV.PLANS p
	LEFT JOIN CMDEV.WORKITEMS w ON p."NUMBER" = w.PLAN_ID
	LEFT JOIN CMDEV.VW_QUEUES_TOP v ON w.ID = v.WORKITEM_ID
	LEFT JOIN ODSDEV.CONTRACT@ABC_LINK c ON p."NUMBER" = c.POLICYNO
	LEFT JOIN ODSDEV.CONTRACTSTATUS@ABC_LINK c1 ON c.POLICYNO = c1.POLICYNO
	LEFT JOIN ODSDEV.UWWORKSHEET@ABC_LINK u ON c1.APPLICATIONNO = u.RECEIPT
	WHERE w.STATUS = 'Completed';