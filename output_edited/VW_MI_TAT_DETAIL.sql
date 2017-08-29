CREATE VIEW VW_MI_TAT_DETAIL (POLICY_NO, SOURCE, LOCATION, BUSINESS, PLAN_CODE, ADVISOR_NAME, FIRM_NAME, OWNER, LIFE_ASSURED, POLICY_STATUS, START_TIME, UPLOAD_TIME, PROPOSAL_SIGN_DATE, PROPOSAL_RECEIVE_DATE, ISSUE_DATE, DISPATCH_DATE, AVIVA_TAT, TRANSACTION_NAME, DOCUMENT_TYPE, AGENT_NAME, END_TIME, PROCESS_STATUS, LAST_USER, LAST_ACTIVITY, DECISION, TATDAYS, EXC_PARAM_REGION, EXC_PARAM_PERIOD) AS 
	SELECT 	p."NUMBER" AS POLICY_NO,
	w.SOURCE AS SOURCE,
	p.REGION_CODE AS LOCATION,
	w.LOB AS BUSINESS,
	'N/A' AS PLAN_CODE,
	'N/A' AS ADVISOR_NAME,
	c.AGENTNAME AS FIRM_NAME,
	c.PROPOSALNAME AS OWNER,
	u.LIFEGIVENNAME AS LIFE_ASSURED,
	c.CONTRACTSTATUS AS POLICY_STATUS,
	w.DATE_CREATED AS START_TIME,
	w.DATE_CREATED AS UPLOAD_TIME,
	c.PROPOSALSIGNEDDATE AS PROPOSAL_SIGN_DATE,
	c.PROPOSALRECEIVEDDATE AS PROPOSAL_RECEIVE_DATE,
	'N/A' AS ISSUE_DATE,
	'N/A' AS DISPATCH_DATE,
	'N/A' AS AVIVA_TAT,
	w.TRANSACTION_TYPE AS TRANSACTION_NAME,
	w.DOCUMENT_TYPE AS DOCUMENT_TYPE,
	c.AGENTNAME AS AGENT_NAME,
	w.DATE_MODIFIED AS END_TIME,
	w.STATUS AS PROCESS_STATUS,
	v.MODIFIED_BY AS LAST_USER,
	v.TASK_NAME AS LAST_ACTIVITY,
	'N/A' AS DECISION,
	'N/A' AS TATDAYS,
	p.REGION_CODE AS EXC_PARAM_REGION,
	v.DATE_CREATED AS EXC_PARAM_PERIOD
	FROM CMDEV.PLANS p
	LEFT JOIN CMDEV.WORKITEMS w ON p."NUMBER" = w.PLAN_ID
	LEFT JOIN CMDEV.VW_QUEUES_TOP v ON w.ID = v.WORKITEM_ID
	LEFT JOIN ODSDEV.CONTRACT@ABC_LINK c ON p."NUMBER" = c.POLICYNO
	LEFT JOIN ODSDEV.CONTRACTSTATUS@ABC_LINK c1 ON c.POLICYNO = c1.POLICYNO
	LEFT JOIN ODSDEV.UWWORKSHEET@ABC_LINK u ON c1.APPLICATIONNO = u.RECEIPT
	WHERE w.STATUS = 'Completed';