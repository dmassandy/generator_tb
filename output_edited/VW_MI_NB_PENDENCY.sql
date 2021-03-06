CREATE VIEW VW_MI_NB_PENDENCY (POLICY_NO, SOURCE, LOCATION, BUSINESS, PLAN_CODE, ADVISOR_NAME, FIRM_NAME, OWNER, LIFE_ASSURED, POLICY_STATUS, START_TIME, UPLOAD_TIME, PROPOSAL_SIGN_DATE, PROPOSAL_RECEIVE_DATE, LAST_ACTIVITY, LAST_USER, USER_ACTION_DATE, NTU_DATE, RESUME_DATE, REASON, PEND_PERIOD, AGEING, NO, PROPOSAL_NUMBER, PRODUCT_NAME, STATUS_UW, PENDING_CODE_STATUSR_OR_W, SUM_ASSURED, TOTAL_PREMIUM, AGENT_NUMBER, AGENT_NAME, DISTRIBUTION_CHANNEL, STATUS, REMAIN_TIME, EXC_PARAM_REGION, EXC_PARAM_PERIOD) AS 
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
	v.TASK_NAME AS LAST_ACTIVITY,
	v.MODIFIED_BY AS LAST_USER,
	v.DATE_MODIFIED AS USER_ACTION_DATE,
	v.NTU_DATE AS NTU_DATE,
	'N/A' AS RESUME_DATE,
	'' AS REASON,
	'CALCULATED_FIELD' AS PEND_PERIOD,
	'CALCULATED_FIELD' AS AGEING,
	'N/A' AS NO,
	c1.APPLICATIONNO AS PROPOSAL_NUMBER,
	'N/A' AS PRODUCT_NAME,
	v.STATUS AS STATUS_UW,
	'N/A' AS PENDING_CODE_STATUSR_OR_W,
	u.SUMASSURED AS SUM_ASSURED,
	'N/A' AS TOTAL_PREMIUM,
	c.COMMISSIONAGENTNO AS AGENT_NUMBER,
	c.AGENTNAME AS AGENT_NAME,
	'N/A' AS DISTRIBUTION_CHANNEL,
	w.STATUS AS STATUS,
	'N/A' AS REMAIN_TIME,
	p.REGION_CODE AS EXC_PARAM_REGION,
	v.DATE_CREATED AS EXC_PARAM_PERIOD
	FROM CMDEV.PLANS p
	LEFT JOIN CMDEV.WORKITEMS w ON p."NUMBER" = w.PLAN_ID
	LEFT JOIN CMDEV.VW_QUEUES_TOP v ON w.ID = v.WORKITEM_ID
	LEFT JOIN ODSDEV.CONTRACT@ABC_LINK c ON p."NUMBER" = c.POLICYNO
	LEFT JOIN ODSDEV.CONTRACTSTATUS@ABC_LINK c1 ON c.POLICYNO = c1.POLICYNO
	LEFT JOIN ODSDEV.UWWORKSHEET@ABC_LINK u ON c1.APPLICATIONNO = u.RECEIPT
	WHERE w.PROCESS_CODE IN ('NBMP', 'NBALT', 'NBRP', 'NBACK', 'NBWD', 'NBRFD', 'NBROPN', 'NBRR')
	AND w.STATUS != 'Completed';