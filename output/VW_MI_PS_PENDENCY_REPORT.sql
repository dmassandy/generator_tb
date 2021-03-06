CREATE VIEW VW_MI_PS_PENDENCY_REPORT (POLICY_NO, SOURCE, LOCATION, BUSINESS, PLAN_CODE, TRANSACTION_NAME, OWNER, AGENT_NAME, LIFE_ASSURED, START_TIME, PROCESS_STATUS, LAST_USER, USER_ACTION_DATE, LAST_ACTIVITY, PEND_PERIOD, AGEING, RESUME_DATE, BRANCHREGION, SERVICING_AGENT_NAME, AGENT_CODE, PRODUCT, PAIDTODATE, SUBMITTED_PREMIUM_AMOUNT, PREMIUM_RECEIVING_DATE, REQUEST_RECEIVING_DATE, REQUEST_TYPE, PENDING_REASON, STANDARD_PENDING_PERIOD, ACTUAL_PENDING_TIME, OVER_TAT_DAY) AS 
	SELECT 	p.NUMBER AS POLICY_NO,
	w.SOURCE AS SOURCE,
	p.REGION_CODE AS LOCATION,
	w.LOB AS BUSINESS,
	'N/A' AS PLAN_CODE,
	w.TRANSACTION_TYPE AS TRANSACTION_NAME,
	c.PROPOSALNAME AS OWNER,
	c.AGENTNAME AS AGENT_NAME,
	u.LIFEGIVENNAME AS LIFE_ASSURED,
	w.DATE_CREATED AS START_TIME,
	w.STATUS AS PROCESS_STATUS,
	v.MODIFIED_BY AS LAST_USER,
	v.DATE_MODIFIED AS USER_ACTION_DATE,
	v.TASK_NAME AS LAST_ACTIVITY,
	'CALCULATED_FIELD' AS PEND_PERIOD,
	'CALCULATED_FIELD' AS AGEING,
	'CALCULATED_FIELD' AS RESUME_DATE,
	c.CONTRACTBRANCH AS BRANCHREGION,
	c.SERVAGENTNAME AS SERVICING_AGENT_NAME,
	c.SERVAGENTNO AS AGENT_CODE,
	'N/A' AS PRODUCT,
	'N/A' AS PAIDTODATE,
	u.SINGLEPREMIUM AS SUBMITTED_PREMIUM_AMOUNT,
	'N/A' AS PREMIUM_RECEIVING_DATE,
	c.PROPOSALRECEIVEDDATE AS REQUEST_RECEIVING_DATE,
	w.TRANSACTION_TYPE AS REQUEST_TYPE,
	'BLANK - FILL IN' AS PENDING_REASON,
	'CALCULATED_FIELD' AS STANDARD_PENDING_PERIOD,
	'CALCULATED_FIELD' AS ACTUAL_PENDING_TIME,
	'CALCULATED_FIELD' AS OVER_TAT_DAY
	FROM 	ODSDEV.UWWORKSHEET@ABC_LINK u,
	CMDEV.WORKITEMS w,
	CMDEV.PLANS p,
	ODSDEV.CONTRACT@ABC_LINK c,
	CMDEV.VW_QUEUES_TOP v;