"""
SQL Query Templates for Daily Supervision Pull

This module contains all SQL query templates used by the daily supervision pull script.
"""

# Placeholder values for f-string evaluation 
# These will be replaced with actual dates when .format() is called in pull_data.py
# The f-string evaluates these to literal '{start_date}' strings which .format() can then replace
start_date = '{start_date}'
end_date = '{end_date}'

# SQL query template for direct service data (ServiceCode = '97153')
# Excludes BCBAs from being direct providers
DIRECT_SERVICES_SQL_TEMPLATE = f"""
SELECT DISTINCT
    b.BillingEntryId,
    b.ClientContactId,
    c.ClientFullName,
    c.ClientOfficeLocationName,
    b.ProviderContactId,
    pdir.FirstName AS ProviderFirstName,
    pdir.LastName AS ProviderLastName,
    sc.ServiceCode,
    b.ServiceStartTime,
    b.ServiceEndTime,
    COALESCE(b.ServiceLocationName, '(Unknown)') AS ServiceLocationName
FROM [insights].[dw2].[BillingEntriesCurrent] AS b
INNER JOIN [insights].[insights].[ServiceCode] AS sc
    ON b.ServiceCodeId = sc.ServiceCodeId
INNER JOIN [insights].[insights].[Client] AS c
    ON b.ClientContactId = c.ClientId
LEFT JOIN [insights].[dw2].[Contacts] AS pdir
    ON pdir.ContactId = b.ProviderContactId
LEFT JOIN [insights].[insights].[Employee] AS e
    ON e.EmployeeFirstName = pdir.FirstName
   AND e.EmployeeLastName = pdir.LastName
WHERE b.ServiceEndTime >= '{start_date}'
  AND b.ServiceEndTime <  '{end_date}'
  AND sc.ServiceCode IN ('97153', 'PDS | Technicians')
  AND (e.EmploymentPosition NOT IN ('BCBA', 'Board Certified Behavior Analyst')
       OR e.EmploymentPosition IS NULL)
ORDER BY
    c.ClientFullName,
    b.ServiceStartTime;
"""

# SQL query template for supervision service data (ServiceCode IN ('97155','Non-billable: PM Admin','PDS | BCBA'))
SUPERVISION_SERVICES_SQL_TEMPLATE = f"""
SELECT
    b.BillingEntryId,
    b.ClientContactId,
    c.ClientFullName,
    c.ClientOfficeLocationName,
    b.ProviderContactId,
    psup.FirstName AS ProviderFirstName,
    psup.LastName AS ProviderLastName,
    sc.ServiceCode,
    b.ServiceStartTime,
    b.ServiceEndTime,
    COALESCE(b.ServiceLocationName, '(Unknown)') AS ServiceLocationName
FROM [insights].[dw2].[BillingEntriesCurrent] AS b
INNER JOIN [insights].[insights].[ServiceCode] AS sc
    ON b.ServiceCodeId = sc.ServiceCodeId
INNER JOIN [insights].[insights].[Client] AS c
    ON b.ClientContactId = c.ClientId
LEFT JOIN [insights].[dw2].[Contacts] AS psup
    ON psup.ContactId = b.ProviderContactId
WHERE b.ServiceEndTime >= '{start_date}'
  AND b.ServiceEndTime <  '{end_date}'
  AND sc.ServiceCode IN (
    '97155','Non-billable: PM Admin','PDS | BCBA', '0362T', '0368T', '0373T', 
    'H0032', 'H0032 Program Management Student BCBS PREMERA', 'H2019', 'H2033'
  )
ORDER BY
    c.ClientFullName,
    b.ServiceStartTime;
"""

BACB_SUPERVISION_TEMPLATE = f"""
-- PARAMETERS
DECLARE @StartDate date = '{start_date}';
DECLARE @EndDate   date = '{end_date}';

SELECT
    b.ProviderContactId,
    BACBSupervisionCodes_binary = CAST(1 AS bit),
    BACBSupervisionHours = CAST(SUM(DATEDIFF(MINUTE, b.ServiceStartTime, b.ServiceEndTime)) / 60.0 AS DECIMAL(10,2))
FROM [insights].[dw2].[BillingEntriesCurrent] b
JOIN [insights].[insights].[ServiceCode] sc
  ON sc.ServiceCodeId = b.ServiceCodeId
WHERE b.ServiceEndTime >= @StartDate
  AND b.ServiceEndTime <  @EndDate
  AND b.ProviderContactId IS NOT NULL
  AND (
        sc.ServiceCode LIKE '%BACB%Supervision%Meeting%client%'   -- (w/out client)
     OR sc.ServiceCode LIKE '%VA%Medicaid%Supervision%client%'    -- (w/o Client)
  )
GROUP BY b.ProviderContactId
ORDER BY b.ProviderContactId;
"""

# SQL query template for employee locations (maps provider names to office locations)
EMPLOYEE_LOCATIONS_SQL_TEMPLATE = """
SELECT DISTINCT
    c.ContactId AS ProviderContactId,
    c.FirstName AS ProviderFirstName,
    c.LastName AS ProviderLastName,
    p.ProviderOfficeLocationName AS WorkLocation
FROM [insights].[dw2].[Contacts] AS c
INNER JOIN [insights].[insights].[Provider] AS p
    ON p.ProviderFirstName = c.FirstName
   AND p.ProviderLastName = c.LastName
ORDER BY c.LastName, c.FirstName;
"""