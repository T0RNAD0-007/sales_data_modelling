USE ROLE ACCOUNTADMIN;

DROP USER PRESET;
DROP ROLE REPORTER;

CREATE ROLE IF NOT EXISTS REPORTER;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS PRESET
PASSWORD = 'xxxx'
LOGIN_NAME = 'PRESET'
MUST_CHANGE_PASSWORD = FALSE
DEFAULT_WAREHOUSE = 'COMPUTE_WH'
DEFAULT_ROLE = 'REPORTER'
DEFAULT_NAMESPACE = 'AMAZON.DEV'
COMMENT = 'PRESET user for CREATING REPORTS';

GRANT ROLE REPORTER TO USER PRESET;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AMAZON TO ROLE REPORTER;
GRANT USAGE ON SCHEMA AMAZON.DEV TO ROLE REPORTER;
