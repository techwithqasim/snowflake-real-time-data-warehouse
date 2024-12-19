---- Loading  Data Using:

-- WEB UI
-- SNOWSQL
-- Cloud Provider
-- Streaming Data from AWS S3 using Snowpipe

-------------x----------------x---------x

--===================================
-- Loading Data using Web Interface
--===================================

-- Creating a Testing Database
CREATE DATABASE TEST_DB;

USE DATABASE TEST_DB;

-- Customer Table
CREATE TABLE CUSTOMER_DETAILS (
    first_name STRING,
    last_name STRING,
    address STRING,
    city STRING,
    state STRING
);

-- Table Should Be Empty
SELECT * FROM CUSTOMER_DETAILS;

-- Now Load data into CUSTOMER_DETAILS

SELECT * FROM CUSTOMER_DETAILS;

----------------------x----------------------------------------x--------------------------------------x

TRUNCATE TABLE CUSTOMER_DETAILS;

--===================================
-- Loading Data using SnowCLI
--===================================

-- Login using snowsql

snowsql -a <organization>-<identifier> -u <username>
;

-- Create pipe format
CREATE OR REPLACE FILE FORMAT PIPE_FORMAT_CLI
	type = 'CSV'
	field_delimiter = '|'
	skip_header = 1;
	
-- Create a stage table
CREATE OR REPLACE STAGE PIPE_CLI_STAGE
	file_format = PIPE_FORMAT_CLI;

-- Put Data Into Stage
PUT
file://<file_location>
@PIPE_CLI_STAGE auto_compress=true;

-- List Stage to see how many files are there
LIST @PIPE_CLI_STAGE;

-- Resume warehouse, in case the auto-resume feature is OFF
ALTER WAREHOUSE <name> RESUME;

-- Copy Data from Stage to Table
COPY INTO CUSTOMER_DETAILS
	FROM @PIPE_CLI_STAGE
	file_format = (format_name = PIPE_FORMAT_CLI)
	on_error = 'skip_file';
	
-- We can also give a COPY command with the pattern  if  your stage contains multiple  files
COPY INTO mycsvtable
	FROM @mycsvstage
	file_format = (format_name = PIPE_FORMAT_CLI)
	pattern = '*.contain[1-5].csv.gz'
	on_error = 'skip_file';

-- After Loading Data into CUSTOMER_DETAILS

SELECT * FROM CUSTOMER_DETAILS LIMIT 5;

----------------------x----------------------------------------x--------------------------------------x

--=========================================
-- Loading data using Cloud Provider (AWS S3)
--=========================================

-- Create Tesla Table
CREATE OR REPLACE TABLE TESLA_STOCKS(
    date DATE,
    open_value DOUBLE,
    high_vlaue DOUBLE,
    low_value DOUBLE,
    close_vlaue DOUBLE,
    adj_close_value DOUBLE,
    volume BIGINT
);

-- Table Should be Empty
SELECT * FROM TESLA_STOCKS;

-- External Stage Creation
CREATE OR REPLACE STAGE COPY_TESLA_STOCKS
URL = "<s3 bucket URL>/TESLA.csv"
CREDENTIALS = (AWS_KEY_ID='<access_key>', AWS_SECRET_KEY='<secret_key>');

-- List Stage
LIST @COPY_TESLA_STOCKS;

-- Copy Data from Stage to Table
COPY INTO TESLA_STOCKS
	FROM @COPY_TESLA_STOCKS
	file_format = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1)
    on_error = 'skip_file';

-- Data should be There Now
SELECT * FROM TESLA_STOCKS;

----------------------x----------------------------------------x--------------------------------------x

TRUNCATE TABLE TESLA_STOCKS;

--=========================================
-- Storage Integration
--=========================================

-- Giving Privileges CREATE INTEGRATION from ACCOUNTADMIN to SYSADMIN
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<aws role arn>'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<bucket-prefix URL>');

-- Valdating Integration
DESC INTEGRATION S3_INTEGRATION;

-- Need to add the snowflake STORAGE_AWS_IAM_USER_ARN in Trust relationships of AWS IAM ROLE
-- Example: arn:aws:iam::390402576589:user/91bt0000-s

-- Creating Stage
CREATE OR REPLACE STAGE S3_INTEGRATION_COPY_TESLA_STOCKS
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = '<s3 bucket-prefix URL>'
  FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1);

-- Validating Stage
LIST @S3_INTEGRATION_COPY_TESLA_STOCKS;

-- Making sure the Table is Empty
TRUNCATE TABLE TESLA_STOCKS;

SELECT * FROM TESLA_STOCKS;

-- Copy data using Integration Stage
COPY INTO TESLA_STOCKS FROM @S3_INTEGRATION_COPY_TESLA_STOCKS;

-- Data should be there Now
SELECT * FROM TESLA_STOCKS;

----------------------x----------------------------------------x--------------------------------------x

--=========================================
-- Storage Integration Using Snowpipe
--=========================================

-- 1. Stage the Data
-- 2. Test the Copy Command
-- 3. Create Pipe
-- 4. Configure Cloud Event / Call Snow Pipe Rest API

-- Truncating Data Again
TRUNCATE TABLE TESLA_STOCKS;

SELECT * FROM TESLA_STOCKS;

-- dropping previously created integration & stage
DROP STORAGE INTEGRATION S3_INTEGRATION;
DROP STAGE S3_INTEGRATION_COPY_TESLA_STOCKS;

-- Giving Privileges CREATE INTEGRATION from ACCOUNTADMIN to SYSADMIN
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- Step 1: Configure access permissions (policy) for the S3 bucket
-- Step 2: Create the IAM Role in AWS and attach above policy you created.

-- Step 3: Create a Cloud Storage Integration in Snowflake
CREATE OR REPLACE STORAGE INTEGRATION S3_TESLA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<aws role arn>'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<bucket-prefix URL>');

-- Step 4: Retrieve the AWS IAM User for your Snowflake Account
DESC INTEGRATION S3_TESLA_INTEGRATION;

-- Step 5: Grant the IAM User Permissions to Access Bucket Objects
-- STORAGE_AWS_IAM_USER_ARN 
-- STORAGE_AWS_EXTERNAL_ID

-- Step 6: Create file format for external stage
CREATE OR REPLACE FILE FORMAT S3_TESLA_STAGE_FORMAT
    TYPE= 'CSV'
    FIELD_DELIMITER=','
    SKIP_HEADER=1;

-- Step 6: Create an external stage using file format createbavove
CREATE OR REPLACE STAGE S3_TESLA_STAGE
  STORAGE_INTEGRATION = S3_TESLA_INTEGRATION
  URL = '<bucket-prefix URL>'
  FILE_FORMAT = S3_TESLA_STAGE_FORMAT;

-- Validating Stage
LIST @S3_TESLA_STAGE;

-- Step 7: Create a COPY Into Command
-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-copy

COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- Validating & Dropping again for SnowPipe
SELECT * FROM TESLA_STOCKS;
TRUNCATE TABLE TESLA_STOCKS;

--  Creating Pipe 
CREATE OR REPLACE PIPE S3_TESLA_EVENT_NOTIFICATION_PIPE AUTO_INGEST=TRUE AS
COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- Configure Cloud Event / Call Snow Pipe Rest API (S3_TESLA_EVENT_NOTIFICATION_PIPE)
SHOW PIPES;

-- Data should be there auotmatically
SELECT * FROM TESLA_STOCKS;

-- DROPPING PIPE
DROP PIPE S3_TESLA_EVENT_NOTIFICATION_PIPE;

----------------------x----------------------------------------x--------------------------------------x

--==============
-- TIME TRAVEL
--==============

SELECT * FROM TESLA_STOCKS ORDER BY DATE DESC;

-- Dropping & Getting back the Table (Time Travel)
DROP TABLE TESLA_STOCKS;

SELECT * FROM TESLA_STOCKS ORDER BY DATE DESC;

UNDROP TABLE TESLA_STOCKS;

-- Updating Values
UPDATE TESLA_STOCKS SET OPEN_VALUE=200 WHERE DATE = '2022-08-01';

-- getting data beofre last upodate query
SELECT * FROM TESLA_STOCKS BEFORE (statement => '01b913cc-0001-788b-0000-000206366e61') ORDER BY DATE DESC;


--=========================================
-- Quick Sight Visualization - Snowflake Database Server
--=========================================

SELECT CURRENT_ACCOUNT();

SELECT CURRENT_REGION();

-- <account_locator>.<region>.aws.snowflakecomputing.com