Automate Data Loads from S3 Dynamic Day-Wise Folders to Snowflake.

Create  database HELIX_HEALTH ;
Create schema HELIX_HEALTH_Raw ;


CREATE OR REPLACE STORAGE INTEGRATION HELIX_INTS            
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED=TRUE
STORAGE_ALLOWED_LOCATIONS =('s3://helix-healthcare-system/Dec/')         
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::827859361686:role/helixdeveloper';  


DESC INTEGRATION HELIX_INTS;

-- we need to update two main values in IAM ,
-- STORAGE_AWS_IAM_USER_ARN   
-- STORAGE_AWS_EXTERNAL_ID 

CREATE OR REPLACE FILE FORMAT HELIX_CSV_FORMATT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
RECORD_DELIMITER = '\n'

CREATE OR REPLACE STAGE CUST_DYNAMIC_STAGE
STORAGE_INTEGRATION=HELIX_INTS
URL='s3://helix-healthcare-system/Dec/'
FILE_FORMAT='HELIX_CSV_FORMATT';

list @CUST_DYNAMIC_STAGE ;

CREATE OR REPLACE TABLE patients_raw (
    patient_id              STRING,
    first_name               STRING,
    last_name                STRING,
    gender                   STRING,
    date_of_birth            DATE,
    phone_number             STRING,
    email                    STRING,
    address_line1            STRING,
    address_line2            STRING,
    city                     STRING,
    state                    STRING,
    postal_code              STRING,
    country                  STRING,
    primary_insurance_id     STRING,
    secondary_insurance_id   STRING,
    created_at               TIMESTAMP,
    updated_at               TIMESTAMP
);

DESC TABLE patients_raw;

CREATE OR REPLACE PROCEDURE dynamic_load()    
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    var_month_name   STRING;
    var_file_date    STRING;
    var_s3_path      STRING;
    var_copy_command STRING;
BEGIN
    -- Step 1: Extract date components
    var_month_name := TO_CHAR(CURRENT_DATE(), 'MON');        -- DEC
    var_file_date  := TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD'); -- 2025-12-24

    -- Step 2: Build exact S3 relative path
    var_s3_path := '/' || var_month_name || '/' || var_file_date || '.csv.c';

    -- Step 3: Build COPY command (single-line strings)
    var_copy_command :=
        'COPY INTO patients_raw ' ||
        'FROM @CUST_DYNAMIC_STAGE' || var_s3_path || ' ' ||
        'FILE_FORMAT = HELIX_CSV_FORMATT ' ||
        'ON_ERROR = CONTINUE';

    -- Step 4: Execute
    EXECUTE IMMEDIATE var_copy_command;

    RETURN 'SUCCESS | Executed: ' || var_copy_command;
END;
$$;


CALL dynamic_load();


select * from patients_raw ;


CREATE OR REPLACE TASK DAILY_LOAD_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
CALL DYNAMIC_LOAD();




















