
-------------------------------------RAW DATA LAYER DATA LOADING ASSETS -----------------------------------------

USE ROLE DATAENGINEER;
USE DATABASE INSURANCE_DB;
USE SCHEMA CPST_BRZ;
---------------------------------- CREATE EXTERNAL   STAGE  AND FILE FORMAT  FOR SNOWPIPE -------------------

CREATE OR REPLACE STAGE s3_ext_stage_insurance
  URL='s3://snf-dwh-insurance-sa/'
  CREDENTIALS=(
    --AWS_KEY_ID='xxxxxxxxxxxxxxxxxxxxx'
    --AWS_SECRET_KEY='xxxxxxxxxxxxxxxxxxxxxxxxxx'
  )
  ENCRYPTION=(
    TYPE='AWS_SSE_KMS'
    KMS_KEY_ID='aws/key'
  );


LIST @s3_ext_stage_insurance;

-----------------------------------------  FILE FORMAT -------------------------------------------

CREATE OR REPLACE FILE FORMAT insurance_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ',';




--############################ DATA LOAD INTO CPST_BRZ.INSURANCE_CENTRAL  table ###############################--  

DECLARE file_list STRING;
BEGIN
    -- Get a comma-separated list of filenames that need to be loaded
    SELECT LISTAGG(DISTINCT(METADATA$FILENAME), ', ')
    INTO :file_list
    FROM @s3_ext_stage_insurance/central/ 
    WHERE METADATA$FILENAME NOT IN (SELECT FILE_NAME FROM INSURANCE_METADATA);

    -- Step 3: Check if file_list is populated (optional debugging step)
    -- Return the list for debugging purposes
    --RETURN :file_list;
--END;

EXECUTE IMMEDIATE
        'COPY INTO CPST_BRZ.INSURANCE_CENTRAL 
         FROM @s3_ext_stage_insurance/central/ 
         FILES = (' || :file_list || ') 
         FILE_FORMAT = (FORMAT_NAME = ''insurance_csv_format'') 
         ON_ERROR = ''CONTINUE''';
END;

INSERT INTO INSURANCE_METADATA (FILE_NAME)
SELECT METADATA$FILENAME
FROM @s3_ext_stage_insurance/central/
WHERE METADATA$FILENAME IN (SELECT METADATA$FILENAME FROM @s3_ext_stage_insurance/central());







--############################ DATA LOAD INTO CPST_BRZ.INSURANCE_WEST  table ###############################--  

DECLARE file_list STRING;
BEGIN
    -- Get a comma-separated list of filenames that need to be loaded
    SELECT LISTAGG(DISTINCT(METADATA$FILENAME), ', ')
    INTO :file_list
    FROM @s3_ext_stage_insurance/west/ 
    WHERE METADATA$FILENAME NOT IN (SELECT FILE_NAME FROM INSURANCE_METADATA);

    -- Step 3: Check if file_list is populated (optional debugging step)
    -- Return the list for debugging purposes
    --RETURN :file_list;
--END;

EXECUTE IMMEDIATE
        'COPY INTO CPST_BRZ.INSURANCE_WEST 
         FROM @s3_ext_stage_insurance/west/ 
         FILES = (' || :file_list || ') 
         FILE_FORMAT = (FORMAT_NAME = ''insurance_csv_format'') 
         ON_ERROR = ''CONTINUE''';
END;

INSERT INTO INSURANCE_METADATA (FILE_NAME)
SELECT METADATA$FILENAME
FROM @s3_ext_stage_insurance/west/
WHERE METADATA$FILENAME IN (SELECT METADATA$FILENAME FROM @s3_ext_stage_insurance/west());





--############################ DATA LOAD INTO CPST_BRZ.INSURANCE_EAST  table ###############################--  

DECLARE file_list STRING;
BEGIN
    -- Get a comma-separated list of filenames that need to be loaded
    SELECT LISTAGG(DISTINCT(METADATA$FILENAME), ', ')
    INTO :file_list
    FROM @s3_ext_stage_insurance/east/ 
    WHERE METADATA$FILENAME NOT IN (SELECT FILE_NAME FROM INSURANCE_METADATA);

    -- Step 3: Check if file_list is populated (optional debugging step)
    -- Return the list for debugging purposes
    --RETURN :file_list;
--END;

EXECUTE IMMEDIATE
        'COPY INTO CPST_BRZ.INSURANCE_EAST 
         FROM @s3_ext_stage_insurance/east/ 
         FILES = (' || :file_list || ') 
         FILE_FORMAT = (FORMAT_NAME = ''insurance_csv_format'') 
         ON_ERROR = ''CONTINUE''';
END;

INSERT INTO INSURANCE_METADATA (FILE_NAME)
SELECT METADATA$FILENAME
FROM @s3_ext_stage_insurance/east/
WHERE METADATA$FILENAME IN (SELECT METADATA$FILENAME FROM @s3_ext_stage_insurance/east());






--############################ DATA LOAD INTO CPST_BRZ.INSURANCE_SOUTH  table ###############################--  

DECLARE file_list STRING;
BEGIN
    -- Get a comma-separated list of filenames that need to be loaded
    SELECT LISTAGG(DISTINCT(METADATA$FILENAME), ', ')
    INTO :file_list
    FROM @s3_ext_stage_insurance/south/ 
    WHERE METADATA$FILENAME NOT IN (SELECT FILE_NAME FROM INSURANCE_METADATA);

    -- Step 3: Check if file_list is populated (optional debugging step)
    -- Return the list for debugging purposes
    --RETURN :file_list;
--END;

EXECUTE IMMEDIATE
        'COPY INTO CPST_BRZ.INSURANCE_SOUTH 
         FROM @s3_ext_stage_insurance/south/ 
         FILES = (' || :file_list || ') 
         FILE_FORMAT = (FORMAT_NAME = ''insurance_csv_format'') 
         ON_ERROR = ''CONTINUE''';
END;

INSERT INTO INSURANCE_METADATA (FILE_NAME)
SELECT METADATA$FILENAME
FROM @s3_ext_stage_insurance/south/
WHERE METADATA$FILENAME IN (SELECT METADATA$FILENAME FROM @s3_ext_stage_insurance/south());