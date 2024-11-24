
------------------------------  ACCOUNTADMIN & SYSADMIN SCRIPTS-----------------------

---USER & ROLE CREATION AND GRANTS

USE DATABASE INSURANCE_DB;

GRANT USAGE ON DATABASE insurance_db TO ROLE sysadmin

USE ROLE ACCOUNTADMIN;


-- CUSTOME ROLE DATAENGINEER CONFIGS 

CREATE ROLE "DATAENGINEER";

-- Grant permissions on db
GRANT USAGE ON DATABASE insurance_db TO ROLE DATAENGINEER ;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATAENGINEER;


-- Grant Permissions to Create Stages, Pipes, and Integrations CPST_BRZ ,SLV & DWH objects 
GRANT CREATE STAGE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;
GRANT CREATE STAGE ON SCHEMA INSURANCE_DB.CPST_SLV TO ROLE DATAENGINEER;
GRANT CREATE STAGE ON SCHEMA INSURANCE_DB.CPST_DWH TO ROLE DATAENGINEER;

GRANT CREATE FILE FORMAT ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;
GRANT USAGE, CREATE STAGE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;


GRANT CREATE STREAM ON SCHEMA INSURANCE_DB.CPST_SLV TO ROLE DATAENGINEER;

GRANT CREATE PROCEDURE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;

GRANT USAGE , CREATE PIPE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;
GRANT CREATE PIPE ON SCHEMA INSURANCE_DB.CPST_SLV TO ROLE DATAENGINEER;
GRANT CREATE PIPE ON SCHEMA INSURANCE_DB.CPST_DWH TO ROLE DATAENGINEER;


-- Grant Read and Write Access to Database Objects (Tables, Views, etc.)
GRANT TRUNCATE, SELECT, INSERT, UPDATE, DELETE 
ON ALL TABLES IN SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;
GRANT CREATE TABLE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;

GRANT TRUNCATE, SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA INSURANCE_DB.CPST_SLV TO ROLE DATAENGINEER;
GRANT TRUNCATE ,SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA INSURANCE_DB.CPST_DWH TO ROLE DATAENGINEER;

-- Grant Usage Access on Database and Schema
GRANT USAGE ON DATABASE INSURANCE_DB TO ROLE DATAENGINEER;
GRANT USAGE ON SCHEMA INSURANCE_DB.CPST_BRZ TO ROLE DATAENGINEER;
GRANT USAGE ON SCHEMA INSURANCE_DB.CPST_SLV TO ROLE DATAENGINEER;
GRANT USAGE ON SCHEMA INSURANCE_DB.CPST_DWH TO ROLE DATAENGINEER;



-- Creating user and assinging role 

CREATE USER Shubh
PASSWORD = 'xyz'
MUST_CHANGE_PASSWORD = TRUE
DEFAULT_ROLE = "DATAENGINEER"
DEFAULT_WAREHOUSE = COMPUTE_WH
EMAIL = 'shubham-chatterjee@hcltech.com';


GRANT ROLE "DATAENGINEER" TO USER Shubh;

SHOW ROLES LIKE 'DATAENGINEER';






-- CUSTOM ROLE BI-ANALYST CONFIGS 

CREATE ROLE "BIANALYST";

GRANT USAGE ON DATABASE INSURANCE_DB TO ROLE BIANALYST;
GRANT USAGE ON SCHEMA INSURANCE_DB.CPST_DWH TO ROLE BIANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA INSURANCE_DB.CPST_DWH TO ROLE BIANALYST;



CREATE USER Shubham
PASSWORD = 'xyz'
MUST_CHANGE_PASSWORD = TRUE
DEFAULT_ROLE = "BIANALYST"
DEFAULT_WAREHOUSE = COMPUTE_WH
EMAIL = 'shubham26may2000@gmail.com';

GRANT ROLE "BIANALYST" TO USER ShubhamINSURANCE_DB

SHOW ROLES LIKE 'BIANALYST';






----------------------------------------------- SYSADMIN ASSEST - SCHEMA SCRIPT----------------------------------------------------

USE ROLE SYSADMIN;


-- Create a Virtual Warehouse 

CREATE WAREHOUSE COMPUTE_WH
WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED =  FALSE
    COMMENT = 'Compute for the DWH loads'; 




-- Create the Database 

CREATE DATABASE  INSURANCE_DB
DATA_RETENTION_TIME_IN_DAYS = 7;

USE DATABASE  INSURANCE_DB;

CREATE SCHEMA CPST_BRZ;

CREATE SCHEMA CPST_SLV;

CREATE SCHEMA CPST_DWH;



--################################# METADATA LOAD TABLE ###################################

CREATE OR REPLACE TABLE CPST_BRZ.INSURANCE_METADATA (
    FILE_NAME                        STRING           PRIMARY KEY,    
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);




--################################ CREATE RAW LAYER TABLE  ################################## 

USE SCHEMA CPST_BRZ;


CREATE OR REPLACE TABLE INSURANCE_EAST (
    Customer_ID INT,
    Customer_Name STRING,
    Customer_Segment STRING,
    Marital_Status STRING,
    Gender STRING,
    DOB DATE,
    Effective_Start_Dt DATE,
    Effective_End_Dt DATE,
    Policy_Type_Id INT,
    Policy_Type STRING,
    Policy_Type_Desc STRING,
    Policy_Id STRING,
    Policy_Name STRING,
    Premium_Amt DECIMAL(10, 2),
    Policy_Term STRING,
    Policy_Start_Dt DATE,
    Policy_End_Dt DATE,
    Next_Premium_Dt DATE,
    Actual_Premium_Paid_Dt DATE,
    Country STRING,
    Region STRING,
    State_or_Province STRING,
    City STRING,
    Postal_Code STRING,
    Total_Policy_Amt DECIMAL(15, 2),
    Premium_Amt_Paid_TillDate DECIMAL(15, 2)
);




CREATE OR REPLACE TABLE INSURANCE_WEST (
    Customer_ID INT,
    Customer_Name STRING,
    Customer_Segment STRING,
    Marital_Status STRING,
    Gender STRING,
    DOB DATE,
    Effective_Start_Dt DATE,
    Effective_End_Dt DATE,
    Policy_Type_Id INT,
    Policy_Type STRING,
    Policy_Type_Desc STRING,
    Policy_Id STRING,
    Policy_Name STRING,
    Premium_Amt DECIMAL(10, 2),
    Policy_Term STRING,
    Policy_Start_Dt DATE,
    Policy_End_Dt DATE,
    Next_Premium_Dt DATE,
    Actual_Premium_Paid_Dt DATE,
    Country STRING,
    Region STRING,
    State_or_Province STRING,
    City STRING,
    Postal_Code STRING,
    Total_Policy_Amt DECIMAL(15, 2),
    Premium_Amt_Paid_TillDate DECIMAL(15, 2)
);



CREATE OR REPLACE TABLE INSURANCE_SOUTH (
    Customer_ID INT,
    Customer_Name STRING,
    Customer_Segment STRING,
    Marital_Status STRING,
    Gender STRING,
    DOB DATE,
    Effective_Start_Dt DATE,
    Effective_End_Dt DATE,
    Policy_Type_Id INT,
    Policy_Type STRING,
    Policy_Type_Desc STRING,
    Policy_Id STRING,
    Policy_Name STRING,
    Premium_Amt DECIMAL(10, 2),
    Policy_Term STRING,
    Policy_Start_Dt DATE,
    Policy_End_Dt DATE,
    Next_Premium_Dt DATE,
    Actual_Premium_Paid_Dt DATE,
    Country STRING,
    Region STRING,
    State_or_Province STRING,
    City STRING,
    Postal_Code STRING,
    Total_Policy_Amt DECIMAL(15, 2),
    Premium_Amt_Paid_TillDate DECIMAL(15, 2)
);



CREATE OR REPLACE TABLE INSURANCE_CENTRAL (
    Customer_ID INT,
    Customer_Title STRING,
    Customer_First_Name STRING,
    Customer_Middle_Name STRING,
    Customer_Last_Name STRING,
    Customer_Segment STRING,
    Marital_Status STRING,
    Gender STRING,
    DOB DATE,
    Effective_Start_Dt DATE,
    Effective_End_Dt DATE,
    Policy_Type_Id INT,
    Policy_Type STRING,
    Policy_Type_Desc STRING,
    Policy_Id STRING,
    Policy_Name STRING,
    Premium_Amt DECIMAL(10, 2),
    Policy_Term STRING,
    Policy_Start_Dt DATE,
    Policy_End_Dt DATE,
    Next_Premium_Dt DATE,
    Actual_Premium_Paid_Dt DATE,
    Country STRING,
    Region STRING,
    State_or_Province STRING,
    City STRING,
    Postal_Code STRING,
    Total_Policy_Amt DECIMAL(15, 2),
    Premium_Amt_Paid_TillDate DECIMAL(15, 2)
);


USE SCHEMA CPST_SLV;

CREATE OR REPLACE TABLE CPST_SLV.INSURANCE_CMB (
    Customer_ID STRING,
    Customer_Name STRING,
    Customer_Segment STRING,
    Marital_Status STRING,
    Gender STRING,
    DOB DATE,
    Effective_Start_Date DATE,
    Effective_End_Date DATE,
    Policy_Type_Id STRING,
    Policy_Type STRING,
    Policy_Type_Desc STRING,
    Policy_Id STRING,
    Policy_Name STRING,
    Premium_Amt DECIMAL(18, 2),
    Policy_Term STRING,
    Policy_Start_Date DATE,
    Policy_End_Date DATE,
    Next_Premium_Date DATE,
    Actual_Premium_Paid_Date DATE,
    Country STRING,
    Region STRING,
    State_or_Province STRING,
    City STRING,
    Postal_Code STRING,
    Total_Policy_Amt DECIMAL(18, 2),
    Premium_Amt_Paid_TillDate DECIMAL(18, 2)
);


ALTER TABLE CPST_SLV.INSURANCE_CMB SET CHANGE_TRACKING = TRUE;


-- ####################################### SLV DIMENSION TABLES ##############################_-


CREATE OR REPLACE TABLE CPST_SLV.DIM_DATE (

    Date                 DATE,
    DateKey              INT        PRIMARY KEY,
    Day                  INT,
    Month                INT,
    Year                 INT,
    DayOfMonth           INT,
    QuarterOfYear        STRING,
    DayOfQuarter         INT,
    DayOfWeek            INT,
    DayOfYear            INT,
    WeekOfYear           INT,
    WeekOfMonth          INT,
    FiscalYear           STRING,
    FiscalQuarter        STRING,
    FiscalMonth          STRING,
    FiscalWeekMonth      STRING,
    FiscalWeekYear       STRING,
    WeekDayStatus        STRING
);






CREATE OR REPLACE TABLE CPST_SLV.DIM_POLICY (

    PolicyTypeID         VARCHAR(20)                     PRIMARY KEY,
    Policy_Name          VARCHAR(100),
    PolicyTerm           VARCHAR(100),
    PolicyType           VARCHAR(50),
    PolicyTypeDesc       VARCHAR(255),
    PremiumAmount        DECIMAL(18,2)
);





CREATE OR REPLACE TABLE CPST_SLV.DIM_CUSTOMER (

    CustomerID                   INT              PRIMARY KEY,
    CustomerName                 VARCHAR(100),
    CustomerSegment              VARCHAR(50),
    MaritalStatus                VARCHAR(20),
    Gender                       VARCHAR(10),
    DOB                          DATE,
    StateOrProvince              VARCHAR(50),
    City                         VARCHAR(50),
    PostalCode                   VARCHAR(20),
    Region                       VARCHAR(50),
    IS_CURRENT                   BOOLEAN DEFAULT TRUE,
    VALID_FROM TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    VALID_TO                     TIMESTAMP
);








CREATE OR REPLACE TABLE Fact_Insurance (

    StartDateKey                         INT           REFERENCES DIM_DATE(DateKey),       
    EndDateKey                           INT           REFERENCES DIM_DATE(DateKey),
    PolicyTypeIDKey                      VARCHAR(20)   REFERENCES DIM_POLICY(PolicyTypeID),
    PolicyID                             STRING,
    CustomerKey                          INT           REFERENCES DIM_CUSTOMER(CustomerID),
    PremiumAmt                           NUMBER(18, 2),
    PolicyStartDate                      DATE,
    PolicyEndDate                        DATE,
    NextPremiumDate                      DATE,
    ActualPremiumPaidDate                DATE,
    EffectiveStartDate                   DATE,
    EffectiveEndDate                     DATE,
    TotalPolicyAmt                       NUMBER(18, 2),
    PremiumAmtPaidTillDate               NUMBER(18, 2),
    RegionKey                            VARCHAR(50),
    MonthsDelayed                        NUMBER(10, 2),
    PercentageLateFees                   NUMBER(10, 4),
    LateFees                             NUMBER(15, 2),
    ActualPremiumAmountToBePaid          NUMBER(15, 2),
    PolicyStatus                         CHAR(1)
    
);



