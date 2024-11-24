

-------------------------------------------FACT & DIMENSION TABLES ---------------------------------------




--################################ Date Dimension ###########################


--2002-10-31   --MIN policy START  date         
--2025-12-27   --MAX plicy   END date


USE SCHEMA CPST_SLV;

-- Generate Date series and store in temp table 

SET (start_date, end_date) = (SELECT '2002-01-01', MAX(POLICY_END_DATE) FROM CPST_SLV.INSURANCE_CMB);

CREATE OR REPLACE TEMP TABLE ext_date_temp AS
SELECT 
    $start_date::DATE + VALUE::INT AS date
FROM 
    TABLE(FLATTEN(ARRAY_GENERATE_RANGE(0, DATEDIFF('DAY', $start_date::DATE, $end_date::DATE) + 1)));



CREATE OR REPLACE TEMP TABLE DATEDIM AS
SELECT 
    to_date(date) AS Date,
    TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS DateKey,
    EXTRACT(DAY FROM date) AS Day,
    EXTRACT(MONTH FROM date) AS Month,
    EXTRACT(YEAR FROM date) AS Year,
    CASE 
        WHEN EXTRACT(MONTH FROM date) BETWEEN 1 AND 3 THEN 1
        WHEN EXTRACT(MONTH FROM date) BETWEEN 4 AND 6 THEN 2
        WHEN EXTRACT(MONTH FROM date) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS Quarter,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM date) ORDER BY date) AS Dayofyear,
    CEIL(EXTRACT(DAY FROM date) / 7.0) AS Weekofmonth,
    CEIL(ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM date) ORDER BY date) / 7.0) AS WeekofYear,
    CONCAT('FY-', EXTRACT(YEAR FROM date)) AS Fiscalyear
FROM ext_date_temp;



CREATE OR REPLACE TEMP TABLE DATEDIMOPERATION AS
SELECT 
    Date,
    DateKey,
    Day,
    Month,
    Year,
    Quarter,
    Dayofyear,
    Weekofmonth,
    WeekofYear,
    Fiscalyear,
    ROW_NUMBER() OVER (PARTITION BY WeekofYear ORDER BY Date ASC) AS Dayofweek,
    CONCAT(Year, ' Q', Quarter) AS Quarterofyear,
    ROW_NUMBER() OVER (PARTITION BY CONCAT(Year, ' Q', Quarter) ORDER BY Month) AS Dayofquarter,
    CONCAT('FY-', Year, '-Q', Quarter) AS Fiscalquarter,
    CONCAT(Month, '-FY-', Year) AS Fiscalmonth,
    CONCAT(CEIL(Day / 7.0), '-FY-', Year) AS Fiscalweekmonth,
    CONCAT(CEIL(Dayofyear / 7.0), '-FY-', Year) AS Fiscalweekyear
FROM DATEDIM;


SELECT * FROM DATEDIMOPERATION;

TRUNCATE TABLE CPST_SLV.DIM_DATE ;

INSERT INTO CPST_SLV.DIM_DATE 
SELECT 
    Date,
    DateKey            AS Date_key,
    Day,
    Month,
    Year,
    Day                AS Day_of_month,
    Quarterofyear      AS Quarter_of_year,
    Dayofquarter       AS Day_of_quarter,
    Dayofweek          AS Day_of_week,
    Dayofyear          AS Day_of_year,
    WeekofYear         AS Week_of_Year,
    Weekofmonth        AS Week_of_month,
    Fiscalyear         AS Fiscal_year,
    Fiscalquarter      AS Fiscal_quarter,
    Fiscalmonth        AS Fiscal_month,
    Fiscalweekmonth    AS Fiscal_week_month,
    Fiscalweekyear     AS Fiscal_week_year,
    CASE 
        WHEN Dayofweek IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS WeekdayStatus
FROM DATEDIMOPERATION;




SELECT Date FROM dim_date ORDER BY DateKey;





--#################################### Policy Dimension ######################################--

TRUNCATE TABLE CPST_SLV.DIM_POLICY ;

INSERT INTO CPST_SLV.DIM_POLICY

SELECT 

    DISTINCT(POLICY_TYPE_ID) ,
    POLICY_NAME,
    POLICY_TERM,
    POLICY_TYPE,
    POLICY_TYPE_DESC,
    PREMIUM_AMT
    
FROM CPST_SLV.INSURANCE_CMB;
  
SELECT * FROM CPST_SLV.DIM_POLICY;


--#################################### Customer Dimension ######################################--

--# Implementing CDC for tracking changes and loading the new changes to existing tab;le (Incremental load) and 
--#  manages the SCD Type 2 for adress & Marital status



--CREATE OR REPLACE STREAM CUSTOMER_CHANGES ON TABLE CPST_SLV.INSURANCE_CMB;


MERGE INTO CPST_SLV.DIM_CUSTOMER AS target
USING (
    SELECT 
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_SEGMENT,
        MARITAL_STATUS,
        GENDER,
        DOB,
        STATE_OR_PROVINCE,
        CITY,
        POSTAL_CODE,
        REGION
    FROM CPST_SLV.INSURANCE_CMB
) AS source
ON target.CUSTOMERID = source.CUSTOMER_ID
   AND target.IS_CURRENT = TRUE
WHEN MATCHED AND (
    target.MARITALSTATUS != source.MARITAL_STATUS OR
    target.STATEORPROVINCE != source.STATE_OR_PROVINCE OR
    target.CITY != source.CITY OR
    target.POSTALCODE != source.POSTAL_CODE
) THEN
    -- Close the current record
    UPDATE SET 
        IS_CURRENT = FALSE,
        VALID_TO = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    -- Insert the new record
    INSERT (
        CUSTOMERID,
        CUSTOMERNAME,
        CUSTOMERSEGMENT,
        MARITALSTATUS,
        GENDER,
        DOB,
        STATEORPROVINCE,
        CITY,
        POSTALCODE,
        REGION,
        IS_CURRENT,
        VALID_FROM
    )
    VALUES (
        source.CUSTOMER_ID,
        source.CUSTOMER_NAME,
        source.CUSTOMER_SEGMENT,
        source.MARITAL_STATUS,
        source.GENDER,
        source.DOB,
        source.STATE_OR_PROVINCE,
        source.CITY,
        source.POSTAL_CODE,
        source.REGION,
        TRUE,
        CURRENT_TIMESTAMP
    );




SELECT * FROM CPST_SLV.DIM_CUSTOMER;

SELECT * FROM CPST_SLV.DIM_CUSTOMER WHERE IS_CURRENT = TRUE;




--######################################## INSURANCE FACT  ######################################--



TRUNCATE TABLE CPST_SLV.FACT_INSURANCE ;

INSERT INTO CPST_SLV.FACT_INSURANCE

SELECT  
        TO_NUMBER(TO_CHAR(Policy_Start_Date, 'YYYYMMDD'))         AS StartDateKey,
        TO_NUMBER(TO_CHAR(Policy_End_Date, 'YYYYMMDD'))           AS EndDateKey,
        Policy_Type_Id                                            AS Policy_Type_Id_key,
        Policy_Id,
        Customer_ID                                               AS Customer_ID_key,    
        Premium_Amt,
        TO_DATE(Policy_Start_Date)                                 AS Policy_Start_Date,
        TO_DATE(Policy_End_Date)                                   AS Policy_End_Date,
        TO_DATE(Next_Premium_Date)                                 AS Next_Premium_Date,
        TO_DATE(Actual_Premium_Paid_Date)                          AS Actual_Premium_Paid_Date,   
        TO_DATE(Effective_Start_Date)                              AS Effective_Start_Date,
        TO_DATE(Effective_End_Date)                                AS Effective_End_Date,  
        Total_Policy_Amt,  
        Premium_Amt_Paid_TillDate,
        Region                                                   AS Region_key,
    
    -- Months Delayed calculation
       MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) AS Months_Delayed,

    
    -- Percentage Late Fees based on Region
    CASE 
        WHEN Region = 'Central' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.06
                
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'East' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.075
                
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.025
                ELSE 0
            END
        WHEN Region = 'West' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.095
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'South' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.03
                ELSE 0.025
            END
    END                                                                        AS Percentage_Late_Fees,
    
    -- Late Fees calculation
    
    Premium_Amt * CASE 
        WHEN Region = 'Central' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.06
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'East' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.075
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.025
                ELSE 0
            END
        WHEN Region = 'West' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.095
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'South' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.03
                ELSE 0.025
            END
        END                                                                              AS Late_Fees,
    
    -- Actual Premium Amount to be Paid
    
    Premium_Amt + (Premium_Amt * CASE 
        WHEN Region = 'Central' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.06
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'East' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.075
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.025
                ELSE 0
            END
        WHEN Region = 'West' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.095
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 5 
                THEN 0.05
                ELSE 0
            END
        WHEN Region = 'South' THEN 
            CASE 
                WHEN MONTHS_BETWEEN(TO_DATE(Actual_Premium_Paid_Date), TO_DATE(Next_Premium_Date)) >= 6 
                THEN 0.03
                ELSE 0.025
            END
    END)                                                                AS Actual_Premium_Amt_to_be_Paid,
    
    'Y' AS PolicyStatus,
    
FROM CPST_SLV.INSURANCE_CMB;


