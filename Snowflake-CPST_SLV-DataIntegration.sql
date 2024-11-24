
-----------------------------------------  DATA INTEGRATION SCRIPT -------------------------------------



USE ROLE DATAENGINEER;
USE DATABASE INSURANCE_DB;


TRUNCATE TABLE CPST_SLV.INSURANCE_CMB;

INSERT INTO CPST_SLV.INSURANCE_CMB (
    Customer_ID,
    Customer_Name,
    Customer_Segment,
    Marital_Status,
    Gender,
    DOB,
    Effective_Start_Date,
    Effective_End_Date,
    Policy_Type_Id,
    Policy_Type,
    Policy_Type_Desc,
    Policy_Id,
    Policy_Name,
    Premium_Amt,
    Policy_Term,
    Policy_Start_Date,
    Policy_End_Date,
    Next_Premium_Date,
    Actual_Premium_Paid_Date,
    Country,
    Region,
    State_or_Province,
    City,
    Postal_Code,
    Total_Policy_Amt,
    Premium_Amt_Paid_TillDate
)
SELECT 
    Customer_ID,
    Customer_Name,
    Customer_Segment,
    Marital_Status,
    Gender,
    DOB,
    Effective_Start_Date,
    Effective_End_Date,
    Policy_Type_Id,
    Policy_Type,
    Policy_Type_Desc,
    Policy_Id,
    Policy_Name,
    Premium_Amt,
    Policy_Term,
    Policy_Start_Date,
    Policy_End_Date,
    Next_Premium_Date,
    Actual_Premium_Paid_Date,
    Country,
    Region,
    State_or_Province,
    City,
    Postal_Code,
    Total_Policy_Amt,
    Premium_Amt_Paid_TillDate
FROM (
    SELECT 
        ic.Customer_ID, 
        CONCAT(
            ic.Customer_Title, 
            ' ', 
            ic.Customer_First_Name, 
            ' ', 
            COALESCE(ic.Customer_Middle_Name, ''), 
            ' ', 
            ic.Customer_Last_Name
        ) AS Customer_Name,
        ic.Customer_Segment,  
        ic.Marital_Status,  
        ic.Gender,
        TO_DATE(ic.DOB) AS DOB,
        TO_DATE(ic.Effective_Start_Dt) AS Effective_Start_Date,
        TO_DATE(ic.Effective_End_Dt) AS Effective_End_Date,  
        ic.Policy_Type_Id,  
        ic.Policy_Type,  
        ic.Policy_Type_Desc,  
        ic.Policy_Id,  
        ic.Policy_Name,  
        ic.Premium_Amt,  
        ic.Policy_Term,  
        TO_DATE(ic.Policy_Start_Dt) AS Policy_Start_Date,
        TO_DATE(ic.Policy_End_Dt) AS Policy_End_Date,
        TO_DATE(ic.Next_Premium_Dt) AS Next_Premium_Date,
        TO_DATE(ic.Actual_Premium_Paid_Dt) AS Actual_Premium_Paid_Date,   
        ic.Country,  
        ic.Region,  
        ic.State_or_Province,  
        ic.City,  
        ic.Postal_Code,  
        ic.Total_Policy_Amt,  
        ic.Premium_Amt_Paid_TillDate
    FROM CPST_BRZ.INSURANCE_CENTRAL ic

    UNION  

    SELECT 
        es.Customer_ID,
        es.Customer_Name,
        es.Customer_Segment,  
        es.Marital_Status,  
        es.Gender,
        TO_DATE(es.DOB) AS DOB,
        TO_DATE(es.Effective_Start_Dt) AS Effective_Start_Date,
        TO_DATE(es.Effective_End_Dt) AS Effective_End_Date,  
        es.Policy_Type_Id,  
        es.Policy_Type,  
        es.Policy_Type_Desc,  
        es.Policy_Id,  
        es.Policy_Name,  
        es.Premium_Amt,  
        es.Policy_Term,  
        TO_DATE(es.Policy_Start_Dt) AS Policy_Start_Date,
        TO_DATE(es.Policy_End_Dt) AS Policy_End_Date,
        TO_DATE(es.Next_Premium_Dt) AS Next_Premium_Date,
        TO_DATE(es.Actual_Premium_Paid_Dt) AS Actual_Premium_Paid_Date,   
        es.Country,  
        es.Region,  
        es.State_or_Province,  
        es.City,  
        es.Postal_Code,  
        es.Total_Policy_Amt,  
        es.Premium_Amt_Paid_TillDate
    FROM CPST_BRZ.INSURANCE_EAST es

    UNION  

    SELECT  
        ws.Customer_ID,
        ws.Customer_Name,
        ws.Customer_Segment,  
        ws.Marital_Status,  
        ws.Gender,
        TO_DATE(ws.DOB) AS DOB,
        TO_DATE(ws.Effective_Start_Dt) AS Effective_Start_Date,
        TO_DATE(ws.Effective_End_Dt) AS Effective_End_Date,  
        ws.Policy_Type_Id,  
        ws.Policy_Type,  
        ws.Policy_Type_Desc,  
        ws.Policy_Id,  
        ws.Policy_Name,  
        ws.Premium_Amt,  
        ws.Policy_Term,  
        TO_DATE(ws.Policy_Start_Dt) AS Policy_Start_Date,
        TO_DATE(ws.Policy_End_Dt) AS Policy_End_Date,
        TO_DATE(ws.Next_Premium_Dt) AS Next_Premium_Date,
        TO_DATE(ws.Actual_Premium_Paid_Dt) AS Actual_Premium_Paid_Date,   
        ws.Country,  
        ws.Region,  
        ws.State_or_Province,  
        ws.City,  
        ws.Postal_Code,  
        ws.Total_Policy_Amt,  
        ws.Premium_Amt_Paid_TillDate
    FROM CPST_BRZ.INSURANCE_WEST ws

    UNION  

    SELECT  
        ss.Customer_ID,
        ss.Customer_Name,
        ss.Customer_Segment,  
        ss.Marital_Status,  
        ss.Gender,
        TO_DATE(ss.DOB) AS DOB,
        TO_DATE(ss.Effective_Start_Dt) AS Effective_Start_Date,
        TO_DATE(ss.Effective_End_Dt) AS Effective_End_Date,  
        ss.Policy_Type_Id,  
        ss.Policy_Type,  
        ss.Policy_Type_Desc,  
        ss.Policy_Id,  
        ss.Policy_Name,  
        ss.Premium_Amt,  
        ss.Policy_Term,  
        TO_DATE(ss.Policy_Start_Dt) AS Policy_Start_Date,
        TO_DATE(ss.Policy_End_Dt) AS Policy_End_Date,
        TO_DATE(ss.Next_Premium_Dt) AS Next_Premium_Date,
        TO_DATE(ss.Actual_Premium_Paid_Dt) AS Actual_Premium_Paid_Date,   
        ss.Country,  
        ss.Region,  
        ss.State_or_Province,  
        ss.City,  
        ss.Postal_Code,  
        ss.Total_Policy_Amt,  
        ss.Premium_Amt_Paid_TillDate
    FROM CPST_BRZ.INSURANCE_SOUTH ss
) AS combined_data;





--####################   UPDATE THE POLICY_TYPE_ID 12468 and 2468 to 02468   #####################--

UPDATE CPST_SLV.INSURANCE_CMB 
SET POLICY_TYPE_ID = '02468'
WHERE POLICY_TYPE_ID = '12468' OR POLICY_TYPE_ID = '2468';



--####################   UPDATE THE POLICY_NAME HSB_Termclaim_23579  to HSB_Termclaim_19181 and ---- 
--####################   HSB_WholeLife_22468  to  HSB_WholeLife_19182   #####################

UPDATE CPST_SLV.INSURANCE_CMB 
SET   POLICY_NAME = 'HSB_Termclaim_19181' 
WHERE POLICY_TYPE_ID = '19181' ;

UPDATE CPST_SLV.INSURANCE_CMB 
SET   POLICY_NAME    = 'HSB_WholeLife_19182' 
WHERE POLICY_TYPE_ID = '19182' ;
