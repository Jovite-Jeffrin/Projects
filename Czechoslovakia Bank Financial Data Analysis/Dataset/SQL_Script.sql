CREATE WAREHOUSE MY_WH;
use warehouse MY_WH;

drop database bank;

CREATE DATABASE BANK;

USE BANK;


CREATE OR REPLACE TABLE DISTRICT(
    District_Code INT PRIMARY KEY,
    District_Name VARCHAR(100)	,
    Region VARCHAR(100)	,
    No_of_inhabitants	INT,
    No_of_municipalities_with_inhabitants_less_499 INT,
    No_of_municipalities_with_inhabitants_500_btw_1999	INT,
    No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
    No_of_municipalities_with_inhabitants_less_10000 INT,	
    No_of_cities	INT,
    Ratio_of_urban_inhabitants	FLOAT,
    Average_salary	INT,
    No_of_entrepreneurs_per_1000_inhabitants INT,
    No_committed_crime_2017	INT,
    No_committed_crime_2018 INT
) ;

CREATE OR REPLACE TABLE ACCOUNT(
    account_id INT PRIMARY KEY,
    district_id	INT,
    frequency	VARCHAR(40),
    Date DATE ,
    Account_Type VARCHAR(100) ,
    Card_Assigned VARCHAR(20),
    FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE OR REPLACE TABLE ORDER_LIST (
    order_id	INT PRIMARY KEY,
    account_id	INT,
    bank_to	VARCHAR(45),
    account_to	INT,
    amount FLOAT,
    FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE OR REPLACE TABLE LOAN(
    loan_id	INT ,
    account_id	INT,
    Date	DATE,
    amount	INT,
    duration	INT,
    payments	INT,
    status VARCHAR(35),
    FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE OR REPLACE TABLE TRANSACTIONS(
    trans_id INT,	
    account_id	INT,
    Date	DATE,
    Type	VARCHAR(30),
    operation	VARCHAR(40),
    amount	INT,
    balance	FLOAT,
    Purpose	VARCHAR(40),
    bank	VARCHAR(45),
    account_partner_id INT,
    FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE OR REPLACE TABLE CLIENT(
    client_id	INT PRIMARY KEY,
    Sex	CHAR(10),
    Birth_date	DATE,
    district_id INT,
    FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);


CREATE OR REPLACE TABLE DISPOSITION(
    disp_id	INT PRIMARY KEY,
    client_id INT,
    account_id	INT,
    type CHAR(15),
    FOREIGN KEY (account_id) references ACCOUNT(account_id),
    FOREIGN KEY (client_id) references CLIENT(client_id)
);


CREATE OR REPLACE TABLE CARD(
    card_id	INT PRIMARY KEY,
    disp_id	INT,
    type CHAR(10)	,
    issued DATE,
    FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::305486080261:role/czechoslovakiabankrole'
STORAGE_ALLOWED_LOCATIONS =('s3://czechoslovakiabankpro');

DESC integration s3_int;

CREATE OR REPLACE FILE FORMAT CSV 
TYPE = 'CSV' 
COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1;

CREATE OR REPLACE STAGE BANK
URL ='s3://czechoslovakiabankpro'
file_format = CSV
storage_integration = s3_int;

LIST @BANK;

SHOW STAGES;

--CREATE SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE
-- The AUTO_INGEST=true parameter specifies to read 
-- event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.

CREATE OR REPLACE PIPE BANK_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISTRICT" --yourdatabase -- your schema ---your table
FROM '@BANK/District/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ACCOUNT"
FROM '@BANK/Account/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_TXNS AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."TRANSACTIONS"
FROM '@BANK/Trnx/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_DISP AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISPOSITION"
FROM '@BANK/Disp/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_CARD AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CARD"
FROM '@BANK/Card/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_ORDER_LIST AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ORDER_LIST"
FROM '@BANK/Order/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_LOAN AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."LOAN"
FROM '@BANK/Loan/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_CLIENT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CLIENT"
FROM '@BANK/Client/'
FILE_FORMAT = CSV;

SHOW PIPES;

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

select SYSTEM$PIPE_STATUS('BANK_ACCOUNT');

-- select * from table(information_schema.copy_history(table_name=> 'tab_patient',start_time=&gt;
-- dateadd(hours, -1, current_timestamp())));
                          
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
                          

SELECT count(*) FROM DISTRICT;
SELECT count(*) FROM ACCOUNT;
SELECT count(*) FROM TRANSACTIONS;
SELECT count(*) FROM DISPOSITION;
SELECT count(*) FROM CARD;
SELECT count(*) FROM ORDER_LIST;
SELECT count(*) FROM LOAN;
SELECT count(*) FROM CLIENT;

ALTER PIPE BANK_DISTRICT refresh;

ALTER PIPE BANK_ACCOUNT refresh;

ALTER PIPE BANK_TXNS refresh;

ALTER PIPE BANK_DISP refresh;

ALTER PIPE BANK_CARD refresh;

ALTER PIPE BANK_ORDER_LIST refresh;

ALTER PIPE BANK_LOAN refresh;

ALTER PIPE BANK_CLIENT refresh;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM DISTRICT;
SELECT * FROM ACCOUNT;
SELECT * FROM TRANSACTIONS;
SELECT * FROM DISPOSITION;
SELECT * FROM CARD;
SELECT * FROM ORDER_LIST;
SELECT * FROM LOAN;
SELECT * FROM CLIENT;

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT YEAR(DATE), COUNT(*)
FROM TRANSACTIONS
GROUP BY 1
ORDER BY 1;

-- NOW()
SELECT CURRENT_DATE();

SELECT MIN(DATE), MAX(DATE) 
FROM TRANSACTIONS;

SELECT YEAR(CURRENT_DATE) - YEAR(BIRTH_DATE) AS AGE 
FROM CLIENT;

SELECT SUB.AGE, COUNT(*) AS TOTAL_COUNT
FROM (
    SELECT YEAR(CURRENT_DATE) - YEAR(BIRTH_DATE) AS AGE 
    FROM CLIENT
) SUB
GROUP BY 1
ORDER BY 1;

SELECT *, YEAR(CURRENT_DATE) - YEAR(BIRTH_DATE) AS AGE
FROM CLIENT
ORDER BY 3;

select max(date) 
from transactions;

SELECT DATE 
FROM TRANSACTIONS WHERE YEAR(DATE) = 2021;

SELECT * 
FROM TRANSACTIONS
ORDER BY YEAR(date) DESC
LIMIT 20;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- DATA TRANSFORMATION
-- TREATING THE NULL VALUES

-- LIST OF BANKS: Kameron bank, Trinity Bank, Max bank, Bank Creditas, J&T Bank, Moneta Money Bank
-- Fio bank, Hypotecni bank, UniCredit Bank, Czech export bank, Raiffeisen bank, Sky Bank
-- PPF Bank, Air Bank 

-- 1 CZK = 0.04 USD
-- 1 CZK = 3.55 INR

SELECT YEAR(DATE) AS YR, COUNT(*) AS CNT
FROM ACCOUNT
GROUP BY 1; -- 2016 - 2020

SELECT YEAR(ISSUED) AS YR, COUNT(*) AS CNT
FROM CARD
GROUP BY 1; -- 2016 - 2021

SELECT YEAR(DATE) AS YR, COUNT(*) AS CNT
FROM LOAN
GROUP BY 1; -- 2016 - 2021

SELECT YEAR(DATE) AS YR, COUNT(*) AS CNT
FROM TRANSACTIONS
GROUP BY 1;

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2016; 

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2017; 

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2018; 

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2019; -- CLEAN

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2020; 

SELECT count(*)
FROM TRANSACTIONS 
WHERE BANK IS NULL AND YEAR(DATE) = 2021; 

UPDATE TRANSACTIONS 
SET BANK = 'Kameron bank' WHERE BANK IS NULL AND YEAR(DATE) = 2021;

UPDATE TRANSACTIONS 
SET BANK = 'Trinity Bank' WHERE BANK IS NULL AND YEAR(DATE) = 2020;

UPDATE TRANSACTIONS 
SET BANK = 'Moneta Money Bank' WHERE BANK IS NULL AND YEAR(DATE) = 2018;

UPDATE TRANSACTIONS 
SET BANK = 'UniCredit Bank' WHERE BANK IS NULL AND YEAR(DATE) = 2017;

UPDATE TRANSACTIONS 
SET BANK = 'Raiffeisen bank' WHERE BANK IS NULL AND YEAR(DATE) = 2016;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT YEAR(ISSUED) 
FROM CARD; -- 16-21

SELECT DISTINCT YEAR(DATE) 
FROM ACCOUNT; -- 16-20

SELECT DISTINCT YEAR(DATE) 
FROM LOAN; -- 16-21

SELECT DISTINCT YEAR(DATE) 
FROM TRANSACTIONS; -- 16-21

SELECT ACCOUNT_ID, TYPE, SUM(AMOUNT) AS SUM_AMT
FROM TRANSACTIONS;
GROUP BY 1,2; 

SELECT BANK, YEAR(DATE), SUM(AMOUNT)
FROM TRANSACTIONS
WHERE BANK IS NULL
GROUP BY 1,2; 

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- What is the demographic profile of the bank's clients and how does it vary across districts?
ALTER TABLE CLIENT 
ADD AGE INT;  

UPDATE CLIENT 
SET AGE = YEAR(CURRENT_DATE) - YEAR(BIRTH_DATE);

SELECT  
    D.DISTRICT_NAME, D.REGION,
    COUNT(*) AS TOTAL_CLIENT,
    SUM(CASE WHEN C.SEX = 'Male' THEN 1 END) AS MALE_CLIENT,
    SUM(CASE WHEN C.SEX = 'Female' THEN 1 END) AS FEMALE_CLIENT,
    ROUND(AVG(YEAR(CURRENT_DATE()) - YEAR(c.Birth_date)),0) AS AVERAGE_AGE
FROM CLIENT C 
JOIN DISTRICT D ON C.DISTRICT_ID = D.DISTRICT_CODE
GROUP BY 1,2
ORDER BY 2,3;

-- How the banks have performed over the years. Give their detailed analysis year & month-wise.
SELECT YEAR(t.Date) AS Year, MONTH(t.Date) AS Month, 
       AVG(t.balance) AS Average_Monthly_Balance
FROM TRANSACTIONS t
GROUP BY YEAR(t.Date), MONTH(t.Date)
ORDER BY Year, Month;

SELECT YEAR(t.Date) AS Year, MONTH(t.Date) AS Month, 
       COUNT(*) AS Total_Transactions,
       SUM(CASE WHEN t.Type = 'Deposit' THEN t.amount ELSE 0 END) AS Total_Deposits,
       SUM(CASE WHEN t.Type = 'Withdrawal' THEN t.amount ELSE 0 END) AS Total_Withdrawals
FROM TRANSACTIONS t
GROUP BY YEAR(t.Date), MONTH(t.Date), t.Type
ORDER BY Year, Month, t.Type;

SELECT
    YEAR(T.Date) AS Year,
    MONTH(T.Date) AS Month,
    COUNT(DISTINCT L.loan_id) AS Total_Loans,
    SUM(L.amount) AS Total_Loan_Amount,
    AVG(L.amount) AS Average_Loan_Amount,
    COUNT(DISTINCT T.trans_id) AS Total_Transactions,
    SUM(T.amount) AS Total_Transaction_Amount,
    AVG(T.amount) AS Average_Transaction_Amount
FROM
    LOAN L
LEFT JOIN
    TRANSACTIONS T ON L.account_id = T.account_id
GROUP BY
    Year, Month
ORDER BY
    Year, Month;


-- What are the most common types of accounts and how do they differ in terms of usage and profitability?
SELECT
    A.Account_Type,
    COUNT(*) AS Total_Accounts,
    ROUND(AVG(L.amount),2) AS Average_Loan_Amount,
    ROUND(AVG(L.duration),2) AS Average_Loan_Duration,
    AVG(L.payments) AS Average_Loan_Payments,
    COUNT(DISTINCT T.trans_id) AS Total_Transactions,
    ROUND(AVG(T.amount),2) AS Average_Transaction_Amount,
    ROUND(AVG(T.balance),2) AS Average_Transaction_Balance
FROM
    ACCOUNT A
LEFT JOIN
    LOAN L ON A.account_id = L.account_id
LEFT JOIN
    TRANSACTIONS T ON A.account_id = T.account_id
GROUP BY
    A.Account_Type
ORDER BY
    Total_Accounts DESC;


-- Which types of cards are most frequently used by the bank's clients and what is the overall profitability of the credit card business?
SELECT
    C.type AS Card_Type,
    COUNT(*) AS Frequency,
    SUM(L.amount) AS Total_Loan_Amount,
    SUM(L.payments) AS Total_Loan_Payments,
    SUM(L.amount - L.payments) AS Profitability
FROM
    CARD C
JOIN
    DISPOSITION D ON C.disp_id = D.disp_id
JOIN
    ACCOUNT A ON D.account_id = A.account_id
LEFT JOIN
    LOAN L ON A.account_id = L.account_id
GROUP BY
    C.type
ORDER BY
    Frequency DESC;
    

-- What is the bank’s loan portfolio and how does it vary across different purposes and client segments?
SELECT
    L.status AS Loan_Status,
    C.Sex AS Client_Gender,
    COUNT(*) AS Number_of_Loans,
    ROUND(AVG(L.amount),2) AS Average_Loan_Amount,
    ROUND(AVG(L.duration),2) AS Average_Loan_Duration,
    ROUND(AVG(L.payments),2) AS Average_Loan_Payments
FROM
    LOAN L
JOIN
    ACCOUNT A ON L.account_id = A.account_id
JOIN
    CLIENT C ON A.district_id = C.district_id
GROUP BY
    Loan_Status, Client_Gender
ORDER BY
    Loan_Status, Client_Gender;