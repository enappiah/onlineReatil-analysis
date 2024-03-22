-- CREATOR: ENOCH N. APPIAH
-- DATE: 17 MAR 24

-- CREATE DATABASE PortfolioDB;
USE PortfolioDB;

-- Cleaning Data
-- Total Records = 1067371
-- 243007 Records have no customerID --> Already cleaned in python
-- 824364 Records have customerID
-- 18744 Records are cancelled invoices ---> add tag column
-- 26483 Duplicate records ---> Drop duplicate records
-- Some StockCode have letter labeling such as POSTAGE, MANUAL(M)??
-- Total cleaned records : 776929

SELECT * FROM onlineretail;

-- Totale records with customerID (824364)
SELECT count(*) FROM onlineretail;

-- cancelled invoices: Invoice records containing 'C'
SELECT count(*) FROM onlineretail
WHERE Invoice like '%C%';

-- StockCode:A 5-digit integral number uniquely assigned to each distinct product. Check
SELECT *,
CASE WHEN StockCode REGEXP '^[A-Za-z]+$' THEN 0 ELSE 1 END AS stockcode_flag
FROM onlineretail;

-- Temporary table for duplicate check : 26483 Duplicate records
DROP TEMPORARY TABLE IF EXISTS duplicates_check;
CREATE TEMPORARY TABLE duplicates_check AS
SELECT *,
		ROW_NUMBER() OVER (PARTITION BY Invoice, StockCode, Quantity, Price, CustomerID ORDER BY InvoiceDate) AS dup_flag,
        CASE WHEN StockCode REGEXP '^[A-Za-z]+$' THEN 0 ELSE 1 END AS stockcode_flag
FROM onlineretail;
 
 -- drop duplicates and add invoice cancelled flag column
 -- Temporary table for duplicate check : 26483 Duplicate records
DROP TEMPORARY TABLE IF EXISTS duplicates_dropped;
CREATE TEMPORARY TABLE duplicates_dropped AS 
SELECT *,
		CASE WHEN Invoice like '%C%' THEN 1 ELSE 0 END AS Invoice_cancelled
FROM duplicates_check
WHERE dup_flag =1;    

-- After duplicates removed:797881 records
SELECT count(*) FROM duplicates_dropped;

-- Clean Data: for records with no cancelled invoice, there are no negative quantity and Price. This great!
DROP TEMPORARY TABLE IF EXISTS data_clean;
CREATE TEMPORARY TABLE data_clean AS
SELECT * FROM duplicates_dropped
WHERE Invoice_cancelled=0 ;

-- We check if same customerID in different country. Could they be travellers?
DROP TEMPORARY TABLE IF EXISTS customer_country;
CREATE TEMPORARY TABLE customer_country AS
SELECT CustomerID,
    COUNT(DISTINCT Country) AS distinct_country_count
FROM data_clean
GROUP BY CustomerID
HAVING COUNT(DISTINCT Country)>1;

-- check one of this situation: same customer in diff country
SELECT *
FROM data_clean
WHERE CustomerID=12422;


-- ADD date variables: Date Month, Week; Time (Hr).Also add TotalPrice (Quantity*Price)
DROP TEMPORARY TABLE IF EXISTS data_clean_agg;
CREATE TEMPORARY TABLE data_clean_agg AS
	SELECT *,
		ROUND(Quantity*Price,2) AS TotalPrice,
		DATE(InvoiceDate)AS Invoice_date,
        YEAR(InvoiceDate)AS Invoice_Year,
		MONTHNAME(InvoiceDate)AS Invoice_month,
		WEEK(InvoiceDate)AS Invoice_week,
		DAYNAME(InvoiceDate)AS Invoice_day,
		HOUR(InvoiceDate)AS Invoice_hour
	FROM data_clean;

SELECT * FROM data_clean_agg;


-- Save records with invoices not cancelled, stockcode not letters only for Tableau
-- Total cleaned records : 776929
DROP TEMPORARY TABLE IF EXISTS onlineretail_cleaned_0;
CREATE TEMPORARY TABLE onlineretail_cleaned_0 AS
	SELECT *
	FROM data_clean_agg
	WHERE Invoice_cancelled = 0 AND stockcode_flag = 1
	ORDER BY InvoiceDate;
    
SELECT * FROM onlineretail_cleaned_0;

-- Drop unwanted columns
ALTER TABLE onlineretail_cleaned_0
DROP COLUMN dup_flag,
DROP COLUMN stockcode_flag,
DROP COLUMN Invoice_cancelled;

SELECT * FROM onlineretail_cleaned_0;

-- ADD columns for cohort analysis (Retention time for each customer)
-- Create Cohort table (first time a customer made a purchase)
DROP TEMPORARY TABLE IF EXISTS cohort;
CREATE TEMPORARY TABLE cohort AS
SELECT
	CustomerID,
	MIN(InvoiceDate) AS first_purchase_date,
	YEAR(MIN(InvoiceDate)) AS cohort_year,
	MONTH(MIN(InvoiceDate)) AS cohort_month,
	STR_TO_DATE(CONCAT(YEAR(MIN(InvoiceDate)), '-', MONTH(MIN(InvoiceDate)), '-01'),'%Y-%m-%d') AS cohort_date
FROM onlineretail_cleaned_0
GROUP BY CustomerID;

SELECT * FROM cohort;

-- Create cohort index and save final table to PortfolioDB
DROP TABLE IF EXISTS PortfolioDB.onlineretail_cleaned;
CREATE TABLE PortfolioDB.onlineretail_cleaned AS
SELECT mmm.*,
		(year_diff * 12 + month_diff + 1) AS cohort_index
FROM
	(SELECT mm.*,
			(YEAR(InvoiceDate) - cohort_year) AS year_diff,
			(MONTH(InvoiceDate) - cohort_month) AS month_diff
	FROM
		(SELECT m.*,
				c.first_purchase_date,
				c.cohort_year,
				c.cohort_month,
				c.cohort_date
		FROM onlineretail_cleaned_0 m
		JOIN cohort c 
		ON m.CustomerID = c.CustomerID) mm
		) mmm;

SELECT * FROM onlineretail_cleaned;

-- =================SOME ANALYSIS ====================
-- How much Revenue generated per Year
SELECT Invoice_Year,
	Count(Invoice) As 'Total Orders',
	SUM(TotalPrice) AS 'Revenue(£)'
FROM onlineretail_cleaned
GROUP BY Invoice_Year;

-- How much Revenue generated per MONTH FOR EACH Year
SELECT Invoice_Year, Invoice_month,
	Count(Invoice) As 'Total Orders',
	SUM(TotalPrice) AS 'Revenue(£)'
FROM onlineretail_cleaned
GROUP BY Invoice_Year, Invoice_month;

-- Order count by hour of the day
SELECT Invoice_hour AS 'Hour of the Day',
		Count(Invoice) As 'Total Order'
FROM onlineretail_cleaned
GROUP BY Invoice_hour
ORDER BY Invoice_hour;

-- Customer retention analysis: Cohort Analysis
SELECT distinct
	cohort_index
FROM onlineretail_cleaned
GROUP BY cohort_index;

-- Create a temporary table to store the pivot result
-- CREATE TEMPORARY TABLE cohort_pivot AS
DROP TABLE IF EXISTS PortfolioDB.Cohort_retention;
CREATE TABLE PortfolioDB.Cohort_retention AS
SELECT
  Cohort_Date,
  COUNT(CASE WHEN cohort_index = 1 THEN CustomerID END) AS cohort_1,
  COUNT(CASE WHEN cohort_index = 2 THEN CustomerID END) AS cohort_2,
  COUNT(CASE WHEN cohort_index = 3 THEN CustomerID END) AS cohort_3,
  COUNT(CASE WHEN cohort_index = 4 THEN CustomerID END) AS cohort_4,
  COUNT(CASE WHEN cohort_index = 5 THEN CustomerID END) AS cohort_5,
  COUNT(CASE WHEN cohort_index = 6 THEN CustomerID END) AS cohort_6,
  COUNT(CASE WHEN cohort_index = 7 THEN CustomerID END) AS cohort_7,
  COUNT(CASE WHEN cohort_index = 8 THEN CustomerID END) AS cohort_8,
  COUNT(CASE WHEN cohort_index = 9 THEN CustomerID END) AS cohort_9,
  COUNT(CASE WHEN cohort_index = 10 THEN CustomerID END) AS cohort_10,
  COUNT(CASE WHEN cohort_index = 11 THEN CustomerID END) AS cohort_11,
  COUNT(CASE WHEN cohort_index = 12 THEN CustomerID END) AS cohort_12,
  COUNT(CASE WHEN cohort_index = 13 THEN CustomerID END) AS cohort_13,
  COUNT(CASE WHEN cohort_index = 14 THEN CustomerID END) AS cohort_14,
  COUNT(CASE WHEN cohort_index = 15 THEN CustomerID END) AS cohort_15,
  COUNT(CASE WHEN cohort_index = 16 THEN CustomerID END) AS cohort_16,
  COUNT(CASE WHEN cohort_index = 17 THEN CustomerID END) AS cohort_17,
  COUNT(CASE WHEN cohort_index = 18 THEN CustomerID END) AS cohort_18,
  COUNT(CASE WHEN cohort_index = 19 THEN CustomerID END) AS cohort_19,
  COUNT(CASE WHEN cohort_index = 20 THEN CustomerID END) AS cohort_20,
  COUNT(CASE WHEN cohort_index = 21 THEN CustomerID END) AS cohort_21,
  COUNT(CASE WHEN cohort_index = 22 THEN CustomerID END) AS cohort_22,
  COUNT(CASE WHEN cohort_index = 23 THEN CustomerID END) AS cohort_23,
  COUNT(CASE WHEN cohort_index = 24 THEN CustomerID END) AS cohort_24,
  COUNT(CASE WHEN cohort_index = 25 THEN CustomerID END) AS cohort_25
FROM onlineretail_cleaned
GROUP BY Cohort_Date;

SELECT * FROM Cohort_retention;
