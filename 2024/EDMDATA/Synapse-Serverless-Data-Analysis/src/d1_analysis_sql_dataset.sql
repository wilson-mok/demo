-- Demo - Serverless SQL - 2/2 script

USE dataAnalysisDB;
GO

-- Quick Look at the data and the schema
-- Result: Geolocation data are often null.
SELECT TOP 100 * 
FROM demoData.nycYellow2019Jan
GO

-- Accuracy: Compare the reference data
-- Result: 7.6M rows
SELECT count(*)
FROM demoData.nycYellow2019Jan;

-- Completeness: Mandatory fields.
-- Result: Ok
SELECT count(*)
FROM demoData.nycYellow2019Jan
WHERE [vendorID] IS NULL
   OR [tpepPickupDateTime] IS NULL OR [tpepDropoffDateTime] IS NULL
   OR [puYear] IS NULL or [puMonth] IS NULL
   OR [passengerCount] IS NULL OR [tripDistance] IS NULL
   OR [fareAmount] IS NULL OR [totalAmount] IS NULL;

-- Validity: Valid format or range
-- 1: Passenger Count
-- Results: 0 passenger? 
SELECT min([passengerCount]), max([passengerCount])
FROM demoData.nycYellow2019Jan; 

-- 2: Check Fare Amount
-- Result: min fare is negative and max is 623k? 
SELECT min([fareAmount]), max([fareAmount])
FROM demoData.nycYellow2019Jan; 

-- Consistency: trip duration
-- 1. Pickup time < DropOff Time. 
-- Result: tripDistance is not 0 and fareAmount > 0
SELECT [vendorID], [tpepPickupDateTime], [tpepDropoffDateTime], [passengerCount], [tripDistance], [fareAmount]
FROM demoData.nycYellow2019Jan
WHERE [tpepPickupDateTime] >= [tpepDropoffDateTime];

-- Uniqueness: 
-- No unique columns 

-- Timeliness: Data is current for our analysis?
--  Result: No error. 
SELECT count(*)
FROM demoData.nycYellow2019Jan
WHERE puYear != 2019
   OR puMonth != 1;


-- Create a clean dataset using CETAS
-- Fix the data types using Reference: https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019
CREATE EXTERNAL TABLE demoData.clean_nycYellow2019Jan
WITH (
    LOCATION = '/clean_nycYellow2019Jan/',
    DATA_SOURCE = [ds_stdemodatalake001_demo], 
	FILE_FORMAT = [ff_parquet]
)
AS
SELECT CAST([vendorID] AS INT) AS [vendorID]
,[tpepPickupDateTime]
,[tpepDropoffDateTime]
,[passengerCount]
,[tripDistance]
,CAST([puLocationId] AS INT) AS [puLocationId]
,CAST([doLocationId] AS INT) AS [doLocationId]
,[rateCodeId]
,CAST([storeAndFwdFlag] AS VARCHAR(1)) AS [storeAndFwdFlag]
,CAST([paymentType] AS INT) AS [paymentType]
,CAST([fareAmount] AS DECIMAL(10,2)) AS [fareAmount]
,CAST([extra] AS DECIMAL(10,2)) AS [extra]
,CAST([mtaTax] AS DECIMAL(10,2)) AS [mtaTax]
,CAST([improvementSurcharge] AS DECIMAL(10,2)) AS [improvementSurcharge]
,CAST([tipAmount] AS DECIMAL(10,2)) AS [tipAmount]
,CAST([tollsAmount] AS DECIMAL(10,2)) AS [tollsAmount]
,CAST([totalAmount] AS DECIMAL(10,2)) AS [totalAmount]
,[puYear]
,[puMonth]
FROM demoData.nycYellow2019Jan
WHERE [passengerCount] > 0
  AND [fareAmount] BETWEEN 1 AND 1000
  AND [tpepPickupDateTime] < [tpepDropoffDateTime];

-- Create View: Average number of passengers and avenge number of trips per Day of week
CREATE VIEW demoData.avgPassengerTrip
AS 
SELECT DATENAME("WEEKDAY", [tpepPickupDateTime]) as dayOfWeek, 
       DATEPART("WEEKDAY", [tpepPickupDateTime]) as dayOfWeekId, 
       ROUND(AVG(CAST([passengerCount] AS DECIMAL(10,2))),2) as avgPassengerCount, 
       count(*)/4 as avgTripPerDay -- On avg, each month has 4 weeks.
FROM demoData.clean_nycYellow2019Jan
GROUP BY DATENAME("WEEKDAY", [tpepPickupDateTime]), DATEPART("WEEKDAY", [tpepPickupDateTime]);

-- Select the avgPassengerTripUsage table
SELECT *
FROM demoData.avgPassengerTrip
ORDER BY dayOfWeekId;

-- Chart
--  Type: Line
--  Category: dayOfWeek
-- Legend: avgPassengerCount or avgTrips
