USE dataExplorationDb
GO

-- Check for nulls
SELECT count(*)
FROM sourceData.nycYellow2019Jan
WHERE [tpepPickupDateTime] IS NULL OR [tpepDropoffDateTime] IS NULL
   OR [passengerCount] IS NULL OR [tripDistance] IS NULL
   OR [fareAmount] IS NULL OR [totalAmount] IS NULL;
-- 0

-- Check min/max
SELECT min([passengerCount]), max([passengerCount])
FROM sourceData.nycYellow2019Jan; 
-- 0, 9

SELECT min([fareAmount]), max([fareAmount])
FROM sourceData.nycYellow2019Jan; 
-- -362, 623,259

-- Average number of passengers and averge number of trips per Day of week
SELECT DATENAME("WEEKDAY", [tpepPickupDateTime]) as "dayOfWeek", 
       ROUND(AVG(CAST([passengerCount] AS DECIMAL(10,2))),2) as avgPassengerCount, 
       count(*)/4 as avgTrips
FROM sourceData.nycYellow2019Jan
WHERE [fareAmount] BETWEEN 1 AND 1000
GROUP BY DATENAME("WEEKDAY", [tpepPickupDateTime]);

-- Creat external table for this query
CREATE SCHEMA nycYellow2019Jan
GO

CREATE EXTERNAL TABLE nycYellow2019Jan.avgPassengerTripUsage
WITH (
   LOCATION = 'analyzeData/nycYellow2019Jan/avgPassengerTripUsage/',
     DATA_SOURCE = data_dpsyndlsdemodev_dfs_core_windows_net,
     FILE_FORMAT = SynapseParquetFormat
 )
 AS 
SELECT DATENAME("WEEKDAY", [tpepPickupDateTime]) as "dayOfWeek", 
       ROUND(AVG(CAST([passengerCount] AS DECIMAL(10,2))),2) as avgPassengerCount, 
       count(*)/4 as avgTrips
FROM sourceData.nycYellow2019Jan
WHERE [fareAmount] BETWEEN 1 AND 1000
GROUP BY DATENAME("WEEKDAY", [tpepPickupDateTime])
GO

-- Select the avgPassengerTripUsage table
SELECT *
FROM nycYellow2019Jan.avgPassengerTripUsage;
