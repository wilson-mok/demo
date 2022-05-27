USE dataExplorationDb
GO

-- Select from SQL database
SELECT *
FROM nycYellow2019Jan.avgPassengerTripUsage
ORDER BY avgPassengerCount DESC;

-- Select from Lake database
-- This data has been cleaned, so no need to worry about fairAmount check.
SELECT DATENAME("WEEKDAY", [tpepPickupDateTime]) as "dayOfWeek", 
       ROUND(AVG(CAST([passengerCount] AS DECIMAL(10,2))),2) as avgPassengerCount, 
       count(*)/4 as avgTrips
FROM dataExplorationSparkDb.dbo.nyctripyl2019janclean
-- WHERE [fareAmount] BETWEEN 1 AND 1000
GROUP BY DATENAME("WEEKDAY", [tpepPickupDateTime]);