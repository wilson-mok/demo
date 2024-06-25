USE demoDataSparkDb
GO

-- Select from SQL database
SELECT TOP 10 *
FROM clean_nycTripYellow2019Jan;

-- Select from Lake database
SELECT DATENAME("WEEKDAY", [tpepPickupDateTime]) as dayOfWeek, 
       DATEPART("WEEKDAY", [tpepPickupDateTime]) as dayOfWeekId, 
       ROUND(AVG(CAST([passengerCount] AS DECIMAL(10,2))),2) as avgPassengerCount, 
       count(*)/4 as avgTripPerDay -- On avg, each month has 4 weeks.
FROM clean_nycTripYellow2019Jan
GROUP BY DATENAME("WEEKDAY", [tpepPickupDateTime]), DATEPART("WEEKDAY", [tpepPickupDateTime])
ORDER BY DATEPART("WEEKDAY", [tpepPickupDateTime]);

-- Chart
--  Type: Line
--  Category: dayOfWeek
-- Legend: avgPassengerCount or avgTrips