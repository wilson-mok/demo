-- Reset Demo
-- DROP DATABASE dataAnalysisDb;
-- GO

-- Create Database
IF NOT EXISTS (SELECT * FROM master.sys.databases WHERE name = N'dataAnalysisDb')
    CREATE DATABASE dataAnalysisDb;
GO

USE dataAnalysisDB;
GO

-- Create Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'demoData') 
    EXEC('CREATE SCHEMA demoData');
GO

-- Parquet is an Open Source Format. 
--   Data is stored by column instead of by row like most RDBS
--   Supports Compression: Snappy, Gzip, LZO. 
--   Support Nested Structure.
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ff_parquet') 
	CREATE EXTERNAL FILE FORMAT [ff_parquet] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

-- Create the connection to the storage account. 
-- Naming convention: ds_[storage account name]_[container]
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'ds_stdemodatalake001_demo') 
	CREATE EXTERNAL DATA SOURCE [ds_stdemodatalake001_demo] 
	WITH (
		LOCATION = 'abfss://demo@stdemodatalake001.dfs.core.windows.net' 
	)
GO

-- A quick look at the data
-- Result: geolocation data is null
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK '/nycTripYellow2019Jan/nycTripYellow2019Jan.snappy.parquet',
		DATA_SOURCE = 'ds_stdemodatalake001_demo', 
		FORMAT = 'PARQUET'
    ) AS [result]
GO


CREATE EXTERNAL TABLE demoData.nycYellow2019Jan (
    [vendorID] nvarchar(4000),
	[tpepPickupDateTime] datetime2(7),
	[tpepDropoffDateTime] datetime2(7),
	[passengerCount] int,
	[tripDistance] float,
	[puLocationId] nvarchar(4000),
	[doLocationId] nvarchar(4000),
	[startLon] float,
	[startLat] float,
	[endLon] float,
	[endLat] float,
	[rateCodeId] int,
	[storeAndFwdFlag] nvarchar(4000),
	[paymentType] nvarchar(4000),
	[fareAmount] float,
	[extra] float,
	[mtaTax] float,
	[improvementSurcharge] nvarchar(4000),
	[tipAmount] float,
	[tollsAmount] float,
	[totalAmount] float,
	[puYear] int,
	[puMonth] int
	)
WITH (
	LOCATION = '/nycTripYellow2019Jan/nycTripYellow2019Jan.snappy.parquet',
    DATA_SOURCE = [ds_stdemodatalake001_demo], 
	FILE_FORMAT = [ff_parquet]
)
GO

-- Note: CETAS (Create External Table As Select) creates a copy of the data. 
CREATE EXTERNAL TABLE demoData.copy_nycYellow2019Jan
WITH (
    LOCATION = '/demoCopyData/',
    DATA_SOURCE = [ds_stdemodatalake001_demo], 
	FILE_FORMAT = [ff_parquet]
)
AS
SELECT *
FROM
    OPENROWSET(
        BULK '/nycTripYellow2019Jan/nycTripYellow2019Jan.snappy.parquet',
		DATA_SOURCE = 'ds_stdemodatalake001_demo', 
		FORMAT = 'PARQUET'
    ) AS [result]
GO


SELECT TOP 100 * FROM demoData.nycYellow2019Jan
GO