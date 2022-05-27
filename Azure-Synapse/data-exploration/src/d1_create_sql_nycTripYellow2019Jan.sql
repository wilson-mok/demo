IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'data_dpsyndlsdemodev_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [data_dpsyndlsdemodev_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://data@dpsyndlsdemodev.dfs.core.windows.net' 
	)
GO

CREATE SCHEMA sourceData
GO

CREATE EXTERNAL TABLE sourceData.nycYellow2019Jan (
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
	LOCATION = 'sourceData/nycTripYellow2019Jan/nycTripYellow2019Jan.snappy.parquet',
	DATA_SOURCE = [data_dpsyndlsdemodev_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM sourceData.nycYellow2019Jan
GO