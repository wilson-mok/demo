-- Run this script under the Azure SQL Database.

-- Create user and add to db_owner
CREATE USER [dataUser] FROM LOGIN [dataUser];
GO

ALTER ROLE [db_owner] ADD MEMBER [dataUser];
GO

-- Create schema
CREATE SCHEMA [Sales] AUTHORIZATION dbo;
GO

CREATE SCHEMA [Landing] AUTHORIZATION dbo;
GO

CREATE SCHEMA [ExternalData] AUTHORIZATION dbo;
GO

CREATE SCHEMA [Reporting] AUTHORIZATION dbo;
GO

-- Create Landing - Weather Table
DROP TABLE [Landing].[Weather];
GO

CREATE TABLE [Landing].[Weather](
	[sourceId] [nvarchar](max) NULL,
	[guid] [nvarchar](max) NULL,
	[weatherDate] [datetime2](7) NULL,
	[tempUnit] [nvarchar](max) NULL,
	[avgTemp] [int] NULL,
	[windUnit] [nvarchar](max) NULL,
	[avgSpeed] [int] NULL,
	[hasCloud] [bit] NULL,
	[rainUnit] [nvarchar](max) NULL,
	[avgPerHour] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

-- Create ExternalData - Weather Table
DROP TABLE [ExternalData].[Weather];
GO

CREATE TABLE [ExternalData].[Weather] (
  [WeatherId] [Int] Primary Key IDENTITY(1,1),
  [WeatherDate] [Date] NOT NULL,
  [AvgTemperature] [Int] NOT NULL,
  [TempUnit] nvarchar(50) NOT NULL,
  [copiedDate] [Date] NOT NULL,
  [copiedPipelineId] nvarchar(100) NOT NULL
);

-- Create Sales - Tables
CREATE TABLE [Sales].[SalesInvoices](
	[InvoiceID] [int] PRIMARY KEY NOT NULL,
	[CustomerID] [int] NOT NULL,
	[BillToCustomerID] [int] NOT NULL,
	[OrderID] [int] NULL,
	[DeliveryMethodID] [int] NOT NULL,
	[ContactPersonID] [int] NOT NULL,
	[AccountsPersonID] [int] NOT NULL,
	[SalespersonPersonID] [int] NOT NULL,
	[PackedByPersonID] [int] NOT NULL,
	[InvoiceDate] [date] NOT NULL,
	[CustomerPurchaseOrderNumber] [nvarchar](20) NULL,
	[IsCreditNote] [bit] NOT NULL,
	[CreditNoteReason] [nvarchar](max) NULL,
	[Comments] [nvarchar](max) NULL,
	[DeliveryInstructions] [nvarchar](max) NULL,
	[InternalComments] [nvarchar](max) NULL,
	[TotalDryItems] [int] NOT NULL,
	[TotalChillerItems] [int] NOT NULL,
	[DeliveryRun] [nvarchar](5) NULL,
	[RunPosition] [nvarchar](5) NULL,
	[ReturnedDeliveryData] [nvarchar](max) NULL,
	[ConfirmedDeliveryTime] [datetime2] NULL,
	[ConfirmedReceivedBy]  [nvarchar](max) NULL,
	[LastEditedBy] [int] NOT NULL,
	[LastEditedWhen] [datetime2](7) NOT NULL
);
GO

CREATE TABLE [Sales].[SalesInvoiceLines](
	[InvoiceLineID] [int] PRIMARY KEY NOT NULL,
	[InvoiceID] [int] NOT NULL,
	[StockItemID] [int] NOT NULL,
	[Description] [nvarchar](100) NOT NULL,
	[PackageTypeID] [int] NOT NULL,
	[Quantity] [int] NOT NULL,
	[UnitPrice] [decimal](18, 2) NULL,
	[TaxRate] [decimal](18, 3) NOT NULL,
	[TaxAmount] [decimal](18, 2) NOT NULL,
	[LineProfit] [decimal](18, 2) NOT NULL,
	[ExtendedPrice] [decimal](18, 2) NOT NULL,
	[LastEditedBy] [int] NOT NULL,
	[LastEditedWhen] [datetime2](7) NOT NULL,
);
GO

ALTER TABLE [Sales].[SalesInvoiceLines]  WITH CHECK ADD  CONSTRAINT [FK_Sales_SalesInvoiceLines_InvoiceID_Sales_SalesInvoices] FOREIGN KEY([InvoiceID])
REFERENCES [Sales].[SalesInvoices] ([InvoiceID])
GO

-- Create Stored Proc - Weather
CREATE OR ALTER PROCEDURE ExternalData.transformWeather
	@copiedDate date,
	@copiedPipelineId nvarchar(100)
AS
	DELETE FROM [ExternalData].[Weather]
	WHERE [copiedDate] = @copiedDate;

	INSERT INTO [ExternalData].[Weather] ([WeatherDate],[AvgTemperature],[TempUnit],[copiedDate],[copiedPipelineId])
	SELECT [weatherDate], [avgTemp], [tempUnit], @copiedDate, @copiedPipelineId
	FROM [Landing].[Weather]

GO

-- Create SalesProfits table
DROP TABLE [Reporting].[SalesProfits];
GO

CREATE TABLE [Reporting].[SalesProfits] (
	[SalesProfitID] [Int] Primary Key IDENTITY(1,1),
	[InvoiceDate] [date] NOT NULL,
	[DailyTotal] [decimal](18, 2) NOT NULL,
	[DailyProfit] [decimal](18, 2) NOT NULL,
	[WeatherAvgTemp] [int],
	[WeatherUnit] nvarchar(50),
	[copiedDate] [datetime2](7) NOT NULL,
	[copiedPipelineId] [nvarchar](100) NOT NULL
);
GO