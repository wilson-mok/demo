-- Clean up
-- DROP TABLE Demo2.DimDate;
-- DROP TABLE Demo2.DimStockItem;
-- DROP TABLE Demo2.FactTransaction;
-- DROP SCHEMA Demo2;

-- Create schema
CREATE SCHEMA Demo2;
GO

-- DimDate
--  SCD0
CREATE TABLE Demo2.DimDate (
	skDate			bigint NOT NULL,
	nkDateId		bigint NOT NULL,
	[date]			datetime2 NOT NULL,
	[year]			int NOT NULL,
	[month]			int NOT NULL,
	[dayOfMonth]	int NOT NULL,
	[quarter]		int NOT NULL
);
ALTER TABLE Demo2.DimDate add CONSTRAINT PK_Demo2_DimDate_skDate PRIMARY KEY NONCLUSTERED (skDate) NOT ENFORCED;

-- DimStockItem
--  SCD2
CREATE TABLE Demo2.DimStockItem (
	skStockItem			bigint NOT NULL,
	nkStockItemId		bigint NOT NULL,
	itemDescription		nvarchar(100),
	size				nvarchar(20),
	leadTimeDays		int,
	isChillerRequired	bit,
	idHash				nvarchar(256),
	dataHash			nvarchar(256),
	active				bit NOT NULL,
	effectiveDate		datetime2 NOT NULL,
	expiryDate			datetime2 NOT NULL
);
ALTER TABLE Demo2.DimStockItem add CONSTRAINT PK_Demo2_DimStockItem_skStockItem PRIMARY KEY NONCLUSTERED (skStockItem) NOT ENFORCED;

-- FactTransaction
--  Transactional
CREATE TABLE Demo2.FactTransaction (
	skTransaction		bigint NOT NULL,
	skInvoiceDate		bigint NOT NULL,
	skStockItem			bigint NOT NULL,
	invoiceId			bigint NOT NULL,
	quantity			int NOT NULL,
	unitPrice			decimal(18,2) NOT NULL,
	totalBeforeTax		decimal(18,2) NOT NULL,
	taxRate				decimal(18,3) NOT NULL,
	tax					decimal(18,2) NOT NULL,
	totalAfterTax		decimal(18,2) NOT NULL,
	profit				decimal(18,2) NOT NULL,
	idHash				nvarchar(256),
	dataHash			nvarchar(256)
);
ALTER TABLE Demo2.FactTransaction add CONSTRAINT PK_Demo2_FactTransaction_skTransaction PRIMARY KEY NONCLUSTERED (skTransaction) NOT ENFORCED;

-- NOTE: FOREIGN KEY constraint is not supported in dedicated SQL pool.
