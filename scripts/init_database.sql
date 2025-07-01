/*
==============================
CREATE DATABASE ANDS SCHEMAS
==============================
WARNING:
  Running this script will drop the entire 'DatawareHouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution.
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--CREATE THE 'Datawarehouse' DATABASE
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--CREATE SCHEMAS
CREATE SCHEMA sch_bronze;
GO
PRINT 'sch_bronze CREATED';
GO

CREATE SCHEMA sch_silver;
GO
PRINT 'sch_silver CREATED';
GO

CREATE SCHEMA  sch_gold;
GO
PRINT 'sch_gold CREATED';
GO
