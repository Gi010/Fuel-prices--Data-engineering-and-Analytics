/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.gulf', 'U') IS NOT NULL
    DROP TABLE bronze.gulf;
GO

CREATE TABLE bronze.gulf (
    sales_date        DATE,
    super             FLOAT,
    premium           FLOAT,
    g_force_regular   FLOAT,
    regular           FLOAT
);

GO

IF OBJECT_ID('bronze.brent_oil', 'U') IS NOT NULL
    DROP TABLE bronze.brent_oil;
GO

CREATE TABLE bronze.brent_oil (
    trade_date         DATE,
    price              FLOAT,
    open_price         FLOAT,
    high_price		   FLOAT,
    low_price          FLOAT,
	vol				   NVARCHAR(50),
	change_percentage  FLOAT
);

GO

IF OBJECT_ID('bronze.currency_rates', 'U') IS NOT NULL
    DROP TABLE bronze.currency_rates;
GO

CREATE TABLE bronze.currency_rates (
    trade_date        DATE,
    rate              FLOAT,
);