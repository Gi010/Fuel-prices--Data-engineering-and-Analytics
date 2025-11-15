CREATE OR ALTER PROCEDURE bronze.load_brent_oil AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Brent Oil data into Bronze Layer';
		PRINT '================================================';

		PRINT '>> Truncating Table: bronze.brent_oil';
		TRUNCATE TABLE bronze.brent_oil
		PRINT '>> Inserting Data Into: bronze.brent_oil';
		BULK INSERT bronze.brent_oil
		FROM 'C:\Users\gpaghava\Desktop\Gulf fuel prices analysis\Brent Oil Futures Historical Data.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
)		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING DATA'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END