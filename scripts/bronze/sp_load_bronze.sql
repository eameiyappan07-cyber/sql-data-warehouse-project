/*
==================================================
stored procedure: load Bronze Layer (source->Bronze)
==================================================
Purpose:
  Loads data into Bronze layer from source csv files.
  Uses truncate-and-load approach
To execute:
exec bronze.load_bronze;
*/

CREATE or ALTER PROCEDURE bronze.load_bronze
AS
 BEGIN

		   DECLARE
			@start_time DATETIME,
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME;

			BEGIN TRY
					  SET @batch_start_time = GETDATE();

					  PRINT '========================================';
					  PRINT 'LOADING BRONZE LAYER';
					  PRINT '-----------------------------------------';
					  PRINT '*********LOADING CRM data**********';
					  PRINT '-----------------------------------------';
					  
					  /* 1st Load crm_cust_info*/
					  
					  PRINT '1st Load: Loading Data into crm_cust_info ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.crm_cust_info;
					  PRINT '************Truncate is done; Load started**************';
					  BULK INSERT  bronze.crm_cust_info
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************1st LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********1st load - The END*******************';
					  
					  /* 2nd Load crm_prd_info*/
					  
					  PRINT '2nd Load: Loading Data into crm_prd_info ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************ 2nd load Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.crm_prd_info;
					  PRINT '************2nd load Truncate is done; 2nd load Load started**************';
					  BULK INSERT  bronze.crm_prd_info
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************2nd LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********2nd load - The END*******************';
					  
					  /* 3rd Load crm_sales_details*/
					  
					  PRINT '3rd Load: Loading Data into crm_sales_details ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************ 3rd load Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.crm_sales_details;
					  PRINT '************3rd load Truncate is done; 3rd load Load started**************';
					  BULK INSERT  bronze.crm_sales_details
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************3rd LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********3rd load - The END*******************';
					  
					  
					  /*erp loading code*/
					  PRINT '-----------------------------------------';
					  PRINT '*********LOADING ERP data**********';
					  PRINT '-----------------------------------------';
					  
					  /* 4th Load erp_CUST_AZ12*/
					  
					  PRINT '4th Load: Loading Data into erp_CUST_AZ12 ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.erp_CUST_AZ12;
					  PRINT '************Truncate is done; Load started**************';
					  BULK INSERT  bronze.erp_CUST_AZ12
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************4th LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********1st load - The END*******************';
					  
					  /* 5th Load erp_LOC_A101*/
					  
					  PRINT '5th Load: Loading Data into erp_LOC_A101 ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************ 5th load Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.erp_LOC_A101;
					  PRINT '************5th load Truncate is done; 5th load Load started**************';
					  BULK INSERT  bronze.erp_LOC_A101
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************5th LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********5th load - The END*******************';
					  
					  /* 6th Load erp_PX_CAT_G1V2*/
					  
					  PRINT '6th Load: Loading Data into erp_PX_CAT_G1V2 ';
					  PRINT '========================================';
					  
					  SET @start_time = GETDATE();

					  PRINT '************ 6th load Time capteured and the truncate step will next**************';

					  TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
					  PRINT '************6th load Truncate is done; 6th load Load started**************';
					  BULK INSERT  bronze.erp_PX_CAT_G1V2
					  FROM 'C:\Users\EAM\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
					  WITH
					  (
					  FIRSTROW = 2,
					  FIELDTERMINATOR =',',
					  TABLOCK
					  )
					  SET @end_time = GETDATE();
					  PRINT '************6th LOAD is done; **************';

					  PRINT 'Load Duration is '+ CAST(DATEDIFF(SECOND,@start_time,@end_time )as NVARCHAR) +'seconds';
					  PRINT '***********6th load - The END*******************';
					  
					  SET @batch_end_time = GETDATE();
					  PRINT '************Overall LOAD is done; **************';

					  PRINT 'Overall Load Duration is '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time )as NVARCHAR) +'seconds';
					  PRINT '***********Nandri Vanakkam*******************';
			  
			  END TRY
			  
			  /*Catch Block*/
			  BEGIN CATCH
					PRINT '==========================================';
					PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
					PRINT 'Error Message: ' + ERROR_MESSAGE();
					PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
					PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
					PRINT '==========================================';
			  END CATCH
			/*Catch Block End*/
  END
