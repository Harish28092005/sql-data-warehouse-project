/*
====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
====================================================================
*/




create or alter procedure bronze.load_bronze as 
begin
Declare @start_time DATETIME, @end_time DATETIME,@st DATETIME,@end DATETIME;
begin try
print '========================================================';
print 'loading bronze layer';
print '========================================================';

print '--------------------------------------------------------';
print 'loading crm tables';
print '--------------------------------------------------------';
set @st=GETDATE();

set @start_time=GETDATE();
truncate table bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
print '---------------------------------------------------------------'

set @start_time=GETDATE();
truncate table bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
print '---------------------------------------------------------------'

set @start_time=GETDATE();
truncate table bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
print '---------------------------------------------------------------'

print '--------------------------------------------------------';
print 'loading erp tables';
print '--------------------------------------------------------';
set @start_time=GETDATE();

truncate table bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
print '---------------------------------------------------------------'

set @start_time=GETDATE();

truncate table bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
print '---------------------------------------------------------------'

set @start_time=GETDATE();
truncate table bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
from 'C:\Users\janak\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with  (
    firstrow  = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time=GETDATE();
print 'Duration : '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ ' seconds';
print '---------------------------------------------------------------'

set @end=GETDATE();
print 'loading process completed'
print 'Duration of whole batch to load '+CAST(DATEDIFF(second,@st,@end) as NVARCHAR)+' seconds';
end try
begin catch
   print '==========================================='
   print 'error occured during loading bronze layer'
   print 'Error Message'+ERROR_MESSAGE();
   print 'Error Message'+CAST (ERROR_NUMBER() AS NVARCHAR);
   print '==========================================='
end catch
end
