# Description
This stored procedure exists to return results of column values that have changed from a Temporal table (also known as system-versioned temporal table). It generates dynamic SQL and either executes it or outputs it with the debug parameter.
 
# Supports
2016 and higher version (SQL Server, Azure SQL Database, Azure SQL Managed Instance) is required for the LAG(), OPENJSON, THROW functions.

# Notes
Columns data types Image & RowVersion will be ignored due casting error in comparison operation.

Grouping the output results in the user interface by the ChangeTime column would keep all the column changes that occurred at the same time together.

# Performance
If you do not set the @PrimaryKeyValue parameter, you will scan the entire current table.

An optimal indexing strategy will include a clustered columns store index and / or a B-tree rowstore index on the current table and a clustered columnstore index on the history table for optimal storage size and performance. If you create / use your own history table, it is strongly recommended that you create this type of index consisting of period columns starting with the end of period column, to speed up temporal querying and speed up the queries that are part of the data consistency check. The default history table has a clustered rowstore index created for you based on the period columns (end, start). At a minimum, a nonclustered rowstore index is recommended.
