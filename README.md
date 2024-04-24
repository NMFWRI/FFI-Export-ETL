Author: Corey Beinhart, Data Manager, NMFWRI
Contact: corey@nmhu.edu

# Description

This is an ETL process for migrating data collected in FEAT/FIREMON Integrated (FFI) into our internal databases.
FFI exports data as XML documents containing all tables and data from its own internal database; this program
extracts those XML documents into a collection of pandas DataFrames, transforms the data into the formats of our 
tables and performs certain computations on the data, and then loads the data into our databases. This program
generates MERGE INTO SQL commands that insert non-duplicates based on the primary key(s) of the table.

# Use

In order to use this, create a config.ini file in the base directory for this code. Since FFI servers are built on top 
of SQL Server, the template for a SQL server connection is as below:

[SQLServer]
type = SQLServer
driver = mssql+pyodbc
server = 
database = 
user = 
password = 

where all the blank fields should be filled out as the is relevant by the user, since it will change based on whether 
the connection is local or remote.

Then, in the xml_to_rdb.py file, change the 'path' variable to the directory where your data set is.

# Future
In the near future, I would like to build a simple GUI for this so no one has to look at the code.

If you have any questions or requests, please reach out to me.
