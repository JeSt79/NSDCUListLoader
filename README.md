# NSDCUListLoader
CLI-program for processing files with the list of person who under sanctions of National Security Council of Ukraine

It is study project for trying to use the Scala and the AKKA toolkit
Program implements some ETL process for loading data about persons from CVS-file into file with DML for Oracle DB.
In Actor system defined:
* one Actor for exctract data and starting process
* pool of several Actors for data transforming
* one Actor for data loading

## Example
File with test data test_data.csv 
