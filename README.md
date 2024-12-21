# Taxi Data Processing Application

## Overview
This C# console application processes a CSV file containing taxi trip data, cleans it by removing duplicates, and inserts the cleaned data into a SQL Server database. Additionally, it handles parsing of specific data fields and writes duplicate records into a separate file.

## Features
- **Read CSV in Batches:** The application reads data from a CSV file in batches to efficiently handle large files.
- **Remove Duplicates:** Duplicate records are identified and saved to a separate file while the unique records are passed for further processing.
- **Parse Data Fields:** The application ensures that date fields are correctly parsed into UTC time and converts the `store_and_fwd_flag` to a human-readable format ("Yes"/"No").
- **Bulk Insert into SQL Server:** The clean data is bulk-inserted into a SQL Server database, reducing the time required to insert large datasets.

**Number of rows in table after running the program** is 60 000
