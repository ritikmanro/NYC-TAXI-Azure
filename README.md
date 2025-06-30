# NYC-TAXI-Project Azure

## Data Architecture and Storage
- Implement the Medallion Architecture with Bronze (raw data), Silver (transformed data), and Gold (modeled data) layers for industry-level data engineering projects.
- Use Delta Lake for efficient data storage and management, built on top of Parquet file format with a transactional log for versioning and time travel.
- Create a data lake in Azure by setting up a storage account with hierarchical namespace enabled, then create containers for Bronze, Silver, and Gold layers.

## Data Ingestion and Transformation
- Automate data ingestion from APIs using Azure Data Factory with parameterized dynamic pipelines to pull data directly from websites.
- Utilize for each activity in Azure Data Factory to iterate through a range of values and replace variables in file names dynamically.
- Use Databricks with PySpark to transform and analyze data in a scalable and efficient manner.
- Employ PySpark functions like withColumn, split, and to_date for data transformation tasks such as column renaming, splitting, and date conversion.

## Security and Access Control
- Implement managed identities and access control to restrict reading and writing of data in Azure resources.
- Create a service principal in Azure AD and grant it storage blob data contributor role for secure access to the data lake from Databricks.

## Data Processing and Analysis
- Use dbutils.fs.ls in Databricks to list files in a specific location and spark.read.format() to read data into DataFrames.
- Define custom schemas for DataFrames using StructType and StructField objects in PySpark.
- Write data to specific locations using spark.write.format() with appropriate modes like "overwrite" or "append".

## Delta Lake and SQL Operations
- Create Delta tables on top of data in Delta format using PySpark and SQL commands in Databricks.
- Utilize SQL commands on Delta tables for querying and filtering data using spark.sql() function.
- Leverage Delta Lake's versioning and time travel capabilities to commit, rollback, and restore previous versions of data.

## Visualization and Reporting
- Build line charts or pie charts in Databricks notebooks using the built-in visualization features.
- Connect Power BI to Databricks Delta tables using the partner connect feature for reporting and visualization.

Best Practices and Advanced Techniques
- Implement DML (insert, update, delete) and CRUD operations on Delta tables for efficient data management.
- Push transformed data to the Gold layer in Azure Data Lake using appropriate write operations and file formats.
- Implement dynamic data pipelines in Azure Data Factory using parameterized datasets and loops for efficient data processing.
