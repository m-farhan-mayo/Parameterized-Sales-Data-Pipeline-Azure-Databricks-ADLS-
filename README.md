# Parameterized-Sales-Data-Pipeline-Azure-Databricks-ADLS


This project is an end-to-end, parameterized data pipeline built on the Azure platform. It uses Azure Databricks notebooks (as Python scripts) to process sales-related data from an Azure Data Lake Storage (ADLS) account.

The entire workflow follows the Medallion Architecture (Bronze, Silver, Gold) to progressively refine raw data into high-value, analytics-ready tables. All storage paths, table names, and other configurations are managed in a central Parameters.py file, making the pipeline reusable and easy to manage.

Key Components & Project Recall (File Breakdown)
This is what each file in the project does, organized by the flow of data:

Parameters.py

Purpose: This is the central configuration file for the entire project. It contains all the variables for your Azure Storage Account (container names, directory paths for bronze/silver/gold), database names, table names, and any other reusable values. You built the project this way so you could change the environment in one place without editing every script.

Bronze Layer.py

Purpose (Extract/Load): This is the ingestion script. It reads the raw source data (like CSVs or JSONs for customers, orders, etc.) from the "landing" or "raw" zone of your Azure storage. It then writes this data, unaltered, into the Bronze layer directory.

silver_customers.py / Silver_Orders.py / silver_products.py / silver_regions.py

Purpose (Transform/Clean): These scripts form the Silver layer. Each script reads data from the Bronze Layer tables.

Actions: You applied all your data cleaning logic here:

Handling null values.

Correcting data types (e.g., string to date, string to integer).

Renaming columns for clarity.

Applying business rules (e.g., filtering out test orders).

Possibly joining tables (e.g., joining regions to customers).

The clean, validated, and structured data is then saved as Delta tables in the Silver layer.

gold_customers.py / gold_orders.py / gold_products.py

Purpose (Business Model): These scripts create the final Gold layer for analytics. They read from the clean Silver tables.

Actions: This is where you built your final, aggregated, business-ready tables.

gold_orders: Likely an aggregated Fact Table showing total sales, quantities, etc., probably joined with customer and product keys.

gold_customers / gold_products: Cleaned, de-duplicated Dimension Tables ready to be connected to a BI tool like Power BI.

These tables are saved in the Gold layer of your storage account.
