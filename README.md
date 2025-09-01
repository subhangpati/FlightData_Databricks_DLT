# FlightData_Databricks_DLT

Overview

This repository contains all resources and instructions necessary to execute a modern end-to-end data engineering project using Azure Databricks, DBT, Delta Live Tables, Unity Catalog, and PySpark. The project follows a real-world scenario—building a scalable, production-grade big data stack with incremental data ingestion, advanced dimensional modeling, entity automation, and ML-ready architecture, all with the latest features available in 2025 Databricks Free Edition.

## Project Features

Medallion Architecture: Bronze, Silver, and Gold layers using declarative data pipelines.

Incremental Ingestion: PySpark Streaming + Databricks Autoloader for real-time data updates.

Dynamic Notebooks: Parameterized, production-ready notebooks for automation.

Unity Catalog: Secure, governed cloud storage and table management.

Delta Live Tables / Lakeflow Pipelines: Cutting-edge ETL orchestration and transformation logic.

Slowly Changing Dimensions (SCD) Builder: Automated creation and deployment of SCDs and fact tables.

DBT Models: Modular, reusable DBT core layer for analytics and serving endpoints.

## Architecture


<img width="665" height="146" alt="image" src="https://github.com/user-attachments/assets/87c8a84e-2b74-4215-b48e-c42158023258" />


## Project Structure 

```databricks-end-to-end-project/
├── notebooks/
│ ├── setup.ipynb # Initialization and environment setup
│ ├── bronze_layer.ipynb # Bronze Layer: Data ingestion & raw ingestion logic
│ ├── silver_layer.ipynb # Silver Layer: Transformation & cleaning pipelines
│ ├── gold_layer.ipynb # Gold Layer: Star schema, Dimensions, Facts
│ ├── scd_builder.ipynb # Automated Slowly Changing Dimensions logic
│ └── dbt_models/ # DBT models, configurations & SQL files
├── data/
│ ├── bookings/ # Fact bookings CSVs
│ ├── flights/ # Flight dimension CSVs
│ ├── passengers/ # Passenger dimension CSVs
│ └── airports/ # Airport dimension CSVs
└── README.md # Project documentation and instructions
```


## Learning Outcomes 

Learning Outcomes

By completing this project, you will:

    Master Databricks 2025 features like Autoloader, Lakeflow Pipelines, Unity Catalog.

Automate the creation of complex dimensional models and fact tables.

Prepare for data engineering interviews and industry use cases.

Build scalable, cloud-native analytical pipelines using best practices.
