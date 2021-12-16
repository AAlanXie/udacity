## Introduction
This project will introduce the core concept of Apache Airflow.
## Purpose
A music streaming startup, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. They expect to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.
## How to work
This project has the following files:
- Readme.md
    - Introduction of this project
- dags
    - udac_example_dag.py
        - the main program including the whole process of ETL and data-quality check
- plugins
    - helpers
        - contain the sql query of inserting data
    - operators
        - stage_redshift.py
            - load data from S3 to Redshift
        - load_fact.py
            - insert data from stage table to fact table
        - load_dimension.py
            - insert data from stage table to dim table
        - data_quality.py
            - make sure that there are no problem during the ETL process
- create_tables.sql
    - contain the sql query of creating each table
### Execute
- access Airflow web UI
    ```bash
    /opt/airflow/start.sh
    ```


