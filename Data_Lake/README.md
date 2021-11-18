## Introduction
This is a project introducing the concept of DataLake and how to process ETL.
## Purpose
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 
## How to work
This project has the following files:
- Readme.md
    - Introduction of this project
- dl.cfg
    - contain the access_key and secret_key of users' aws
- etl.py
    - read the data from S3
    - process the data with spark and create 5 tables
        - fact table: songplays
        - dimension tables: songs, artists, users, time
    - load it back to S3
- DataLake.ipynb
    - Using the jupyter notebook of EMR to show some analysis of dataset
### Execute
- local to EMR
    - Moving the files from local to EMR
    - Running the job with spark
    ``` bash
    spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2
    ```
- jupyter notebook
    - create cluster and notebook
    - run the program in jupyter notebook provided by EMR
### Analysis
- Proportion of User's gender
    ``` python
        spark.sql("""
        select
            gender,
            count(userId)
        from log_table
        where userId is not null
        group by gender
        """).show()
    ```
    - null: 286  M: 2288  F: 5482
- Time of songplays
    ```python
    songplays_table.createOrReplaceTempView('songsplay')
    spark.sql("""
    select year, month, count(songplay_id) from songsplay group by year, month
    """).show()
    ```
    year: 2018 month: 11 count: 1144

