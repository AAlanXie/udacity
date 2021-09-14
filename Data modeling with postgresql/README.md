* 1. [Instructions(How to run the scripts)](#InstructionsHowtorunthescripts)
* 2. [Introduction](#Introduction)
* 3. [Purpose](#Purpose)
* 4. [Database design](#Databasedesign)
* 5. [Analysis](#Analysis)

###  1. <a name='InstructionsHowtorunthescripts'></a>Instructions(How to run the scripts)
- terminal
    ``` bash
    python create_tables.py && python etl.py
    ```
###  2. <a name='Introduction'></a>Introduction
- This is the first project introducing the using ways of postgresql and relational databases modeling methods.
- Through this project, we can be familiar with and use python to solve the json file and import it into the database.
###  3. <a name='Purpose'></a>Purpose 
- analytical goals
    - A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.
###  4. <a name='Databasedesign'></a>Database design
- The most important part of databases is their schema, so we have to do some data exploration before making a design of database schema
- Data exploration
    - 1. Observe each file in this project and determine its usefulness
        - data: our dataset(song/log)
        - sql_queries.py: record query sentence(create/drop/insert)
        - create_table.py: connect the database and create all the tables we used (connect - > drop table - > create table)
        - etl.ipynb: extract data from json files and load it into database (Annotation: For me, I use it to explore each column's datatype of json files)
        - etl.py: reads and processes files from song_data and log_data and loads them into your tables
        - test.ipynb: displays the first few rows of each table to let you check your database
    - 2. Determine the datatype from exploring the json data and fill the datatype into our DDL in sql_queries.py
        - songplays(fact table)
            - songplay_id serial primary key (Never appeared in the data, design it as a self-incrementing id as the primary key)
            - start_time timestamp (appear in log files)
            - user_id int (appear in log files)
            - level varchar (appear in log files)
            - song_id varchar (appear in song files)
            - artist_id varchar (appear in song files)
            - session_id int (appear in log files)
            - location varchar (appear in log files)
            - user_agent text (appear in log files)
        - songs(dimension table)
            - song_id varchar primary key (appear in song files)
            - title text (appear in song files)
            - artist_id varchar (appear in song files)
            - year int (appear in song files)
            - duration numeric (appear in song files)
        - artists(dimension table)
            - artist_id varchar primary key (appear in song files)
            - name varchar (appear in song files)
            - location varchar (appear in song files)
            - latitude numeric (appear in song files)
            - longitude numeric (appear in song files)
        - users(dimension table)
            - user_id int primary key (appear in log files)
            - first_name varchar (appear in log files)
            - last_name varchar (appear in log files)
            - gender varchar (appear in log files)
            - level varchar (appear in log files)
        - time(dimension table)
            - start_time timestamp primary key (convert the column ts{eg: 1543537327796})
            - hour int (Separated from the timestamp)
            - day int (Separated from the timestamp)
            - week int (Separated from the timestamp)
            - month int (Separated from the timestamp)
            - year int (Separated from the timestamp)
            - weekday int (Separated from the timestamp)
    - 3. Use "on conflict" to solve the problem of primary key insertion conflict
        - eg: insert into artists (artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s) on conflict (artist_id) do nothing
- Data modeling
    - star schema
- ETL pipline
    - Use pandas to preprocess the data and write it into the database
###  5. <a name='Analysis'></a>Analysis
- Gender
    ``` sql
    select gender, count(1) from (select user_id from songplays) as a join (select user_id, gender from users) as b on a.user_id = b.user_id group by gender
    ```
    - M:1933 F:4887
    - conclusion: Women tend to listen to songs more than men
- Time
    ``` sql
    select hour, count(1) from (select start_time from songplays) as a join (select start_time, hour from time) as b on a.start_time = b.start_time group by hour
    ```
    - 0: 6813
    - conclusion: The data set may be incomplete