import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    :description 
        Load the songs data from S3 and reload it back to S3 after processing it by extract the songs and artists table.
    :param
        spark: sparksession
        input_data: the location where we load the songs_data
        ouput_data: the location where result data will be stored
    :return: None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
    select 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    from songs_table
    where song_id is not null
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data+'songs_table/')
    
    # extract columns to create artists table
    artists_table = spark.sql("""
    select 
        distinct artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from songs_table
    where artist_id is not null
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    :description 
        Load the log data from S3 and reload it back to S3 after processing it by extract the users and time table.
    :param
        spark: sparksession
        input_data: the location where we load the log_data
        ouput_data: the location where result data will be stored
    :return: None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView('log_table')

    # extract columns for users table    
    users_table = spark.sql("""
    select
        distinct userId as user_id, 
        firstName as first_name, 
        lastName as last_name, 
        gender, 
        level
    from log_table
    where userId is not null
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
    select 
        start_time, 
        hour(start_time) as hour, 
        dayofmonth(start_time) as day, 
        weekofyear(start_time) as week, 
        month(start_time) as month, 
        year(start_time) as year, 
        dayofweek(start_time) as weekday
    from
    (
        select 
            to_timestamp(ts/1000) as start_time
        from log_table
        where ts is not null
    ) as a
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df.createOrReplaceTempView("songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    select 
        monotonically_increasing_id() as songplay_id, 
        a.start_time, 
        month(a.start_time) as month,
        year(a.start_time) as year,
        a.user_id, 
        a.level, 
        b.song_id, 
        b.artist_id, 
        a.session_id, 
        a.location, 
        a.user_agent
    from 
    (
        select 
            to_timestamp(ts/1000) as start_time,
            userId as user_id,
            level,
            sessionId as session_id,
            location,
            userAgent as user_agent,
            song,
            artist,
            length
        from log_table
    ) as a
    inner join
    (
        select
            song_id,
            artist_id,
            title,
            duration,
            artist_name
        from 
            songs_table
    ) as b
    on a.song = b.title and a.length = b.duration and a.artist = b.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
