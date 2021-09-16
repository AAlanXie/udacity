import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[songs_columns].values
    try:
        cur.execute(song_table_insert, list(song_data[0]))
    except psycopg2.Error as e:
        print("Error: Issue insert song table")
        print(e)
    
    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_columns].values
    try:
        cur.execute(artist_table_insert, list(artist_data[0]))
    except psycopg2.Error as e:
        print("Error: Issue insert artist table")
        print(e)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the user table.
    Finally it extracts the fact information in order to store it into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    timestamp = t.values

    # use attribute dt to get details
    hour = t.dt.hour.values
    day = t.dt.day.values
    week = t.dt.week.values
    month = t.dt.month.values
    year = t.dt.month.values
    weekday = t.dt.weekday.values
    
    # define time_data
    time_data = []
    for i in range(len(timestamp)):
        tmp = [str(timestamp[i]), hour[i], day[i], week[i], month[i], year[i], weekday[i]]
        time_data.append(tmp)
    
    # define column
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # transfer to dataframe
    time_df = pd.DataFrame(time_data, columns=column_labels)
    
    # insert into time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # convert timestamp datetime
    df['ts'] = pd.to_datetime(df['ts'])

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure extract the absolute filepath for each file provided under the provided filepath.
    Call the function to import the file into the database.

    INPUTS: 
    * cur the cursor variable
    * conn the connection variable to the database
    * filepath the file path to the song file
    * func used function
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This main procedure provide the connection to the database and call various functions
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    """
    Call the main function
    """
    
    main()