### Introduction
- This is the first project introducing the using ways of ApacheCassandra and nosql databases modeling methods.
### Purpose
- A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. 
### Mission
- 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
- 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
- 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
### Database Design
- Mission One: There should be a unique key to separate each row of this table, according to the where clause, I choose (session_id + iteminsession) as primary key -- don't need to care about the users' infomation
    - partition_key: session_id
    - clustering columns: iteminsession
- Mission Two: This mission has include the information of user's, so I need to put the user_id into the primary key
    - partition_key : user_id + session_id
    - clustering columns: iteminsession
- Mission Three: This mission focuses on the song_title, so I have to use song to partition
    - partition_key : song
    - clustering columns : user_id

