import pandas as pd 
import psycopg2
import os 
import queries
import glob
from dotenv import load_dotenv


def process_song_file(cur,path):
    df = None
    try:
        df = pd.read_json(path,lines=True)
        song_table_details = df[["song_id","title","artist_id","year","duration"]].values[0].tolist()
    except Exception as e :
        print(e)

    cur.execute(queries.song_table_insert,song_table_details)

    artist_details = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()

    cur.execute(queries.artist_table_insert,artist_details)
    


def process_log_file(cur, filepath):
    """
        This function reads Log files and reads information of time, user and songplay data and saves into time, user, songplay
        Arguments:
        cur: Database Cursor
        filepath: location of Log files
        Return: None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df['page'] == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], uniet='ms')
    
    # insert time data records
    time_data = list((t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
    time_df =  pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(queries.time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(queries.user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(queries.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId,\
                     row.location, row.userAgent)
        cur.execute(queries.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
        print(datafile)
        try:
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
        except Exception as e:
            print(e)

def main():

    load_dotenv()
    try:
        conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"))
        print(conn.info)
    except:
        raise(f"error in connection")

    cur = conn.cursor()

    


    process_data(cur, conn, filepath='/home/raed/code/Modeling_Postgresql/Song_DB/Data/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='/home/raed/code/Modeling_Postgresql/Song_DB/Data/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()