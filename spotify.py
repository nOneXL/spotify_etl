import pandas as pd
import datetime
import requests
import psycopg2
from sqlalchemy import create_engine

DB_HOST = '192.168.72.128'
DATABASE = 'postgres'
DB_USERNAME = 'myusername'
DB_PASSWORD = 'mypassword'
PORT = 5432

# DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "dessiah.conan"
TOKEN = "BQADr_R711uOc-PhRWQEHLSsBjHivENdkO4DzopzTD1iKp3G49Oei9eBzUI8eQ3h61N5ZLASdTSEgerlbSgHFWxA6iQlSV8YC7r0ToyuM4kKRgJ5uEFHnVEquFinoBRXToxDXm7XwO3ZV6KfLJbonq-vrvlUe6i4I3qpL-rcoD50ZfHEsEH23ILw6G4dZtIkUiqiz1gb"

DAYS_TO_PULL = 2


def check_if_vaild_data(df: pd.DataFrame) -> bool:
    # Check for emtpy list
    if df.empty:
        print("No songs download. Finishing execution")

    # Check for non unique played_at
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")

    # Check for null values
    if df.isnull().values.any():
        raise Exception("Null valued found")

    today = datetime.datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # Check for dates not in interval
    timestamps = df["timestamp"].to_list()
    for timestamp in timestamps:
        if (today - datetime.datetime.strptime(timestamp, '%Y-%m-%d')).days > DAYS_TO_PULL:
            raise Exception("At least one of the returned songs does not come from within the last {days} days".format(
                days=DAYS_TO_PULL))


if __name__ == '__main__':
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    from_date = today - datetime.timedelta(days=DAYS_TO_PULL)
    from_date_unix = int(from_date.timestamp()) * 1000

    request = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
        time=from_date_unix), headers=headers)

    data = request.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["album"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict)

    # Validate
    check_if_vaild_data(song_df)

    # Load
    schema_name = "spotify"
    table_name = "my_played_tracks"

    create_schema = "CREATE SCHEMA IF NOT EXISTS {schema};COMMIT".format(schema=schema_name)
    create_table = """
        CREATE TABLE IF NOT EXISTS {schema}.{table}(
            song_name VARCHAR NOT NULL ,
            artist_name VARCHAR NOT NULL,
            played_at TIMESTAMP NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );COMMIT
    """.format(schema=schema_name, table=table_name)
    # ,PRIMARY KEY (played_at)
    try:
        conn = psycopg2.connect(user=DB_USERNAME,
                                password=DB_PASSWORD,
                                host=DB_HOST,
                                port=PORT,
                                dbname=DATABASE)
        cursor = conn.cursor()

        cursor.execute(create_schema)
        cursor.execute(create_table)
        print(
            "Schema - '{schema}' and Table - '{schema}.{table}' created!".format(schema=schema_name, table=table_name))
        conn.close()
    except:
        print("Cant connect db or create schema or table")

    try:
        engine = create_engine('postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(
            username=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=PORT, database=DATABASE))

        song_df.to_sql(name=table_name, schema=schema_name,
                       con=engine, index=False, if_exists='append')
        print("Appended song df data to postgres db")
    except:
        print("Cant connect db or append dataframe to db")
