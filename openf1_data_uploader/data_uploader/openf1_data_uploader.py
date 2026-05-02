from urllib.request import urlopen
import json
import pandas as pd
import psycopg2
import os
import io
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn = psycopg2.connect(
        host = os.getenv('HOST'),
        dbname = os.getenv('DBNAME'),
        user = os.getenv('DBUSER'),
        password = os.getenv('PASSWORD'),
        port = 5432,
        sslmode="require",
        options = "-c search_path=openf1"
    )
    return conn

def fetch_data(url: str):
    try:
        response = urlopen(url)
        data = json.loads(response.read().decode('utf-8'))

        return data
    
    except Exception as err:
        raise err
    

def fetch_and_transform_meeting_data():

    url = "https://api.openf1.org/v1/meetings"
    data = fetch_data(url=url)
    meeting_df = pd.DataFrame(data)

    # print(session_df.columns)

    # circuit data
    circuit_df = meeting_df[['circuit_key', "circuit_short_name", "circuit_type", "circuit_info_url", "circuit_image"]]
    circuit_df.drop_duplicates(['circuit_key'], inplace=True)
    circuit_df['created_at'] = pd.Timestamp.now()
    circuit_df['updated_at'] = pd.Timestamp.now()

    # Country data
    country_df = meeting_df[['country_key', "country_name", "country_flag", "country_code" ]]
    country_df.drop_duplicates(['country_key'], inplace=True)
    country_df['created_at'] = pd.Timestamp.now()
    country_df['updated_at'] = pd.Timestamp.now()
    
    # meeting data
    meeting_df = meeting_df[['meeting_key', "meeting_name", "meeting_official_name", "year", "location", "is_cancelled", "gmt_offset", "date_start", "date_end", "country_key", "circuit_key"]]
    meeting_df['is_cancelled'] = meeting_df['is_cancelled'].transform(lambda x: 1 if x == 'True' else 0)
    print(meeting_df['is_cancelled'])
    return circuit_df, country_df, meeting_df


def load_df_to_db(cursor, df: pd.DataFrame, table_name: str):
    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_from(
        buffer, 
        table_name,
        sep=",",
        null=""
    )

def apply_cdc_circuit(cur):

    query = """

    Insert into openf1.circuit(
    circuit_key,
    circuit_short_name,
    circuit_type,
    circuit_info_url,
    circuit_image,
    updated_at  
    )
    select circuit_key, circuit_short_name, circuit_type, circuit_info_url, circuit_image, updated_at
    from openf1.circuit_stagging
    on conflict (circuit_key)
    DO UPDATE SET
    circuit_short_name = EXCLUDED.circuit_short_name,
    circuit_type = EXCLUDED.circuit_type,
    circuit_info_url = EXCLUDED.circuit_info_url,
    circuit_image = EXCLUDED.circuit_image,
    updated_at = now()
    """
    cur.execute(query)

    truncate_query = "truncate table openf1.circuit_stagging"
    cur.execute(truncate_query)

def apply_cdc_country(cur):

    query = """
    INSERT INTO openf1.country(
    country_key,
    country_name,
    country_flag,
    country_code
    )
    SELECT 
    country_key,
    country_name,
    country_flag,
    country_code
    FROM openf1.country_stagging
    ON CONFLICT (country_key)
    DO UPDATE SET
    country_name = EXCLUDED.country_name,
    country_flag = EXCLUDED.country_flag,
    country_code = EXCLUDED.country_code,
    updated_at = now()
    """
    cur.execute(query)

    # truncate stagging table
    truncate_query = "truncate table openf1.country_stagging"
    cur.execute(truncate_query)

def apply_cdc_meeting(cur):

    query = """
    INSERT INTO openf1.meeting(
    meeting_key,
    meeting_name,
    meeting_official_name,
    year,
    location,
    is_cancelled ,
    gmt_offset ,
    date_start  ,
    date_end ,
    country_key,
    circuit_key 
    )
    SELECT 
    meeting_key,
    meeting_name,
    meeting_official_name,
    year,
    location,
    is_cancelled ,
    gmt_offset ,
    date_start  ,
    date_end ,
    country_key,
    circuit_key
    FROM openf1.meeting_stagging
    ON CONFLICT (meeting_key)
    DO UPDATE SET
    meeting_key = EXCLUDED.meeting_key,
    meeting_name = EXCLUDED.meeting_name,
    meeting_official_name = EXCLUDED.meeting_official_name,
    year = EXCLUDED.year,
    location = EXCLUDED.location, 
    is_cancelled = EXCLUDED.is_cancelled ,
    gmt_offset = EXCLUDED.gmt_offset ,
    date_start = EXCLUDED.date_start,
    date_end = EXCLUDED.date_end,
    country_key = EXCLUDED.country_key,
    circuit_key = EXCLUDED.circuit_key,
    updated_at = now()
    """
    cur.execute(query)

    #Truncate table 
    truncate_query = "truncate table openf1.meeting_stagging"

    cur.execute(truncate_query)

def fetch_and_transform_session_data():

    url = "https://api.openf1.org/v1/sessions"
    data = fetch_data(url=url)
    data_df = pd.DataFrame(data)

    # session data
    sessions_df = data_df[[
        "session_key",
        "session_name",
        "session_type",
        "year",
        "date_start",
        "date_end",
        "is_cancelled",
        "meeting_key",
        "country_key",
        "circuit_key",
    ]]

    sessions_df['is_cancelled'] = sessions_df["is_cancelled"].transform(lambda x: 1 if x == 'True' else 0)

    return sessions_df


def apply_cdc_sessions(cur):

    query = """
    INSERT INTO sessions(
    session_key,
    session_name,
    session_type,
    year,
    date_start,
    date_end,
    is_cancelled,
    meeting_key,
    country_key,
    circuit_key
    )
    select 
    session_key,
    session_name,
    session_type,
    year,
    date_start,
    date_end,
    is_cancelled,
    meeting_key,
    country_key,
    circuit_key
    from sessions_stagging
    ON CONFLICT (session_key)
    DO UPDATE SET
    session_key = EXCLUDED.session_key,
    session_name = EXCLUDED.session_name,
    session_type = EXCLUDED.session_type,
    year = EXCLUDED.year,
    date_start = EXCLUDED.date_start,
    date_end = EXCLUDED.date_end,
    is_cancelled = EXCLUDED.is_cancelled,
    meeting_key = EXCLUDED.meeting_key,
    country_key = EXCLUDED.country_key,
    circuit_key = EXCLUDED.circuit_key
    """

    cur.execute(query)

    # truncate 
    truncate_query = "truncate table sessions_stagging"
    
    cur.execute(truncate_query)

def fetch_and_transform_drivers_data():

    url = "https://api.openf1.org/v1/drivers"
    data = fetch_data(url=url)
    data_df = pd.DataFrame(data)

    driver_df = data_df[[
        "driver_number",
        "first_name",
        "full_name",
        "last_name" ,
        "broadcast_name",
        "name_acronym" ,
        "team_name" ,
        "team_colour",
        "meeting_key",
        "session_key"
    ]]
    
    return driver_df

def apply_cdc_drivers(cur):

    query = """
    INSERT INTO drivers(
    driver_number,
    first_name,
    full_name,
    last_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour,
    meeting_key,
    session_key)
    SELECT 
    driver_number,
    first_name,
    full_name,
    last_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour,
    meeting_key,
    session_key
    FROM openf1.drivers_stagging
    """
    cur.execute(query)

    truncate_query = "truncate table drivers_stagging"
    cur.execute(truncate_query)


def bulk_load_meeting_data_into_db():
    
    print("Fetching data ......")
    circuit_df, country_df, meeting_df = fetch_and_transform_meeting_data()

    print("Data fetch completed")

    print("Data loading ....")
    with get_connection() as conn:
        with conn.cursor() as cursor:
            print("Circuit Data load started")
            load_df_to_db(cursor=cursor, df=circuit_df, table_name="circuit_stagging")
            apply_cdc_circuit(cur=cursor)
            print("Circute Data load is completed successfully")
            print("Country Data load started")
            load_df_to_db(cursor=cursor, df=country_df, table_name="country_stagging")
            apply_cdc_country(cur=cursor)
            print("Country Data load completed successfully")
            print("Meeting data load started")
            load_df_to_db(cursor=cursor, df=meeting_df, table_name="meeting_stagging")
            apply_cdc_meeting(cur=cursor)
            print("Meeting Data load completed successfully")

def bulk_load_session_data_into_db():
    
    print("Fetching Session data...")
    sessions_df = fetch_and_transform_session_data()
    
    print("Start uploading...")

    with get_connection() as conn:
        with conn.cursor() as cursor:
            load_df_to_db(cursor=cursor, df=sessions_df, table_name='sessions_stagging')
            apply_cdc_sessions(cur=cursor)
    
    print("Session Data upload Completed")


def bulk_load_drive_data_into_db():

    print("Fetching Driver data....")

    driver_df = fetch_and_transform_drivers_data()

    with get_connection() as conn:
        with conn.cursor() as cursor:
            load_df_to_db(cursor=cursor, df=driver_df, table_name='drivers_stagging')
            apply_cdc_drivers(cur=cursor)




if __name__ == "__main__":

    # bulk_load_meeting_data_into_db()

    # print(meeting_df.columns)

    # bulk_load_session_data_into_db()

    bulk_load_drive_data_into_db()
