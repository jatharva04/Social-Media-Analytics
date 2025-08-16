import sqlite3
import pandas as pd

DB_NAME = "twitter_zomato.db"

def connect_db():
    """Connects to the SQLite database and returns the connection object."""
    try:
        conn = sqlite3.connect(DB_NAME)
        print(f"Successfully connected to database '{DB_NAME}'")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

conn = connect_db()

raw_df = pd.read_csv('data/zomato_tweets.csv')
cleaned_df = pd.read_csv('data/twts_clean.csv')
raw_df.to_sql('raw_data', conn, if_exists='replace', index=False)
cleaned_df.to_sql('clean_data', conn, if_exists='replace', index=False)

conn.close()
print(f"Data successfully stored in the {DB_NAME} SQLite file")

