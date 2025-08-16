import pandas as pd
import re
import os # Import the 'os' module to check file size
from pathlib import Path # Import 'Path' for robust path handling

def clean_engagement_count(value):
    """
    Converts string counts like '16K' or '1.2M' into proper integers.
    """
    if isinstance(value, str):
        value = value.lower().strip()
        if 'k' in value:
            return int(float(value.replace('k', '')) * 1000)
        if 'm' in value:
            return int(float(value.replace('m', '')) * 1000000)
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

# --- Main Conversion Logic ---

json_file_path = 'zomato_tweets.json'
csv_file_path = 'zomato_twitter_data_cleaned.csv'

# --- NEW: Check if the file exists and is not empty ---
if Path(json_file_path).exists() and os.path.getsize(json_file_path) > 0:
    
    # --- All the previous logic now goes inside this 'if' block ---
    
    print(f"Reading data from '{json_file_path}'...")
    # 1. Load the JSON Lines file
    df = pd.read_json(json_file_path)

    # 2. Clean the engagement columns
    if 'likes' in df.columns:
        df['likes'] = df['likes'].apply(clean_engagement_count)
    if 'retweets' in df.columns:
        df['retweets'] = df['retweets'].apply(clean_engagement_count)

    # 3. Flatten the 'hashtags' list
    # if 'hashtags' in df.columns:
    #     df['hashtags'] = df['hashtags'].apply(
    #         lambda h: '|'.join(h) if isinstance(h, list) else None
    #     )

    # 4. Clean the 'text' column
    # if 'text' in df.columns:
    #     df['text'] = df['text'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # 5. Save the cleaned DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')

    print(f"\n✅ Success! Converted {len(df)} records.")
    print(f"Cleaned CSV file saved as: {csv_file_path}")

else:
    # --- NEW: Handle the case of an empty or non-existent file ---
    print(f"'{json_file_path}' is empty or does not exist. No data to convert.")