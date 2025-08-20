# save as get_youtube_comments.py
import pandas as pd
from googleapiclient.discovery import build
import config

# --- 1. Setup ---
api_key = config.YOUTUBE_API_KEY
youtube = build('youtube', 'v3', developerKey=api_key)

# --- Load the video IDs from the OFFICIAL videos list ---
try:
    videos_df = pd.read_csv('data/youtube_official_videos.csv')
    video_ids = videos_df['video_id'].tolist()
except FileNotFoundError:
    print("Error: 'youtube_official_videos.csv' not found. Please run 'youtube_zomato.py' first.")
    exit()

# --- NEW: Sort and filter for the latest 100 videos ---
print(f"Loaded details for {len(videos_df)} videos.")
# Convert publish_date to a datetime object to ensure correct sorting
videos_df['publish_date'] = pd.to_datetime(videos_df['publish_date'])
# Sort by date, newest first
videos_df_sorted = videos_df.sort_values(by='publish_date', ascending=False)
# Select the top 100 latest videos
latest_100_videos_df = videos_df_sorted.head(100)
video_ids = latest_100_videos_df['video_id'].tolist()
print(f"Focusing on the {len(video_ids)} most recent videos for comment analysis.")

# --- 3. Loop through each video and get comments ---
all_comments_data = []
print(f"Fetching top 20 comments for {len(video_ids)} videos...")

for video_id in video_ids:
    try:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            order='relevance',
            maxResults=20,
            textFormat='plainText'
        )
        response = request.execute()
        
        for item in response.get('items', []):
            comment_snippet = item['snippet']
            top_comment = comment_snippet['topLevelComment']['snippet']
            all_comments_data.append({
                'video_id': video_id,
                'comment_text': top_comment['textDisplay'],
                'author': top_comment['authorDisplayName'],
                'like_count': top_comment['likeCount'],
                'publish_date': top_comment['publishedAt'],
                'reply_count': comment_snippet['totalReplyCount']
            })
    except Exception as e:
        print(f"Could not get comments for video {video_id}: {e}")
        continue

# --- 4. Save comments to a new CSV ---
df_comments = pd.DataFrame(all_comments_data)
df_comments.to_csv('data/youtube_comments.csv', index=False)

print(f"\n✅ Success! Fetched {len(df_comments)} comments and saved them to 'youtube_comments.csv'")
