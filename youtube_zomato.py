import pandas as pd
from googleapiclient.discovery import build
import config

# --- 1. Setup ---
api_key = config.YOUTUBE_API_KEY
youtube = build('youtube', 'v3', developerKey=api_key)

# Zomato's unique and permanent Channel ID
zomato_channel_id = "UCD7kbZQyYIR6RgJQYW9w0Tg" 
uploads_playlist_id = None
video_ids = []

# --- 2. Find the official Uploads Playlist ID from the Channel ID ---
print(f"Finding the uploads playlist for channel ID: {zomato_channel_id}...")
try:
    request = youtube.channels().list(
        part="contentDetails",
        id=zomato_channel_id
    )
    response = request.execute()
    
    if 'items' in response and response['items']:
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(f"Successfully found Uploads Playlist ID: {uploads_playlist_id}")
    else:
        print("\n--- ERROR ---")
        print("API did not return any items for the channel.")
        print("This is likely because your API Key is invalid or you have exceeded your daily quota.")
        print("Please check your key and quota in the Google Cloud Console.")
        print("API Response:", response) # Prints the raw response for debugging
        exit()

except Exception as e:
    print(f"An error occurred while finding the uploads playlist: {e}")
    exit()

# --- 3. Get all Video IDs directly from the Uploads Playlist ---
if uploads_playlist_id:
    print("Fetching all video IDs from the playlist...")
    next_page_token = None
    try:
        while True:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_playlist_id,
                maxResults=10,
                pageToken=next_page_token
            )
            response = request.execute()
            
            video_ids.extend([item['contentDetails']['videoId'] for item in response.get('items', [])])
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        print(f"Found {len(video_ids)} total video IDs.")

    except Exception as e:
        print(f"An error occurred while fetching video IDs: {e}")
        exit()

# --- 4. Get full details for all found videos ---
if video_ids:
    print("Fetching full details for all videos...")
    videos_data = []
    for i in range(0, len(video_ids), 50):
        chunk_ids = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(chunk_ids)
        )
        response = request.execute()

        for item in response.get('items', []):
            snippet = item.get('snippet', {})
            stats = item.get('statistics', {})
            video_id = item['id']
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            videos_data.append({
                'video_id': video_id,
                'video_url': video_url,
                'title': snippet.get('title'),
                'description': snippet.get('description'),
                'tags': "|".join(snippet.get('tags', [])),
                'publish_date': snippet.get('publishedAt'),
                'view_count': stats.get('viewCount'),
                'like_count': stats.get('likeCount'),
                'comment_count': stats.get('commentCount')
            })

    df = pd.DataFrame(videos_data)
    output_file = 'data/youtube_official_videos.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Success! Data for {len(df)} videos saved to '{output_file}'")
else:
    print("No video IDs were found.")
