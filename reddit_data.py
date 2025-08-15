"""Experiment 2 - data collection from REDDIT"""

import praw
import pandas as pd

# ---AUTHENTICATION---
import config

reddit = praw.Reddit(
    client_id=config.CLIENT_ID,
    client_secret=config.CLIENT_SECRET,
    user_agent=config.USER_AGENT
)

# --- CONFIGURATION ---
search_query = "zomato"
target_subreddits = ['mumbai', 'delhi', 'bangalore', 'pune', 'india', 'IndianStreetBets', 'indiasocial']
scraped_data = []

# --- DATA COLLECTION ---
# Loop through each subreddit individually
for sub_name in target_subreddits:
    print(f"--- Searching in r/{sub_name} ---")
    subreddit = reddit.subreddit(sub_name)
    
    try:
        # Search for the top 40 posts of the year in the subreddit
        for submission in subreddit.search(search_query, sort='top', time_filter='year', limit=30):
            # --- For Posts ---
            # Create a dictionary for the post itself
            scraped_data.append({
                'source_platform': 'Reddit_Post',
                'text': f"Title: {submission.title}. Body: {submission.selftext}",
                'location': sub_name,
                'hashtags': None, # Keeping schema consistent
                'urls': submission.url,
                'post_author': str(submission.author),
                'post_score': submission.score,
                'comment_count': submission.num_comments,
                'upvote_ratio': submission.upvote_ratio,
                'comment_author': None, # Not a comment
                'comment_score': None # Not a comment
            })

            # --- For Comments ---
            # Fetch and loop through the top 5 comments of the post
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list()[:5]:
                # Create a dictionary for each comment
                scraped_data.append({
                    'source_platform': 'Reddit_Comment',
                    'text': comment.body,
                    'location': sub_name,
                    'hashtags': None,
                    'urls': f"https://reddit.com{comment.permalink}",
                    'post_author': str(submission.author),
                    'post_score': submission.score,
                    'comment_count': submission.num_comments,
                    'upvote_ratio': submission.upvote_ratio,
                    'comment_author': str(comment.author),
                    'comment_score': comment.score
                })
    except Exception as e:
        print(f"Could not process r/{sub_name}. Error: {e}")

# --- STORING THE DATA ---
df = pd.DataFrame(scraped_data)
output_file = 'reddit_rawdata.csv'
df.to_csv(output_file, index=False)

print(f"\n✅ Successfully collected data!")
print(f"{len(df)} total entries (posts and comments) saved to '{output_file}'")
