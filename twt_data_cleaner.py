import pandas as pd
import re
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

def clean_twitter_data(df: pd.DataFrame, text_column: str) -> pd.DataFrame:
    """
    Cleans a specified text column in a DataFrame, handling both English and Hindi.
    """
    print("--- Starting Data Cleaning Process ---")
    
    clean_df = df.copy()
    
    # 1. Create a combined set of English and Hindi stopwords
    stop_words = set(stopwords.words('english'))
    
    # 2. Define a nested function to apply all cleaning steps to a single text entry
    def process_text(text):
        # Handle non-string data
        if not isinstance(text, str):
            return ""
            
        # Step 1: Convert to lowercase
        text = text.lower()
        
        # Step 2: Remove HTML tags (if any)
        text = BeautifulSoup(text, "html.parser").get_text()
        
        # Step 3: Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # Step 4: Remove mentions (@) and hashtags (#)
        text = re.sub(r'@[A-Za-z0-9_]+|#[A-Za-z0-9_]+', '', text)
        
        # Step 5: Remove punctuation and numbers
        # This regex keeps English letters and whitespace
        text = re.sub(r'[^a-z\u0900-\u097f\s]', '', text)
        
        # Step 6: Standardize words (remove multiple letters)
        text = re.sub(r'(.)\1{2,}', r'\1\1', text)
        
        # Step 7: Remove all extra whitespaces
        text = " ".join(text.split())
        
        # Step 8: Tokenization (split text into words)
        tokens = word_tokenize(text)
        
        # Step 9: Stopword removal (using the combined list)
        filtered_tokens = [word for word in tokens if word not in stop_words]
        
        return filtered_tokens # Return the list of cleaned tokens

    # 3. Apply the processing function to the specified column
    clean_df['cleaned_tokens'] = clean_df[text_column].apply(process_text)
    
    print("--- Cleaning Process Finished ---")
    return clean_df

# --- DEMONSTRATION ---

# 1. Create a DataFrame with your sample data
import nltk
nltk.download('punkt_tab')

raw_df = raw_df = pd.read_csv("data/zomato_tweets.csv")


# --- RUN THE CLEANING FUNCTION ---
cleaned_df = clean_twitter_data(raw_df, 'text')

# --- CLEANED DATA SNAPSHOT ---
# We'll create a new column with the tokens joined back into a string for easy reading
cleaned_df['cleaned_text_str'] = cleaned_df['cleaned_tokens'].apply(lambda tokens: ' '.join(tokens))
cleaned_df.to_csv('data/twts_clean.csv', index=False)
