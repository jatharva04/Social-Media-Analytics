import pandas as pd

df = pd.read_csv('data/reddit_data_cleaned_2.csv')

print(f"Total rows in the dataset: {len(df)}")

# Filter for rows where the 'location' column is 'mumbai'
mumbai_df = df[df['location'] == 'mumbai']

print(f"Found {len(mumbai_df)} entries specifically for the location 'mumbai'.")

# print("\n--- Mumbai Data Sample ---")
# print(mumbai_df.head())

output_file = 'reddit_filtered_mumbai.csv'
mumbai_df.to_csv(output_file, index=False)
