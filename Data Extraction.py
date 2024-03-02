import json
import os
import praw
import pandas as pd
from datetime import datetime
import boto3


def extract_data_and_save(subreddit_name, search_keyword):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    user_agent = os.environ.get('user_agent')
    reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

    # Extract data from Reddit
    subreddit = reddit.subreddit(subreddit_name)
    search_results = subreddit.search(search_keyword, sort='top', time_filter='all', limit=None)

    # Extract datetime and title
    data = []
    for submission in search_results:
        created_utc = submission.created_utc
        datetime_post = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d')
        title = submission.title
        entry = {
            'datetime_post': datetime_post,
            'title': title,
        }
        data.append(entry)

    # Create a DataFrame
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset='title')
    csv_data = df.to_csv(index=False)

    # Create folders if they don't exist
    folder_path = f"raw_data/to_process/{search_keyword.lower()}"

    # Save CSV to S3
    client = boto3.client('s3')
    client.put_object(
        Bucket="reddit-sentiment-analysis-yash",
        Key=f"{folder_path}/{search_keyword.lower()}_to_process.csv",
        Body=csv_data,
        ContentType='text/csv',
    )





def lambda_handler(event, context):
    extract_data_and_save('all', 'trudeau')
    extract_data_and_save('all', 'poilievre')
