import os
import nltk
import re

#downloading necessary module - vader_lexicon
nltk.data.path.append("/tmp")
nltk.download("vader_lexicon", download_dir="/tmp")
nltk.download("punkt", download_dir="/tmp")


from datetime import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from pprint import pprint
import json
import boto3
from io import StringIO
import pandas as pd


# Function to remove special characters from a text
def remove_special_characters(text):
    # Use regex to remove special characters
    text = re.sub(r'[^A-Za-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    # Remove newlines
    text = text.replace('\n', ' ').replace('\r', '')
    return text


def transform_and_save(search_keyword):
    client = boto3.client('s3')
    Bucket = "reddit-sentiment-analysis-yash"
    old_file_key = f"raw_data/processed/{search_keyword.lower()}/"
    Key = f"raw_data/to_process/{search_keyword.lower()}/"
    
    # Check if the old file exists in the S3 bucket
    try:
        client.head_object(Bucket=Bucket, Key=f'raw_data/processed/{search_keyword.lower()}/{search_keyword.lower()}_raw.csv')
        old_file_exists = True
    except Exception as e:
        old_file_exists = False
    
    reddit_keys=[]
    if old_file_exists:
        # Download the old CSV file from S3
        old_file_obj = client.get_object(Bucket=Bucket, Key=f'raw_data/processed/{search_keyword.lower()}/{search_keyword.lower()}_raw.csv')
        old_content = old_file_obj['Body'].read().decode('utf-8')
        old_data = pd.read_csv(StringIO(old_content))
        
        for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
            file_key = file['Key']
            if file_key.split('.')[-1] == 'csv':
                response = client.get_object(Bucket=Bucket, Key= file_key)
                content = response['Body'].read().decode('utf-8')
                new_data = pd.read_csv(StringIO(content))
                reddit_keys.append(file_key)
                
                # Append the new data to the old data
                df = pd.concat([old_data, new_data], ignore_index=True)
                
                #removing duplicates (only titles)
                df = df.drop_duplicates(subset='title')
                df_csv = df.to_csv(index=False)
                client.put_object(
                    Bucket = Bucket,
                    Key = f'raw_data/processed/{search_keyword.lower()}/{search_keyword.lower()}_raw.csv',
                    Body= df_csv,
                    ContentType='text/csv',
                    )
                s3_resource = boto3.resource('s3')
                for key in reddit_keys:
                    copy_source = {
                        'Bucket' : Bucket,
                        'Key' : key
                        }
                    #s3_resource.meta.client.copy(copy_source, Bucket, f'raw_data/processed/{search_keyword.lower()}/{search_keyword.lower()}_raw.csv')
                    s3_resource.Object(Bucket, key).delete()
                
                
                sia = SIA()
                results = []
                
                for index, row in df.iterrows():
                    line = row['title']
                    pol_score = sia.polarity_scores(line)
                    pol_score['title'] = remove_special_characters(line)
                    pol_score['datetime_post'] = row['datetime_post']  # Retain datetime_post
                    results.append(pol_score)


                df = pd.DataFrame.from_records(results)
                
                #Conditioning, to convert to [-1,0,1]
                df['label'] = 0
                df.loc[df['compound'] > 0.2, 'label'] = 1
                df.loc[df['compound'] < -0.2, 'label'] = -1
                """
                df['neutral_label'] = 0
                df['negative_label'] = 0
                df['positive_label'] = 0
                df.loc[df['label'] == 0, 'neutral_label'] = 1
                df.loc[df['label'] == 1, 'positive_label'] = 1
                df.loc[df['label'] == -1, 'negative_label'] = 1
                """
                
                df['label'] = 0
                df['neutral_label'] = 0
                df['negative_label'] = 0
                df['positive_label'] = 0

                for index, row in df.iterrows():
                    if row['compound'] > 0.2:
                        df.at[index, 'label'] = 1
                        df.at[index, 'positive_label'] = 1
                    elif row['compound'] < -0.2:
                        df.at[index, 'label'] = -1
                        df.at[index, 'negative_label'] = 1
                    else:
                        df.at[index, 'neutral_label'] = 1
                
                #print(df.head())
                
                
                csv_data = df.to_csv(index=False)
                
                
                # Check if the old file exists in the transformed folder
                try:
                    client.head_object(Bucket=Bucket, Key=f'transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv')
                    transformed_file_exists = True
                except Exception as e:
                    transformed_file_exists = False
                
                if transformed_file_exists:
                    s3_resource.Object(Bucket, f'transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv').delete()
                    transformed_key1 = f"transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv"
                    client.put_object(
                        Bucket = Bucket,
                        Key = transformed_key1,
                        Body=csv_data,
                        ContentType='text/csv',
                        )
                else:
                    transformed_key1 = f"transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv"
                    client.put_object(
                        Bucket = Bucket,
                        Key = transformed_key1,
                        Body=csv_data,
                        ContentType='text/csv',
                        )            
                
                
        
    
    else:
        for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
            file_key = file['Key']
            if file_key.split('.')[-1] == 'csv':
                response = client.get_object(Bucket=Bucket, Key= file_key)
                content = response['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(content))
                reddit_keys.append(file_key)
                
                s3_resource = boto3.resource('s3')
                for key in reddit_keys:
                    copy_source = {
                        'Bucket' : Bucket,
                        'Key' : key
                    }
                    s3_resource.meta.client.copy(copy_source, Bucket, f'raw_data/processed/{search_keyword.lower()}/{search_keyword.lower()}_raw.csv')
                    s3_resource.Object(Bucket, key).delete()
            
                sia = SIA()
                results = []

                for index, row in df.iterrows():
                    line = row['title']
                    pol_score = sia.polarity_scores(line)
                    pol_score['title'] = remove_special_characters(line)
                    pol_score['datetime_post'] = row['datetime_post']  # Retain datetime_post
                    results.append(pol_score)

                df = pd.DataFrame.from_records(results)
            
                #Conditioning, to convert to [-1,0,1]
                
                df['label'] = 0
                df.loc[df['compound'] > 0.2, 'label'] = 1
                df.loc[df['compound'] < -0.2, 'label'] = -1
                """
                df['neutral_label'] = 0
                df['negative_label'] = 0
                df['positive_label'] = 0
                df.loc[df['label'] == 0, 'neutral_label'] = 1
                df.loc[df['label'] == 1, 'positive_label'] = 1
                df.loc[df['label'] == -1, 'negative_label'] = 1
                """
                
                df['label'] = 0
                df['neutral_label'] = 0
                df['negative_label'] = 0
                df['positive_label'] = 0

                for index, row in df.iterrows():
                    if row['compound'] > 0.2:
                        df.at[index, 'label'] = 1
                        df.at[index, 'positive_label'] = 1
                    elif row['compound'] < -0.2:
                        df.at[index, 'label'] = -1
                        df.at[index, 'negative_label'] = 1
                    else:
                        df.at[index, 'neutral_label'] = 1
                
                #print(df.head())
                
                csv_data = df.to_csv(index=False)
                
                
                # Check if the old file exists in the transformed folder
                try:
                    client.head_object(Bucket=Bucket, Key=f'transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv')
                    transformed_file_exists = True
                except Exception as e:
                    transformed_file_exists = False
                
                if transformed_file_exists:
                    s3_resource.Object(Bucket, f'transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv').delete()
                    transformed_key1 = f"transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv"
                    client.put_object(
                        Bucket = Bucket,
                        Key = transformed_key1,
                        Body=csv_data,
                        ContentType='text/csv',
                        )
                else:
                    transformed_key1 = f"transformed_data/{search_keyword.lower()}/transformed_{search_keyword.lower()}.csv"
                    client.put_object(
                        Bucket = Bucket,
                        Key = transformed_key1,
                        Body=csv_data,
                        ContentType='text/csv',
                        )
                
                



def lambda_handler(event, context):
    transform_and_save('trudeau')
    transform_and_save('poilievre')
