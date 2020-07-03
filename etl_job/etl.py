# extract transform pipeline
import time
import logging
from datetime import datetime
from pymongo import MongoClient
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. extract data from mongodb, connect to the db
# connect to mongodb
client = MongoClient(host='mongodb', port=27017)
db = client.twitter
tweets_mongo = db.tweets


# connect to Postgres
DATABASE_USER = 'postgres'  # as defined in docker-compose.yaml
DATABASE_PASSWORD = '1234'  # as defined in docker-compose.yaml
DATABASE_HOST = 'postgres'  # name of the service in docker-compose.yaml!!!
DATABASE_PORT = '5432'  # internal port as defined in docker-compose.yaml
DATABASE_DB_NAME = 'postgres'  # ALWAYS THE DEFAULT IN POSTGRES

engine = create_engine(f'postgres://{DATABASE_USER}:{DATABASE_PASSWORD}\
@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB_NAME}')

# create a database table
create_query = """
CREATE TABLE IF NOT EXISTS tweets (
user_name TEXT,
tweet_text TEXT,
followers_count REAL,
sentiment_score REAL,
time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
engine.execute(create_query)


# initialize sentiment analysis
s = SentimentIntensityAnalyzer()


# Instantiate last timestamp
last_timestamp = datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

# write a query to extract database


def extract(last_timestamp):
    '''Extracts all tweets from the MongoDB database'''
    extracted_tweets = list(tweets_mongo.find({"time_stamp": {"$gt": last_timestamp}}))
    return extracted_tweets

# 2. perform sentiment analysis


def transform(extracted_tweets):
    transformed_tweets = []
    for tweet in extracted_tweets:
        sentiment = s.polarity_scores(tweet['tweet_text'])
        tweet['sentiment_score'] = sentiment['compound']
        transformed_tweets.append(tweet)
    return transformed_tweets

# 3. load transformed data into POSTGRES


def load(transformed_tweets):
    for tweet in transformed_tweets:

        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s, %s, %s);"
        data = [tweet["user_name"], tweet["tweet_text"],
                tweet["followers_count"], tweet["sentiment_score"], tweet["time_stamp"]]
        engine.execute(insert_query, data)
    logging.critical('Successfully added all tweets to postgres db')


while True:
    extracted_tweets = extract(last_timestamp)
    transformed_tweets = transform(extracted_tweets)
    load(transformed_tweets)
    last_timestamp = extracted_tweets[-1]['time_stamp']

    time.sleep(60)
