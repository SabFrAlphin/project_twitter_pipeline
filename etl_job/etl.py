# extract transform pipeline
import time
import logging
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
sentiment_score DECIMAL,
time_stamp TIMESTAMP
);
"""

engine.execute(create_query)


# initialize sentiment analysis
s = SentimentIntensityAnalyzer()

# write a query to extract database


def extract():
    # extracts all tweets from MongoDB
    extracted_tweets = list(tweets_mongo.find())
    return extracted_tweets

# 2. perform sentiment analysis


def transform(extracted_tweets):
    # performs sentiment analysis on tweets_mongo and returns in a format
    # so that the tweets can be written into a postgres database
    # for every tweet in extracted tweets we want the perform sentiment
    # ananlysis on the text
    transformed_tweets = []
    for tweet in extracted_tweets:
        # tweet is a dictionary; sentiment score will be calculated in the afternoon
        sentiment = s.polarity_scores(tweet['tweet_text'])
        tweet['sentiment_score'] = sentiment['compound']
        transformed_tweets.append(tweet)
    return transformed_tweets

# 3. load transformed data into POSTGRES


def load(transformed_tweets):
    # transformed_tweets is a list and we want to write each tweet into a db
    # so
    for tweet in transformed_tweets:  # the [-1:] leads to us only loading the last tweet
        # insert_query = f"""INSERT INTO tweets VALUES (
        #                 "{tweet["user_name"]}",
        #                 "{tweet["tweet_text"]}",
        #                 "{tweet["followers_count"]}",
        #                 "{tweet["time_stamp"]}",
        #                 "{tweet["sentiment_score"]}"
        #                 );"""
        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s, %s, %s);"
        data = [tweet["user_name"], tweet["tweet_text"], tweet["followers_count"],
                tweet["time_stamp"], tweet["sentiment_score"]]
        engine.execute(insert_query, data)
    logging.critical('Successfully added all tweets to postgres db')


# until we stop the container or an error occurs
while True:
    # extract the tweets from the mongodb, transform and load to postgres
    extracted_tweets = extract()
    transformed_tweets = transform(extracted_tweets)
    load(transformed_tweets)
    time.sleep(60)

'''
in the code as it is written right now , what is happening is that the whole
collection of documents from the mongodb is extracted in every single rund
of the ETL processand is transformed and loaded into the postgress darabase.
That means we will have a lot of duplicates in the postgres db

easiest fix:
-only load the last tweet

advanced:
- introdue a timestamp into the mondodb DATABASE and only query the tweets
that have not been extracted in the last run
- if you run tweepy, you get a data field called 'extracted_at'
- you could tag the extracted tweets in the mongodb (by object id)
- or drop and run a full load
'''
