import slack
import time
import sqlalchemy as db
from config import OAUTH_TOKEN

oauth_token = OAUTH_TOKEN

client = slack.WebClient(token=oauth_token)

# connect to Postgres
DATABASE_USER = 'postgres'  # as defined in docker-compose.yaml
DATABASE_PASSWORD = '1234'  # as defined in docker-compose.yaml
DATABASE_HOST = 'postgres'  # name of the service in docker-compose.yaml!!!
DATABASE_PORT = '5432'  # internal port as defined in docker-compose.yaml
DATABASE_DB_NAME = 'postgres'  # ALWAYS THE DEFAULT IN POSTGRES

engine = db.create_engine(f'postgres://{DATABASE_USER}:{DATABASE_PASSWORD}\
@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB_NAME}')
connection = engine.connect()
metadata = db.MetaData()

tweets = db.Table('tweets', metadata, autoload=True, autoload_with=engine)
query = db.select([tweets.columns.tweet_text, tweets.columns.sentiment_score]
                  ).where(tweets.columns.sentiment_score >= 0.3)

while True:
    tweet = connection.execute(query).first()
    response = client.chat_postMessage(
        channel='#random', text=f"Here is a positive tweet about Berlin: {tweet}")
    time.sleep(300000)
