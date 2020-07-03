import config
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import logging
from datetime import datetime
from pymongo import MongoClient

# connect to mongodb
client = MongoClient(host='mongodb', port=27017)
db = client.twitter
tweets_mongo = db.tweets


def authenticate():
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth


class TwitterListener(StreamListener):

    def on_data(self, data):
        """Whatever we put in this method defines what is done with
        every single tweet as it is intercepted in real-time"""

        t = json.loads(data)  # t is just a regular python dictionary.
        timeStamp = datetime.strptime(t['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        tweet = {
            'tweet_text': t['text'],
            'user_name': t['user']['screen_name'],
            'followers_count': t['user']['followers_count'],
            'time_stamp': timeStamp
        }
        tweets_mongo.insert(tweet)
        logging.critical('SUCCESSFULLY ADDED TO MONGODB!!!!!!')

    def on_error(self, status):

        if status == 420:
            print(status)
            return False


if __name__ == '__main__':

    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    stream.filter(track=['berlin'], languages=['en'])
