import socket
import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
import json
import time
            
#Stream Listener Class ---> http://docs.tweepy.org/en/latest/streaming_how_to.html
# In Tweepy, an instance of tweepy.Stream establishes a streaming session and routes messages to StreamListener instance. 
# The on_data method of a stream listener receives all messages and calls functions according to the message type.
class TweetsListener(StreamListener):

    def __init__(self, connection):
        self.connection = connection


    def on_data(self, data):
        try:
            print("______________________________________________________________") 
            tweet = json.loads(data)
            text=tweet['text']
            out=bytearray(data, 'utf-8') # convert tweet in a bytearray object
                
            print(text)

            self.connection.send("Twitter_api", out) # publish tweets on the Twitter_api queue

            self.connection.flush()

        except BaseException as e:
            print("Error: ", e)
            pass

        return True


    def on_error(self, status):
        print(status)
        return True

if __name__ == "__main__":

    # twitter authentication
    consumer_key = "consumer_key"
    consumer_secret = "consumer_secret"
    access_token = "access_token"
    access_token_secret = "access_token_secret"
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # definition of a kafka producer to publish the data
    conn = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Establishing the twitter stream
    twitter_stream = Stream(auth, TweetsListener(conn), tweet_mode="extended_tweet", verify = False)

    # filters tweets by selecting the English and Italian language ones related to the coronavirus
    twitter_stream.filter(languages=['it','en'],track=["Coronavirus"])
