import json
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cluster import Cluster
import time
keyspace= "twitter"

# set up of the connection with the cassandra cluster
cluster = Cluster(['127.0.0.1'],port=9042)
session = cluster.connect(keyspace,wait_for_all_pools=True)

# extraction of the tweet entity
def compute_tweet(message):
    tweet= {}
    tweet['id'] = message['id']
    tweet['created_at'] = message['created_at']
    if ('retweeted_status' in message) and ('extended_tweet' in message['retweeted_status']):
        tweet['text'] = message['retweeted_status']['extended_tweet']['full_text']
    elif ('extended_status' in message):
        tweet['text'] = message['extended_status']['full_text']
    else:
        tweet['text'] = message['text']
    tweet['lang'] = message['lang']
    tweet['user_id'] = message['user']['id']

    return tweet

# writes the tweet entity into the database
def write_tweets(tweets):

    session.execute("USE " + keyspace)

    if len(tweets.collect())>0:
        
        for tweet in tweets.collect():
            tweet = json.loads(tweet)
            session.execute(
                """
                INSERT INTO Tweet (id, text, user_id, created_at, lang)
                VALUES (%s,  %s, %s, %s, %s)
                """,
            (tweet['id'], tweet['text'], tweet['user_id'], tweet['created_at'], tweet['lang'])
            )
           
# extraction of the user entity   
def compute_user(message):
    user = {}
    user_tweet = message['user']
    user['id'] = user_tweet['id']
    user['name'] = user_tweet['name']
    user['description'] = user_tweet['description']
    user['location'] = user_tweet['location']
    user['followers'] = user_tweet['followers_count']
    user['following'] = user_tweet['friends_count']
    user['created_at'] = user_tweet['created_at']
    
    return user

# writes the user entity into the database
def write_users(users):   
    
    if len(users.collect())>0:
      
        session.execute("USE " + keyspace)

        for user in users.collect():
            user = json.loads(user)
           
            # Check null values to avoid the creation of tombstones
            # Having an excessive number of tombstones in a table can negatively impact application performance.
            # Many tombstones often indicate potential issues with either the data model or in the application.
            if user['description'] != None and user['location'] != None:
                session.execute(
                """
                INSERT INTO User (id, name, description, location, followers, following, created_at)
                VALUES (%s,  %s, %s, %s, %s, %s, %s)
                """,
                (user['id'], user['name'], user['description'], user['location'], user['followers'], user['following'], user['created_at'])
                )
            elif user['description'] != None and user['location'] == None:
                session.execute(
                """
                INSERT INTO User (id, name, description, followers, following, created_at)
                VALUES (%s,  %s, %s, %s, %s, %s)
                """,
                (user['id'], user['name'], user['description'], user['followers'], user['following'], user['created_at'])
                )
            elif user['description'] == None and user['location'] != None:
                session.execute(
                """
                INSERT INTO User (id, name, location, followers, following, created_at)
                VALUES (%s,  %s, %s, %s, %s, %s)
                """,
                (user['id'], user['name'], user['location'], user['followers'], user['following'], user['created_at'])
                )
            elif user['description'] == None and user['location'] == None: 
                session.execute(
                """
                INSERT INTO User (id, name, followers, following, created_at)
                VALUES (%s,  %s, %s, %s, %s)
                """,
                (user['id'], user['name'], user['followers'], user['following'], user['created_at'])
                )