import findspark
findspark.init()
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from utils_streaming import *
import json


if __name__ == "__main__":

    conf = SparkConf().set("spark.jars", "C:\spark\spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar")
    # Create a local SparkContext with as many worker threads as logical cores on your machine and a StreamingContext with batch interval of 10 seconds.
    sc = SparkContext(master = "local[*]", appName="Kafka_Twitter", conf=conf)
    sc.setLogLevel('ERROR')

    ssc = StreamingContext(sc, batchDuration=10)
    
    # creation of a direct streaming via kafka (create an input DStream)
    bootstrap_servers = str("localhost:9092") #Bootstrap Servers are a list of host/port pairs to use for establishing the initial connection with Kafka.
    # Topic is a category/feed name to which records are stored and published.
    kafka_stream = KafkaUtils.createDirectStream(ssc, topics=["Twitter_api"], kafkaParams={"metadata.broker.list": bootstrap_servers})

    # operations performed on record streams (are executed on all received batches)
    parsed = kafka_stream.map(lambda v: json.loads(v[1])) #Decoding JSON
    parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint() # counts the number of Tweets received in the batch
    users = parsed.map(lambda stream: json.dumps(compute_user(stream))) # extraction of the user entity
    #users.pprint()
    users.foreachRDD(write_users) # writes the user entity into the database
    tweets = parsed.map(lambda stream: json.dumps(compute_tweet(stream))) # extraction of the tweet entity
    #tweets.pprint()
    tweets.foreachRDD(write_tweets) # writes the tweet entity into the database
    
    print("Consumer Online!")
    ssc.start() # Start the computation

    ssc.awaitTermination() # Wait for the computation to terminate