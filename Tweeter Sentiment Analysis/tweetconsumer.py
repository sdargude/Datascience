import json
import sys
from random import randrange

import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import from_json, col


# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,io.delta:delta-core_2.11:0.4.0 \
#  tweetconsumer.py  localhost 9092 twitter


def get_sentiment(tweet="Default"):
    """
    udf to return the sentiment of the tweet.
    :return: -1 0 1 for Negative, Neutral and Positive sentiment.
    """
    tweet_json = tweet
    return t.Row('id', 'full_text', 'len', 'in_reply_to_status_id', 'date',
                 'source', 'likes', 'retweet', 'sent_by', 'friend_of', 'hash_tag') \
        (tweet_json['id'], tweet_json['full_text'], tweet_json['len'], tweet_json['in_reply_to_status_id'],
         tweet_json['date'], tweet_json['source'], tweet_json['likes'], tweet_json['retweet'],
         tweet_json['sent_by'], tweet_json['friend_of'], tweet_json['hash_tag'])


if __name__ == '__main__':
    findspark.init('/Users/sdargude/Playground/Spark/spark-2.4.5-bin-hadoop2.7')

    if len(sys.argv) != 4:
        print("Usage: spark-submit tweetconsumer.py <hostname> <port> <topic>")
        print("eg. spark-submit  --packages org.apache.spark:spark-sql\
        -kafka-0-10_2.11:2.4.5 tweetconsumer.py  localhost 9092 twitter")
        sys.exit(1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]
    connect_string = host + ":" + port

    spark = SparkSession.builder.appName("TweetConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = t.StructType() \
        .add("id", t.LongType()) \
        .add("full_text", t.StringType()) \
        .add("len", t.IntegerType()) \
        .add("in_reply_to_status_id", t.StringType()) \
        .add("date", t.StringType()) \
        .add("source", t.StringType()) \
        .add("likes", t.IntegerType()) \
        .add("retweet", t.IntegerType()) \
        .add("sent_by", t.StringType()) \
        .add("friend_of", t.StringType()) \
        .add("hash_tag", t.StringType()) \


    tweetsRawDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("tweet"))

    add_sentiment_score_udf = f.udf(get_sentiment, schema)
    newDF = tweetsRawDF.withColumn("output", add_sentiment_score_udf(tweetsRawDF.tweet))
    newDF = newDF.select("output.*")
    newDF.writeStream.outputMode('append') \
        .format('delta') \
        .option("checkpointLocation", "deltatables/checkpoint_new") \
        .trigger(processingTime='5 seconds') \
        .start('deltatables/raw_new') \
        .awaitTermination()
