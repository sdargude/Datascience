import random
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StructType, StructField
import requests
from config import SENTIMENT_SERVER_URL, NEGATIVE, POSITIVE, NEUTRAL


def compute_tweet_sentiment(msg):
    parameters = {'tweet': msg}
    r = requests.get(url=SENTIMENT_SERVER_URL, params=parameters)
    sentiment = 1
    psentiment = 0
    ngsentiment = 0
    nsentiment = 0

    nltk_sentiment = 1
    nltk_psentiment = 0
    nltk_ngsentiment = 0
    nltk_nsentiment = 0

    if r.status_code == 200:
        data = r.json()
        sentiment = data['Sentiment']
        nltk_sentiment = data['Sentiment_nltk']

        if sentiment == 0:
            ngsentiment = 1
        elif sentiment == 1:
            nsentiment = 1
        elif sentiment == 2:
            psentiment = 1

        if nltk_sentiment == 0:
            nltk_ngsentiment = 1
        elif nltk_sentiment == 1:
            nltk_nsentiment = 1
        elif nltk_psentiment == 2:
            nltk_psentiment = 1

        print(data)

    return t.Row('sentiment', 'psentiment', 'ngsentiment', 'nsentiment',
                 'nltk_sentiment', 'nltk_psentiment', 'nltk_ngsentiment', 'nltk_nsentiment')(sentiment, psentiment,
                ngsentiment, nsentiment, nltk_sentiment, nltk_psentiment, nltk_ngsentiment, nltk_nsentiment)


# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,io.delta:delta-core_2.11:0.4.0 \
# ./SentimentProcessor.py
if __name__ == '__main__':
    findspark.init('/Users/sdargude/Playground/Spark/spark-2.4.5-bin-hadoop2.7')
    spark = SparkSession.builder.appName("SentimentProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    tweets = spark.readStream.format("delta").load("deltatables/raw_new")
    tweets.printSchema()

    tweets.createOrReplaceTempView("tweettable")
    df = spark.sql(
        "SELECT id as tweetid, sent_by as friendname, friend_of as profilename, \
         full_text as text, date  FROM  tweettable")

    schema = StructType([
        StructField("sentiment", IntegerType(), True),
        StructField("psentiment", IntegerType(), True),
        StructField("ngsentiment", IntegerType(), True),
        StructField("nsentiment", IntegerType(), True),
        StructField("nltk_sentiment", IntegerType(), True),
        StructField("nltk_psentiment", IntegerType(), True),
        StructField("nltk_ngsentiment", IntegerType(), True),
        StructField("nltk_nsentiment", IntegerType(), True)
    ])

    test_udf = udf(lambda text: compute_tweet_sentiment(text), schema)
    new_df = df.withColumn('Sentiment', test_udf(df.text))

    new_df.printSchema()

    new_df.writeStream.outputMode('append') \
        .format('delta') \
        .option("checkpointLocation", "deltatables/checkpoint_processed_new") \
        .trigger(processingTime='5 seconds') \
        .start('deltatables/processed_new') \
        .awaitTermination()