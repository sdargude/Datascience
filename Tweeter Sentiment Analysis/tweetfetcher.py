"""
Fetch tweets from friends.
"""
import datetime
import json
import socket
import sys

import numpy as np
import pandas as pd
from confluent_kafka import Producer
from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler

import mysecrets


class KafkaProducer:
    """
        Kafka Producer.
    """

    def __init__(self, conf):
        """
            Initialize class variables.
        """
        self.conf = conf
        self.producer = Producer(conf)

    def delivery_callback(self, err, msg):
        """
            Callback function for message.
        """
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def producer_message(self, topic, message):
        """
        Produce a kafka message.
        :param topic:
        :param message:
        :return:
        """
        self.producer.produce(topic, message, callback=self.delivery_callback)
        self.producer.poll(0.5)

    def flush(self):
        """
        Flush the kafka batch buffer.
        :return:
        """
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush(10)


class TwitterAuthenticator:
    """
        Authenticates twitter account
    """

    def authenticate_tweeter_app(self):
        """
        Sets the authentication.
        :return:
        """
        auth = OAuthHandler(mysecrets.CONSUMER_KEY, mysecrets.CONSUMER_SECRET)
        auth.set_access_token(mysecrets.ACCESS_TOKEN, mysecrets.ACCESS_TOKEN_SECRETS)
        return auth


class TweetAnalyzer:
    """
        Functionality to analyze and categorize the tweets.
    """

    def tweets_to_data_frame(self, tweets, sent_by, friend_of):
        """
        Converts tweets to dataframe
        :param friend_of:
        :param sent_by:
        :param tweets:
        :return:
        """
        tweets_df = pd.DataFrame(data=[tweet.full_text for tweet in tweets], columns=['Tweets'])
        tweets_df['id'] = np.array([tweet.id for tweet in tweets])
        tweets_df['full_text'] = np.array([tweet.full_text for tweet in tweets])
        tweets_df['len'] = np.array([len(tweet.full_text) for tweet in tweets])
        tweets_df['in_reply_to_status_id'] = np.array([tweet.in_reply_to_status_id for tweet in tweets])
        tweets_df['date'] = np.array([tweet.created_at for tweet in tweets])
        tweets_df['source'] = np.array([tweet.source for tweet in tweets])
        tweets_df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        tweets_df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])
        hash_tag = ""
        try:
            for ht in tweets['entities']['hashtags']:
                hash_tag = hash_tag + " " + ht['text']
        except Exception:
            pass
        tweets_df['hash_tag'] = hash_tag
        tweets_df['sent_by'] = sent_by
        tweets_df['friend_of'] = friend_of
        return tweets_df

    def send_to_kafka(self, tweets, kafka, sent_by, friend_of, topic="twitter"):
        """
        Produce messages on kafka.
        :param sent_by:
        :param tweets:
        :param kafka:
        :param sent_by:
        :param friend_of:
        :param topic:
        :return:
        """
        for tweet in tweets:
            message = {}
            message['id'] = tweet.id
            message['full_text'] = tweet.full_text
            message['len'] = len(tweet.full_text)
            message['in_reply_to_status_id'] = tweet.in_reply_to_status_id
            message['date'] = tweet.created_at.__str__()
            message['source'] = tweet.source
            message['likes'] = tweet.favorite_count
            message['retweet'] = tweet.retweet_count
            message['sent_by'] = sent_by
            message['friend_of'] = friend_of
            hash_tag = ""
            for ht in tweet.entities['hashtags']:
                hash_tag = hash_tag + " " + ht['text']

            message['hash_tag'] = hash_tag
            message = json.dumps(message)

            kafka.producer_message(topic, message)
        kafka.flush()

    def friends_list(self, friends, screen_name):
        """
        creates a dataframe of friends.
        :param friends:
        :param screen_name:
        :return:
        """
        friends_df = pd.DataFrame(data=[friend.screen_name for friend in friends],
                                  columns=['screen_name'])
        friends_df['followers_count'] = np.array([friend.followers_count for friend in friends])
        friends_df['friends_count'] = np.array([friend.friends_count for friend in friends])
        friends_df['friend_of'] = screen_name
        return friends_df


class TwitterClient:
    """
    Gets tweets from a time line.
    """

    def __init__(self):
        """
        initialize and authenticate twitter api.
        """
        self.auth = TwitterAuthenticator().authenticate_tweeter_app()
        self.twitter_client = \
            API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_twitter_client_api(self):
        """
        get api handle
        """
        return self.twitter_client

    def get_user_timeline_tweets(self, screen_name, num_tweets=25):
        """
        Get timeline tweets
        """
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline,
                            screen_name=screen_name, tweet_mode='extended').items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, screen_name, num_friends=25):
        """
        get friend list of a twitter user.
        """
        friend_list = []
        for friend in Cursor(self.twitter_client.friends,
                             screen_name=screen_name).items(num_friends):
            friend_list.append(friend)
        return friend_list


def run(screen_name=None):
    """
    Orchestrates the flow.
    :param screen_name:
    :return:
    """
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}

    twitter_client = TwitterClient()
    twitter_analyzer = TweetAnalyzer()
    kafka_producers = KafkaProducer(conf)
    twitter_client.get_twitter_client_api()

    friends_list = twitter_client.get_friend_list(screen_name=screen_name, num_friends=50)
    friend_df = twitter_analyzer.friends_list(friends_list, screen_name=screen_name)

    twitter_df = pd.DataFrame()

    for _, row in friend_df.iterrows():
        print("Fetching timeline of user {0}".format(row.screen_name))
        tweets = twitter_client.get_user_timeline_tweets(row.screen_name)
        df = twitter_analyzer.tweets_to_data_frame(tweets, row.screen_name, screen_name)
        twitter_analyzer.send_to_kafka(tweets, kafka_producers, row.screen_name, screen_name)
        twitter_df = twitter_df.append(df, ignore_index=True)

    tweets = twitter_client.get_user_timeline_tweets(screen_name)
    df = twitter_analyzer.tweets_to_data_frame(tweets, screen_name, screen_name)
    print(df.head(5))
    twitter_analyzer.send_to_kafka(tweets, kafka_producers, screen_name, screen_name)
    twitter_df = twitter_df.append(df, ignore_index=True)

    return friend_df, twitter_df


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


def run_from_file(screen_name=None):
    """
    Orchestrates the flow.
    :param screen_name:
    :return:
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname() + str(epoch)}

    kafka_producers = KafkaProducer(conf)

    tweets = pd.read_json("Json/" + screen_name + ".json")
    tweets.date = tweets.date.astype(str)

    for row in tweets.iterrows():
        message = row[1].to_dict()
        message = json.dumps(message, default=datetime_handler)
        kafka_producers.producer_message("twitter", message)

    kafka_producers.flush()
    return


def run_to_file(screen_name):
    try:
        friend_df, twitter_df = run(screen_name)
        run(screen_name)
    except ValueError as e: \
            print(str(e))

    twitter_df.to_json("Json/"+screen_name + ".json")


# Note: Set advertised.host.name=127.0.0.1
if __name__ == '__main__':
    #run_from_file('realDonaldTrump')
    #run_to_file('AskAnshul')
    #run_to_file('EnayetSpeaks')
    run_to_file('realDonaldTrump')
    #run_to_file('mjfree')