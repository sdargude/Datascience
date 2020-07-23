import random
import re
import json
import nltk
import spacy
import tensorflow as tf
import numpy as np
from flask import Flask
from flask import request
from textblob import TextBlob
import re
from textblob import TextBlob
from config import NEGATIVE, POSITIVE, NEUTRAL

nlp = spacy.load('en')
wn = nltk.WordNetLemmatizer()
ps = nltk.PorterStemmer()
stopword = nltk.corpus.stopwords.words('english')
tf.keras.backend.clear_session()
max_tweet_length = 10
model_recreated = None


def clean_data(data):
    """
     Removes punctuation and return lower case string.
    """
    global ps
    global wn
    global np
    global stopword

    if not isinstance(data, str):
        return None
    try:
        data = re.sub(r'^http.?:\/\/.*[\r\n]*', '', data, flags=re.MULTILINE)
        no_punctuation = str(nlp(data).text)
        no_numbers = re.sub('[0-9]+', '', no_punctuation)
        tokenize = re.split('\W+', no_numbers)
        no_stopwords = [str.lower(word) for word in tokenize if word not in stopword]
        stemming = [ps.stem(word) for word in no_stopwords]
        lemmatize = [wn.lemmatize(str(word)) for word in stemming]
    except Exception as e:
        print(e)
        return None
    return " ".join(lemmatize)


def get_general_wordset():
    """
    Load Word Vectors from glove
    :return:
    """
    wordsList = np.load('glove_twitter/wordsList.npy')
    print('Loaded the word list!')
    wordsList = wordsList.tolist()  # Originally loaded as numpy array
    wordsList = [word.decode('UTF-8') for word in wordsList]  # Encode words as UTF-8
    wordVectors = np.load('glove_twitter/wordVectors.npy')
    print('Loaded the word vectors!')
    return wordsList, wordVectors


wordsList, wordVectors = get_general_wordset()


def vectorize(tweet):
    global max_tweet_length
    global wordsList

    tweetid = [0] * max_tweet_length
    indexCounter = 0
    for word in tweet.split(" "):
        if indexCounter < max_tweet_length:
            try:
                tweetid[indexCounter] = wordsList.index(word)
            except ValueError:
                tweetid[indexCounter] = 399999  # Vector for unknown words
        indexCounter = indexCounter + 1

    return tweetid


def count_unknown_words(tweet):
    count = 0
    for i in tweet:
        if i == 399999:
            count += 1
    return count


def compute_tweet_sentiment(msg):
    msg = clean_data(msg)
    if msg is None:
        return 0
    msg = vectorize(msg)
    msg = np.asarray(msg)
    msg = msg.reshape(1, 10)
    prediction = model_recreated.predict(msg)
    return prediction


app = Flask(__name__)


@app.route('/sentiment')
def hello_word():
    msg = request.args.get('tweet')
    if msg is None:
        return {0}
    prediction = compute_tweet_sentiment(msg).tolist()
    value = prediction[0].index(max(prediction[0]))
    obj = TextBlob(msg)
    nltk_sentiment = NEUTRAL
    if obj.sentiment.polarity < - 0.5:
        nltk_sentiment = NEGATIVE
    elif - 0.5 <= obj.sentiment.polarity <= 0.5:
        nltk_sentiment = NEUTRAL
    else:
        nltk_sentiment = POSITIVE

    print(prediction, value)
    return {"Sentiment": value, "Sentiment_nltk": nltk_sentiment}


with open('./model.json') as fp:
    model_json = json.load(fp)

model_recreated = tf.keras.models.model_from_json(json.dumps(model_json))
model_recreated.summary()
app.run(debug=True)

# Movie was beautiful awesome very good incredible well deserved
# Movie was boring  awefully bad and does deservice any ratings.
