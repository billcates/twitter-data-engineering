import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import boto3


# config = Config(
#    signature_version = 's3v4'
# )

# s3 = boto3.resource('s3',
#                     aws_access_key_id='AKIAWBHVBUPBHAUQXNHO',
#                     aws_secret_access_key='SXUBiFC9fCFb1FRlTU6SHbsbRvHOLht26',
#                     config=config)

def twitter_etl():
    consumer_key="ACLkkxhDuQEj9dDIQgYXrWbZV"
    consumer_secret="dO173BoRd4qfORf8FJABZegmJmJucVGEpXvIuhftiX82SOj9n5"
    access_key="903938718-TEXOafWflwLmYb1NNJjCEhkOqblonxL6Ei94OsNQ"
    access_secret="aQOre7UB7I5hRl5wQLEuJkliYPQbtUMCmC6DoazFvP1Wj"

    #create connection with twitter api
    auth=tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_key,access_secret)

    #creating an API object
    api=tweepy.API(auth)

    tweets=api.user_timeline(screen_name="@chennaiipl",
                            count=200,
                            include_rts=False,
                            tweet_mode='extended')
    lt=[]
    for tweet in tweets:
        text=tweet._json['full_text']
        refined_tweet={
            "user":tweet.user.screen_name,
            "text":text,
            "favorite_count":tweet.favorite_count,
            "retweet_count":tweet.retweet_count,
            "created_at":tweet.created_at
        }
        lt.append(refined_tweet)
    #print(lt[3]['text'])

    df=pd.DataFrame(lt)
    df.to_csv("chennai_ipl_data.csv")
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('chennai_ipl_data.csv', 's3-first-bucket-billy', 'chennaiipl_tweets.csv')

twitter_etl()