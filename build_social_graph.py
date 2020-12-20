# import the neo4j driver for Python
from neo4j import GraphDatabase

import argparse
import gzip
import json
import os
import signal
import sys

assert sys.version_info >= (3, 6)

import redis
import tweepy
import toml

parser = argparse.ArgumentParser()
parser.add_argument(
    "user_id_path", help="path to text file containing twitter user ids - 1 per line"
)
parser.add_argument(
    "credentials", help="name of the table containing the twitter credentials"
)
args = parser.parse_args()
user_id_path = args.user_id_path

if not os.path.isfile(user_id_path):
    sys.exit(f"{user_id_path} does not exist!")

if not os.path.isfile("config.toml"):
    sys.exit("Twitter credentials missing. You must have a config.toml file.")

config = toml.load("config.toml")

if args.credentials not in config:
    sys.exit(f"{args.credentials} is not a valid table name in config.toml")

credentials = config[args.credentials]

if "neo4j_database" not in config:
    sys.exit(f"Missing neo4j database credentials in config.toml")

uri = f"bolt://{config['neo4j_database']['host']}:{config['neo4j_database']['port']}"
userName = config['neo4j_database']["username"]
password = config['neo4j_database']["password"]


def retrieved_followers(user_id):
    return red.sismember("retrieved_followers", user_id)


def retrieved_friends(user_id):
    return red.sismember("retrieved_friends", user_id)


def deleted_or_protected(user_id):
    return red.sismember("deleted_user", user_id)


def processed(user_id):
    return (
        retrieved_followers(user_id)
        or retrieved_friends(user_id)
        or deleted_or_protected(user_id)
    )


def SIGINT_handler(signum, frame):
    print("It's quitin' time...soon...ish")
    global quitin_time
    quitin_time = True


quitin_time = False
signal.signal(signal.SIGINT, SIGINT_handler)

timelines_already_collected = True

TWITTER_CONSUMER_KEY = credentials["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = credentials["TWITTER_CONSUMER_SECRET"]
TWITTER_ACCESS_TOKEN_KEY = credentials["TWITTER_ACCESS_TOKEN_KEY"]
TWITTER_ACCESS_TOKEN_SECRET = credentials["TWITTER_ACCESS_TOKEN_SECRET"]

if "" in [
    TWITTER_CONSUMER_KEY,
    TWITTER_CONSUMER_SECRET,
    TWITTER_ACCESS_TOKEN_KEY,
    TWITTER_ACCESS_TOKEN_SECRET,
]:
    sys.exit("Missing Twitter API credentials")

auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
auth.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)

api = tweepy.API(
    auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
)

red = redis.Redis(host="127.0.0.1")
graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))

i = 0

with open(user_id_path, "r") as user_ids, graphDB_Driver.session() as graphDB_Session:
    for user_id in user_ids:
        user_id = user_id.strip()
        if processed(user_id):
            continue
        try:
            for follower_ids in tweepy.Cursor(
                api.followers_ids, user_id=user_id
            ).pages():
                query = f"""merge (user:User {{id: "{user_id}"}})\nwith user\nunwind {follower_ids} as follower_id\nmerge (follower:User {{id: follower_id}})\nmerge (follower)-[:FOLLOWS]->(user)"""
                graphDB_Session.run(query)
            red.sadd('retrieved_followers', user_id)

            for friend_ids in tweepy.Cursor(
                api.friends_ids, user_id=user_id
            ).pages():
                query = f"""merge (user:User {{id: "{user_id}"}})\nwith user\nunwind {friend_ids} as friend_id\nmerge (friend:User {{id: friend_id}})\nmerge (user)-[:FOLLOWS]->(friend)"""
                graphDB_Session.run(query)
            red.sadd('retrieved_friends', user_id)
        except:
            red.sadd("deleted_user", user_id)
        i += 1
        if quitin_time:
            break
        if i % 100 == 0:
            print(f"{i} users processed")

print("complete")
