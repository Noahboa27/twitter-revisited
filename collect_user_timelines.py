# ./collect_user_timelines.py minutes_to_run

from tweet_collector import sort_tweet
from time import time
import os
import signal
import sys
import redis
import tweepy
import toml
import pymysql


def main():
    # toml must be named 'config.toml' and have a table named 'database' and 'credentials'
    if not os.path.isfile("config.toml"):
        sys.exit("Must have config.toml file with Twitter credentials and database connection info.")

    config = toml.load("config.toml")

    if ("database" not in config):
        sys.exit("Missing 'database' table in config.toml")

    if ("credentials" not in config):
        sys.exit("Missing 'credentials' table in config.toml")

    USERNAME = config["database"]["username"]
    PASSWORD = config["database"]["password"]
    HOSTADDR = config["database"]["hostaddr"]
    DATABASE_NAME = config["database"]["database_name"]

    # Fall back time for the rate limit
    # TIMEOUT_BACKOFF = 60

    TWITTER_CONSUMER_KEY = config["credentials"]["consumer_key"]
    TWITTER_CONSUMER_SECRET = config["credentials"]["consumer_secret"]
    TWITTER_ACCESS_TOKEN_KEY = config["credentials"]["access_token_key"]
    TWITTER_ACCESS_TOKEN_SECRET = config["credentials"]["access_token_secret"]

    auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)

    db = pymysql.connect(user=USERNAME, passwd=PASSWORD, host=HOSTADDR, 
                        database=DATABASE_NAME)

    # getting a tuple of all the user id's
    cursor = db.cursor()
    get_user_ids = ("SELECT id_str FROM Users")
    cursor.execute(get_user_ids)
    user_ids = cursor.fetchall()

    minutes_to_run = float(sys.argv[1])
    start_time = time()
    # time_string = ""
    seconds = minutes_to_run * 60

    def delete_user_obj(status):
        del status["user"]
        return status


    # def id_checked(user_id):
    #     return red.sismember("retrieved_200", user_id) or red.sismember(
    #         "deleted_user", user_id
    #     )


    def SIGINT_handler(signum, frame):
        # print("It's quitin' time...soon...ish")
        global quitin_time
        quitin_time = True


    quitin_time = False
    signal.signal(signal.SIGINT, SIGINT_handler)

    # timelines_already_collected = True


    auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)

    api = tweepy.API(
        auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
    )

    # red = redis.Redis(host="127.0.0.1")
    # current_file_num = int(red.get("last_file")) + 1
    # timelines_file = f"timelines-{current_file_num}.json.gz"

    i = 0

    # with gzip.open(timelines_file, "wb") as timelines, open(user_id_path, "r") as user_ids:
    #     red.set("last_file", f"{current_file_num}")
    # red.set()
    for user_id in user_ids:
        # user_id = user_id.strip()
        if (time() >= start_time + seconds):
            os.kill(os.getpid(), signal.SIGINT)
        # if id_checked(user_id):
        #     continue
        # timelines_already_collected = False
        try:
            statuses = api.user_timeline(user_id, count=200)
            for status in statuses:
                delete_user_obj(status)
                sort_tweet(status)
            # statuses = [delete_user_obj(status._json) for status in statuses]
            # data = {user_id: statuses}
            # timelines.write(f"{json.dumps(data)}\n".encode())
            # red.sadd("retrieved_200", user_id)
        except:
            continue
            # red.sadd("deleted_user", user_id)
        i += 1
        if quitin_time:
            cursor.close()
            db.close()
            sys.exit(0)
        if i % 100 == 0:
            print(f"{i} users retrieved")

    # if timelines_already_collected:
    #     os.remove(timelines_file)

    print("complete")

if __name__ == "__main__":
    main()
