import twitter
import time
from datetime import datetime, timedelta
from .config import logger
import re

def create_twitter_instance():
    """Create Twitter API instance"""
    try:
        api = twitter.Api(
            consumer_key='{{ var.value.twitter_consumer_key }}' or '5Q3CFnvq02nKj6kI9gRpGNHXH',
            consumer_secret='{{ var.value.twitter_consumer_secret }}' or '4OBnuBjedjwZUZmtslwzzPmWxeQtN7LHUeYHf4jsqZjQkEyW4v',
            access_token_key='{{ var.value.twitter_access_token_key }}' or '907341293717737473-for4ikiKhPAHxD54pnRqhJSPpr1QmNB',
            access_token_secret='{{ var.value.twitter_access_token_secret }}' or 'jV6TplxXfCQOu8C8zArB2wzlwGisq2Y0kRHUtrvuKYQNr',
            sleep_on_rate_limit=True
        )
        return api
    except Exception as e:
        logger.error(f"Failed to create Twitter API instance: {e}")
        raise

def get_tweet_timestamp(tweet_id, twitter_object):
    """Get tweet timestamp using Twitter API"""
    try:
        twitter_response = twitter_object.GetStatus(tweet_id)
        tweet_date_time = datetime.strptime(twitter_response.created_at, "%a %b %d %H:%M:%S %z %Y")
        return int(tweet_date_time.timestamp())
    except Exception as e:
        logger.warning(f"API error for tweet_id {tweet_id}: {e}, retrying after 300s")
        time.sleep(300)
        return get_tweet_timestamp(tweet_id, twitter_object)

def find_tweet_timestamp(tweet_id):
    """Extract timestamp from Twitter Snowflake ID"""
    try:
        pre_snowflake_last_tweet_id = 29700859247
        if int(tweet_id) < pre_snowflake_last_tweet_id:
            return -1  # Placeholder for pre-Snowflake logic
        offset = 1288834974657  # Twitter epoch in milliseconds
        timestamp_ms = (int(tweet_id) >> 22) + offset
        timestamp_s = timestamp_ms / 1000.0
        tweet_datetime = datetime.fromtimestamp(timestamp_s)
        if tweet_datetime.year < 2010 or tweet_datetime.year > datetime.now().year + 1:
            logger.error(f"Invalid year {tweet_datetime.year} for tweet_id {tweet_id}")
            return -1
        return int(timestamp_s)
    except ValueError as ve:
        logger.error(f"ValueError in find_tweet_timestamp for tweet_id {tweet_id}: {ve}")
        return -1
    except Exception as e:
        logger.error(f"Error in find_tweet_timestamp for tweet_id {tweet_id}: {e}")
        return -1

def is_within_timeframe(timestamp, hours):
    """Check if timestamp is within the last N hours"""
    if timestamp is None or timestamp == -1:
        return False
    current_time = datetime.now()
    tweet_time = datetime.fromtimestamp(timestamp)
    time_diff = current_time - tweet_time
    return time_diff <= timedelta(hours=hours)

def extract_tweet_id_from_link(link):
    """Extract tweet ID from X/Twitter link"""
    pattern = r'(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/[^/]+/status/(\d+)'
    match = re.search(pattern, link)
    return match.group(1) if match else None