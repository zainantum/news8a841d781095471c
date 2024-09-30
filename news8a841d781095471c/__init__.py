import aiohttp
import asyncio
import hashlib
import logging
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator
import tldextract as tld
import random
import base64
import json

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
)

DEFAULT_OLDNESS_SECONDS = 3600*3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10

async def fetch_data(url, headers=None):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                if response.status == 200:
                    json_data = await response.json(content_type=None)  # Manually handle content type
                    return json_data
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = 1800
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
    return max_oldness_seconds, maximum_items_to_collect, min_post_length

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    data = await fetch_data(feed_url)

    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
    yielded_items = 0
    logging.info(f"[News stream collector] Return from {feed_url} : {len(data)}")
    sorted_data = sorted(data, key=lambda x: x["pubDate"], reverse=True)
    logging.info(f"[News stream collector] Data sorted length : {len(sorted_data)}")
    sorted_data = [entry for entry in sorted_data if is_within_timeframe_seconds(convert_to_standard_timezone(entry["pubDate"]), max_oldness_seconds)]
    logging.info(f"[News stream collector] Filtered data time seconds : {len(sorted_data)}")

    sorted_data = random.sample(sorted_data, int(len(sorted_data) * 0.75))

    successive_old_entries = 0

    # Define async processing for each entry
    async def process_entry(entry):
        nonlocal yielded_items, successive_old_entries
        if yielded_items >= maximum_items_to_collect:
            return None

        # if random.random() < 0.25:
        #     return None

        logging.info(f"[News stream collector] Processing entry: {entry['title']} - {entry['pubDate']} - {entry['source_url']}")

        pub_date = convert_to_standard_timezone(entry["pubDate"])

        if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
            sha1 = hashlib.sha1()
            author = entry["creator"][0] if entry.get("creator") else "anonymous"
            sha1.update(author.encode())
            author_sha1_hex = sha1.hexdigest()

            content_article_str = entry.get("content") or entry.get("description") or entry["title"]
            domain_str = tld.extract(entry.get("source_url", "unknown")).registered_domain

            new_item = Item(
                content=Content(str(content_article_str)),
                author=Author(str(author_sha1_hex)),
                created_at=CreatedAt(pub_date),
                title=Title(entry["title"]),
                domain=Domain(str(domain_str)),
                url=Url(entry["link"]),
                external_id=ExternalId(entry["article_id"])
            )

            yielded_items += 1
            return new_item
        else:
            dt = datett.strptime(pub_date, "%Y-%m-%dT%H:%M:%S.00Z").replace(tzinfo=timezone.utc)
            current_dt = datett.now(timezone.utc)
            time_diff = current_dt - dt
            logging.info(f"[News stream collector] Entry is {abs(time_diff)} old: skipping.")

            successive_old_entries += 1
            if successive_old_entries >= 5:
                logging.info(f"[News stream collector] Too many old entries. Stopping.")
                return None

        return None

    # Create tasks for entries and run them concurrently
    tasks = [process_entry(entry) for entry in sorted_data]
    results = await asyncio.gather(*tasks)

    # Yield valid results only
    for item in results:
        if item is not None:
            yield item

    logging.info(f"[News stream collector] Done.")
