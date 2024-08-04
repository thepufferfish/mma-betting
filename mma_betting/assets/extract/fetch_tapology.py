from dagster import op, asset
from .tapology_scraper.run_tapology_spider import run_tapology_spider
import json

@op
def crawl_tapology():
    run_tapology_spider()

@asset
def fetch_events():
    events = []
    with open('tapology_scraper/events.jsonl', 'r') as f:
        for line in f:
            events.append(json.loads(line))
    return events

@asset
def fetch_fights():
    fights = []
    with open('tapology_scraper/fights.jsonl', 'r') as f:
        for line in f:
            fights.append(json.loads(line))
    return fights