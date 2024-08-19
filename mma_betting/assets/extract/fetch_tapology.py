from dagster import op, asset
from .tapology_scraper.run_tapology_spider import run_tapology_spider
import json

@op
def crawl_tapology():
    run_tapology_spider()
