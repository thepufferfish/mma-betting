from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from mma_betting.assets.extract.tapology_scraper.spiders.tapology_spider import TapologySpider

tapology_settings = get_project_settings()

def run_tapology_spider():
    process = CrawlerProcess(settings=tapology_settings)
    process.crawl(TapologySpider)
    process.start()