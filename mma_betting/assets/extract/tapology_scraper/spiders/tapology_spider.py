import re
from scrapy.spiders import SitemapSpider
from pymongo import MongoClient
from datetime import datetime
from mma_betting.assets.extract.tapology_scraper.items import EventItem, FightItem

class TapologySpider(SitemapSpider):
    name = 'tapology'
    sitemap_urls = ['https://www.tapology.com/sitemap.xml']
    sitemap_follow = ['/events/sitemap', '/fighters/sitemap']
    sitemap_rules = [
        ('/events/', 'parse_event_page'),
        ('/fighters/', 'parse_fighter_page')
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(TapologySpider, cls).from_crawler(crawler, *args, **kwargs)
        spider.mongo_uri = crawler.settings.get('MONGO_URI')
        spider.mongo_db = crawler.settings.get('MONGO_DATABASE')
        return spider
    
    def sitemap_filter(self, entries):
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        collection = db['tapology_crawled_pages']

        for entry in entries:
            url = entry['loc']
            lastmod = entry.get('lastmod')

            if lastmod:
                lastmod_date = datetime.strptime(lastmod, '%Y-%m-%d')
                crawled_entry = collection.find_one({'url': url})
                now_date = datetime.utcnow()

                if crawled_entry:
                    if crawled_entry['lastmod'] < lastmod_date:
                        yield entry
                        collection.update_one({'url': url}, {'$set': {'lastmod': lastmod_date, 'last_crawled': now_date}})
                    else:
                        self.logger.debug(f'Skipping unchanged page: {url}')
                else:
                    yield entry
                    collection.insert_one({'url': url, 'lastmod': lastmod_date, 'last_crawled': now_date})
            else:
                # just going to follow but not record if no lastmod
                # these are often just links to sitemap pages
                yield entry

        client.close()

    def parse_event_page(self, response):
        yield EventItem(
            event_id=self.get_event_id_from_url(response.url),
            datetime=self.get_event_detail(response, 'Date/Time'),
            location=self.get_event_detail(response, 'Location'),
            venue=self.get_event_detail(response, 'Venue'),
            promotion=self.get_event_detail(response, 'Promotion')
        )

    def parse_fighter_page(self, response):
        matches = response.css('#proResults,#amResults').css('li')
        fighter = {
            'fighter_id': self.get_fighter_id_from_url(response.url),
            'name': self.get_fighter_attr(response, 'Name')
        }
        for match in matches:
            result = match.xpath('@data-status').get()
            fighter['result'] = result
            opponent_info = match.css('.opponent .name a')
            opponent_href = opponent_info.xpath('@href').get()
            opponent_name = opponent_info.css('::text').get()
            if result in ['unknown', 'cancelled'] or not result:
                self.logger.debug(f'Weird result: {result} in match against {opponent_name}')
                continue
            opponent = {
                'fighter_id': self.get_fighter_id_from_url(opponent_href),
                'name': opponent_name,
                'result': 'loss' if result == 'win' else 'win'
            }
            yield FightItem(
                fight_id=match.xpath('@data-bout-id').get(),
                event_id=self.get_event_id_from_url(match.xpath('//a[@title="Event Page"]/@href').get()),
                division=match.xpath('@data-division').get(),
                sport=match.xpath('@data-sport').get(),
                duration=self.get_fight_detail(match, 'Duration'),
                weightclass=self.get_fight_detail(match, 'Weight'),
                fighters=sorted([fighter, opponent], key=lambda x: x['fighter_id'])
            )

    def get_fighter_id_from_url(self, url):
        if url:
            m = re.search(r'(?<=fightcenter/fighters/)[0-9]+(?=-)', url)
            if m:
                return m.group()
            else:
                return url
        return ''
    
    def get_event_id_from_url(self, url):
        if url:
            m = re.search(r'(?<=fightcenter/events/)[0-9]+(?=-)', url)
            if m:
                return m.group()
            else:
                return url
        return ''

    def get_fighter_attr(self, response, attr):
        attr = attr + ':'
        attr_labels = response.css('#stats ul li strong::text').getall()
        attrs = response.css('#stats ul li span::text').getall()
        idx = [i for i, item in enumerate(attr_labels) if re.search(re.compile(attr), item)]
        if idx:
            return attrs[idx[0]].strip()
        return None
    
    def get_fight_detail(self, response, detail):
        detail = detail + ':'
        detail_labels = response.css('.details div .label::text').getall()
        details = response.css('.details div span:not(.label)::text').getall()
        if detail in detail_labels:
            idx = detail_labels.index(detail)
            return details[idx].strip()
        return None
    
    def get_event_detail(self, response, detail):
        detail = detail + ':'
        detail_labels = response.xpath('//ul[@data-controller="unordered-list-background"]')[0].css('li span.font-bold::text').getall()
        details = response.xpath('//ul[@data-controller="unordered-list-background"]')[0].css('li span.text-neutral-700')
        if detail in detail_labels:
            idx = detail_labels.index(detail)
            span = details[idx].css('::text').get().strip()
            a = details[idx].css('a::text').get().strip()
            return a if a else span
        return None
