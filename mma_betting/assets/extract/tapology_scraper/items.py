# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class EventItem(scrapy.Item):
    id = scrapy.Field()
    datetime = scrapy.Field()
    location = scrapy.Field()
    venue = scrapy.Field()
    promotion = scrapy.Field()

class FightItem(scrapy.Item):
    id = scrapy.Field()
    event_id = scrapy.Field()
    division = scrapy.Field()
    sport = scrapy.Field()
    duration = scrapy.Field()
    weightclass = scrapy.Field()
    fighters = scrapy.Field()
