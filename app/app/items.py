# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AppItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class CountryItem(scrapy.Item):
    name = scrapy.Field()
    population = scrapy.Field()
    land_area = scrapy.Field()
    migrants = scrapy.Field()
    medium_age = scrapy.Field()
    urban_pop = scrapy.Field()

    def json(self):
        return {
            "population": self['population'],
            "land_area": self['land_area'],
            "migrants": self['migrants'],
            "medium_age": self['medium_age'],
            "urban_pop": self['urban_pop']
            }

class CityItem(scrapy.Item):
    country_name = scrapy.Field()
    name = scrapy.Field()
    population = scrapy.Field()

class RegionItem(scrapy.Item):
    country_name = scrapy.Field()
    name = scrapy.Field()
