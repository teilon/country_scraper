# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3
import logging
from itemadapter import ItemAdapter

CREATE_TABLE_COUNTRIES = '''
'''
CREATE_TABLE_CITIES = '''
'''
CREATE_TABLE_REGION = '''
'''
INSERT_COUNTRY = '''
'''
INSERT_CITY = '''
'''
INSERT_REGION = '''
'''

class DB_Counties_Pipeline(object):

    def open_spider(self, spider):
        self.connection = sqlite3.connect('data.db')
        self.c = self.connection.cursor()
        try:
            self.c.execute(CREATE_TABLE_COUNTRIES)
            self.connection.commit()
        except sqlite3.OpertionalError:
            logging.warning("Table already exists")
    
    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        self.c.execute(INSERT_COUNTRY, (
            item.get('name'),
            item.get('population'),
            item.get('land_area'),
            item.get('migrants'),
            item.get('medium_age'),
            item.get('urban_pop'),
        ))
        self.connection.commit()
        return item

class DB_Cities_Pipeline(object):

    def open_spider(self, spider):
        self.connection = sqlite3.connect('data.db')
        self.c = self.connection.cursor()
        try:
            self.c.execute(CREATE_TABLE_CITIES)
            self.connection.commit()
        except sqlite3.OpertionalError:
            logging.warning("Table already exists")
    
    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        self.c.execute(INSERT_CITY, (
            item.get('name'),
            item.get('population'),
            item.get('country_name'),
        ))
        self.connection.commit()
        return item

class DB_Region_Pipeline(object):

    def open_spider(self, spider):
        self.connection = sqlite3.connect('data.db')
        self.c = self.connection.cursor()
        try:
            self.c.execute(CREATE_TABLE_REGION)
            self.connection.commit()
        except sqlite3.OpertionalError:
            logging.warning("Table already exists")
    
    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        self.c.execute(INSERT_REGION, (
            item.get('name'),
            item.get('country_name'),
            item.get('city_name'),
        ))
        self.connection.commit()
        return item