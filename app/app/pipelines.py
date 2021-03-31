# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from app.items import CountryItem, CityItem, RegionItem

CREATE_TABLE_COUNTRIES = '''
    CREATE TABLE Countries(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        population TEXT,
        land_area TEXT,
        migrants TEXT,
        medium_age TEXT,
        urban_pop TEXT
    );
'''
CREATE_TABLE_CITIES = '''
    CREATE TABLE Cities(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_id INTEGER,
        name TEXT,
        population TEXT,
        FOREIGN KEY (country_id) REFERENCES Countries (id)
    );
'''
CREATE_TABLE_REGION = '''
    CREATE TABLE Regions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_id INTEGER,
        name TEXT,
        FOREIGN KEY (country_id) REFERENCES Countries (id)
    );
'''
INSERT_COUNTRY = '''
    INSERT INTO Countries (name, population, land_area, migrants, medium_age, urban_pop) 
    VALUES (?, ?, ?, ?, ?, ?);
'''
INSERT_CITY = '''
    INSERT INTO Cities (country_id, name, population) 
    VALUES (?, ?, ?);
'''
INSERT_REGION = '''
    INSERT INTO Regions (country_id, name) 
    VALUES (?, ?);
'''
SELECT_COUNTRY_ID = '''
    SELECT id FROM Countries WHERE name=?;
'''

class CountryPipline:

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
        if isinstance(item, CountryItem):
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

class CityPipline:

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
        if isinstance(item, CityItem):
            # get country id
            self.c.execute(SELECT_COUNTRY_ID, (item.get('country_name'),))
            country_id = self.c.fetchone()[0]

            # insert city
            self.c.execute(INSERT_CITY, (
                country_id,
                item.get('name'),
                item.get('population'),
            ))
            self.connection.commit()
        return item

class RegionPipeline:
    regions = []

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
        if isinstance(item, RegionItem):
            if item['name'] in self.regions:
                raise DropItem(f"Region {item['name']} already exists.")
            self.regions.append(item['name'])

            # get country id
            self.c.execute(SELECT_COUNTRY_ID, (item.get('country_name'),))
            country_id = self.c.fetchone()[0]

            # insert city
            self.c.execute(INSERT_REGION, (
                country_id,
                item.get('name'),
            ))
            self.connection.commit()
        return item