# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3
import logging
from itemadapter import ItemAdapter
import scrapy
from scrapy.exceptions import DropItem
import requests
import json

from app.items import CountryItem, CityItem, RegionItem

# create table if not exists TableName (col1 typ1, ..., colN typN)

CREATE_TABLE_COUNTRIES = '''
    CREATE TABLE IF NOT EXISTS Countries(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        population TEXT,
        land_area TEXT,
        migrants TEXT,
        medium_age TEXT,
        urban_pop TEXT,
        timestamp DATE DEFAULT (datetime('now','localtime'))
    );
'''
CREATE_TABLE_CITIES = '''
    CREATE TABLE IF NOT EXISTS Cities(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_id INTEGER,
        name TEXT,
        population TEXT,
        timestamp DATE DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (country_id) 
          REFERENCES Countries (id)
            ON DELETE CASCADE 
            ON UPDATE NO ACTION
    );
'''
CREATE_TABLE_REGION = '''
    CREATE TABLE IF NOT EXISTS Regions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_id INTEGER,
        name TEXT UNIQUE,
        FOREIGN KEY (country_id) 
          REFERENCES Countries (id)
            ON DELETE CASCADE 
            ON UPDATE NO ACTION
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
    VALUES (?, ?) ON CONFLICT(name) DO NOTHING;
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

class CountrySenderPipline:
    url = 'http://{manager_host}/country/{country_name}'
    headers = {'Content-type': 'application/json',
               'Accept': 'text/plain',
               'Content-Encoding': 'utf-8'}

    def process_item(self, item, spider):
        if isinstance(item, CountryItem):
            current_url = self.url.format(manager_host='127.0.0.1:5080', country_name=item['name']) # os.environ['MANAGER_HOST']
            response = requests.post(current_url, data=json.dumps(item.json()), headers=self.headers)

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