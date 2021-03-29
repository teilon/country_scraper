import scrapy


class PopCrawlerSpider(scrapy.Spider):
    name = 'pop_crawler'
    allowed_domains = ['worldometers.info']
    # start_urls = ['https://www.worldometers.info/world-population/population-by-country']

    def start_requests(self):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'}
        url = 'https://www.worldometers.info/world-population/population-by-country'

        yield scrapy.Request(url=url, callback=self.parse, headers=headers)

    def parse(self, response):
        trs = response.xpath("//table[@id='example2']/tbody/tr")
        for tr in trs:
            link = tr.xpath(".//td[2]/a/@href").get()
            name = tr.xpath(".//td[2]/a/text()").get()
            population = tr.xpath(".//td[3]/text()").get()
            land_area = tr.xpath(".//td[7]/text()").get()
            migrants = tr.xpath(".//td[8]/text()").get()
            medium_age = tr.xpath(".//td[10]/text()").get()
            urban_pop = tr.xpath(".//td[12]/text()").get()

            data = {
                'country_name': name,
                'population': population,
                'land_ area': land_area,
                'migrants': migrants,
                'medium_age': medium_age,
                'urban_pop': urban_pop,
            }

            # yield data

            headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'}
            yield response.follow(url=link, callback=self.parse_country, headers=headers, meta={'country_name': name})
    
    def parse_country(self, response):
        country_name = response.meta['country_name']

        # breadcrumbs
        breadcrumb = response.xpath("//ul[@class='breadcrumb']/li")
        breadcrumbs = [crumb.xpath(".//a/text()").get() for crumb in breadcrumb[3:-1]]

        # cities
        cities = response.xpath("//table[@class='table table-hover table-condensed table-list']/tbody/tr")
        for city in cities:
            name = city.xpath(".//td[2]/text()").get()
            population = city.xpath(".//td[3]/text()").get()

            data = {
                'country_name': country_name,
                'breadcrumbs': breadcrumbs,

                'city_name': name,
                'population': population,
            }

            yield data
