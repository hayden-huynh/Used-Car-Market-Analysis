import scrapy


class SpideroneSpider(scrapy.Spider):
    name = "SpiderOne"
    # allowed_domains = ["example.com"]
    start_urls = ["https://example.com"]

    def parse(self, response):
        pass
