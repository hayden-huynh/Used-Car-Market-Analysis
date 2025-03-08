import scrapy


class CarGurusSpecSpider(scrapy.Spider):
    name = "CarGurusSpec"

    start_urls = ["https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=&zip=75081"]

    def parse(self, response):
        
        pass
