import scrapy
import json

class TransactionsSpider(scrapy.spiders.CrawlSpider):
    name = "transactions"

    def start_requests(self):
        url = "https://hkapi.centanet.com/api/Transaction/Map.json" 

        page = 1

        headers = {
            'lang': 'tc',
            'Content-Type': 'application/json; charset=UTF-8',
            'Connection': 'Keep-Alive',
            'User-Agent': 'okhttp/4.7.2' 
        }

        payload = {
            "daterange": 180,
            "postType": "s",
            "refdate": "20200701",
            "order": "desc",
            "page": f"{page}",
            "pageSize": 100,
            "pixelHeight": 2220,
            "pixelWidth": 1080,
            "points[0].lat": 22.695053063373795,
            "points[0].lng": 113.85844465345144,
            "points[1].lat": 22.695053063373795,
            "points[1].lng": 114.38281349837781,
            "points[2].lat": 21.993328259196705,
            "points[2].lng": 114.38281349837781,
            "points[3].lat": 21.993328259196705,
            "points[3].lng": 113.85844465345144,
            "sort": "score",
            "zoom": 9.745128631591797,
            "platform": "android"
        }

        yield scrapy.Request(url, callback=self.parse, method="POST", headers=headers, body=json.dumps(payload))

    def parse(self, response):
        json_response = json.loads(response.text)
        yield json_response