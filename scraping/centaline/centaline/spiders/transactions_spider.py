import scrapy
import json

class TransactionsSpider(scrapy.spiders.CrawlSpider):
    name = "transactions"
    url = "https://hkapi.centanet.com/api/Transaction/Map.json" 
    url_details = "https://hkapi.centanet.com/api/Transaction/Detail.json"
    headers = {
            'lang': 'tc',
            'Content-Type': 'application/json; charset=UTF-8',
            'Connection': 'Keep-Alive',
            'User-Agent': 'okhttp/4.7.2' 
        }
    first_payload = {
            "daterange": 180,
            "postType": "s",
            "refdate": "20200701",
            "order": "desc",
            "page": 1,
            "pageSize": 1,
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

    def start_requests(self):
        """
        Get the first page of transactions
        """
        yield scrapy.Request(self.url, callback=self.parse, method="POST", headers=self.headers, body=json.dumps(self.first_payload))

    def parse(self, response):
        """
        loop through all the pages
        """
        json_response = json.loads(response.text)

        total_pages = 1 #json_response['TransactionCount']//10000

        for i in range(1, total_pages+1):
            page = i
            payload = {
                "daterange": 180,
                "postType": "s",
                "refdate": "20200701",
                "order": "desc",
                "page": page,
                "pageSize": 2,
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
            yield scrapy.Request(self.url, callback=self.parse_secondary_requests, method="POST", headers=self.headers, body=json.dumps(payload), dont_filter=True)

    def parse_secondary_requests(self, response):
        """
        Parse transactions to get the transaction id to request transaction details
        """
        json_response = json.loads(response.text)
        yield json_response

        for transaction in json_response["AItems"]:
            transaction_id = transaction["TransactionID"]
            data_source = transaction["Data_Source"].lower()
            payload = {
                "data_source": f"{data_source}",
                "id": f"{transaction_id}",
                "platform": "android" 
            }
            yield scrapy.Request(self.url_details, callback=self.parse_transaction_details, method="POST", headers=self.headers, body=json.dumps(payload))

    def parse_transaction_details(self, response):
        json_response = json.loads(response.text)
        yield json_response

        