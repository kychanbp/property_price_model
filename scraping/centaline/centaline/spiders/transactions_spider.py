import scrapy
import json
import math
import pandas as pd
from datetime import datetime
import logging

class TransactionsSpider(scrapy.spiders.CrawlSpider):
    name = "transactions"
    url = "https://hkapi.centanet.com/api/Transaction/Map.json" 
    url_details = "https://hkapi.centanet.com/api/Transaction/Detail.json"
    page_size =  100
    periods = 2
    daterange = 180

    headers = {
            'lang': 'tc',
            'Content-Type': 'application/json; charset=UTF-8',
            'Connection': 'Keep-Alive',
            'User-Agent': 'okhttp/4.7.2' 
        }

    def start_requests(self):
        """
        loop through all the date range
        Get the first page of transactions
        """
        date_list = [d.strftime('%Y%m%d') for d in pd.date_range(datetime.today(), periods=self.periods, freq=f"-{self.daterange}D")]
        for date in date_list:
            first_payload = {
            "daterange": self.daterange,
            "postType": "s",
            "refdate": date,
            "order": "desc",
            "page": 1,
            "pageSize": self.page_size,
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
            yield scrapy.Request(self.url, callback=self.parse, method="POST", headers=self.headers, body=json.dumps(first_payload), meta={"refdate":date})

    def parse(self, response):
        """
        loop through all the pages
        """
        refdate = response.meta["refdate"]
        json_response = json.loads(response.text)

        total_pages = math.ceil(json_response['TransactionCount']/self.page_size)

        for i in range(1, total_pages+1):
            page = i
            payload = {
                "daterange": self.daterange,
                "postType": "s",
                "refdate": f"{refdate}",
                "order": "desc",
                "page": page,
                "pageSize": self.page_size,
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

        