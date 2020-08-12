import scrapy
from ..items import CentalineTransactionsItem,CentalineTransactionsDetailItem
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
    daterange = 1

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
        transaction_items = CentalineTransactionsItem()
        
        json_response = json.loads(response.text)
        AItems = json_response['AItems']

        for transaction in AItems:
            transaction_items['TransactionID'] = transaction['TransactionID']
            transaction_items['IsRelated'] = transaction['IsRelated']
            transaction_items['RegDateString'] = transaction['RegDateString']
            transaction_items['CblgCode'] = transaction['CblgCode']
            transaction_items['CestCode'] = transaction['CestCode']
            transaction_items['Data_Source'] = transaction['Data_Source']
            transaction_items['Memorial'] = transaction['Memorial']
            transaction_items['RegDate'] = transaction['RegDate']
            transaction_items['InsDate'] = transaction['InsDate']
            transaction_items['PostType'] = transaction['PostType']
            transaction_items['Price'] = transaction['Price']
            transaction_items['Rental'] = transaction['Rental']
            transaction_items['RFT_NArea'] = transaction['RFT_NArea']
            transaction_items['RFT_UPrice'] = transaction['RFT_UPrice']
            transaction_items['INT_GArea'] = transaction['INT_GArea']
            transaction_items['INT_UPrice'] = transaction['INT_UPrice']
            transaction_items['CX'] = transaction['CX']
            transaction_items['CY'] = transaction['CY']
            transaction_items['c_estate'] = transaction['c_estate']
            transaction_items['c_phase'] = transaction['c_phase']
            transaction_items['c_property'] = transaction['c_property']
            transaction_items['scp_c'] = transaction['scp_c']
            transaction_items['scp_mkt'] = transaction['scp_mkt']
            transaction_items['pc_addr'] = transaction['pc_addr']

            yield transaction_items

        for transaction in AItems:
            transaction_id = transaction["TransactionID"]
            data_source = transaction["Data_Source"].lower()
            payload = {
                "data_source": f"{data_source}",
                "id": f"{transaction_id}",
                "platform": "android" 
            }
            yield scrapy.Request(self.url_details, callback=self.parse_transaction_details, method="POST", headers=self.headers, body=json.dumps(payload))

    def parse_transaction_details(self, response):
        transaction_detail_items = CentalineTransactionsDetailItem()
        json_response = json.loads(response.text)
        transaction = json_response['Transaction']
        property_info = json_response['PropertyInfo']
        HMA = json_response['HMA']

        transaction_detail_items['ID'] = transaction['ID']
        transaction_detail_items['Bigestcode'] = transaction['Bigestcode']
        transaction_detail_items['Cestcode'] = transaction['Cestcode']
        transaction_detail_items['Cblgcode'] = transaction['Cblgcode']
        transaction_detail_items['ThumbnailUrl'] = transaction['ThumbnailUrl']
        transaction_detail_items['Consider'] = transaction['Consider']
        transaction_detail_items['Elocation1']= transaction['Elocation1']
        transaction_detail_items['X_Axis'] = transaction['X_Axis']
        transaction_detail_items['Y_Axis'] = transaction['Y_Axis']
        transaction_detail_items['CX_Axis'] = transaction['CX_Axis']
        transaction_detail_items['CY_Axis'] = transaction['CY_Axis']
        transaction_detail_items['Rft_NArea'] = transaction['Rft_NArea']
        transaction_detail_items['Rft_UPrice'] = transaction['Rft_UPrice']
        transaction_detail_items['Int_Garea'] = transaction['Int_Garea']
        transaction_detail_items['Int_Uprice'] = transaction['Int_Uprice']
        transaction_detail_items['Memorial'] = transaction['Memorial']
        transaction_detail_items['Reg_Date'] = transaction['Reg_Date']
        transaction_detail_items['Ins_Date'] = transaction['Ins_Date']
        transaction_detail_items['Int_BlgAge'] = transaction['Int_BlgAge']
        transaction_detail_items['RegAddr'] = transaction['RegAddr']
        transaction_detail_items['Data_Source'] = transaction['Data_Source']
        transaction_detail_items['PostType'] = transaction['PostType']
        transaction_detail_items['NameTC'] = transaction['NameTC']

        transaction_detail_items['scp_c'] = property_info['scp_c']
        transaction_detail_items['scp_e'] = property_info['scp_e']
        transaction_detail_items['c_estate'] = property_info['c_estate']
        transaction_detail_items['e_estate'] = property_info['e_estate']
        transaction_detail_items['c_phase'] = property_info['c_phase']
        transaction_detail_items['e_phase'] = property_info['e_phase']
        transaction_detail_items['c_property'] = property_info['c_property']
        transaction_detail_items['e_property'] = property_info['e_property']
        transaction_detail_items['pc_addr'] = property_info['pc_addr']
        transaction_detail_items['pe_addr'] = property_info['pe_addr']
        transaction_detail_items['Est_Type'] = property_info['Est_Type']

        transaction_detail_items['HMA_type'] = HMA['type']
        transaction_detail_items['code'] = HMA['code']
        transaction_detail_items['est_type'] = HMA['est_type']
        transaction_detail_items['dbcode'] = HMA['dbcode']
        transaction_detail_items['dbc'] = HMA['dbc']
        transaction_detail_items['dbe'] = HMA['dbe']
        transaction_detail_items['bldggpid'] = HMA['bldggpid']
        transaction_detail_items['bldggp_c'] = HMA['bldggp_c']
        transaction_detail_items['bldggp_e'] = HMA['bldggp_e']
        transaction_detail_items['url'] = HMA['url']
        transaction_detail_items['hma_id'] = HMA['hma_id']
        transaction_detail_items['HMA_Lng'] = HMA['HMA_Lng']
        transaction_detail_items['HMA_Lat'] = HMA['HMA_Lat']
        transaction_detail_items['hma_c'] = HMA['hma_c']
        transaction_detail_items['hma_e'] = HMA['hma_e']

        yield transaction_detail_items

        