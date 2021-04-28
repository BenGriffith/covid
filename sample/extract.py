import os
import requests
import time
import json
import warnings
import logging
import utils
import pandas as pd
from bs4 import BeautifulSoup
from logs import log
from azure.storage.blob import BlobClient

warnings.filterwarnings("ignore")

class CovidData:

    def __init__(self, path, name, ext, mode, run_date=None):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode
        self.run_date = run_date

    def get_response(self, url):
        """
        Retrieve URL response
        """

        # Submit API request
        self.response = requests.get(url)

        # Output to log
        log.logging.info("Request-Response submitted for {} with status code of {}".format(type(self).__name__, self.response.status_code))

    def create_file(self, response_type):
        """
        File creation
        """
        
        if self.run_date == None:

            file = open(f"{self.path}/{self.name}.{self.ext}", self.mode)
            file.write(response_type)
            file.close()
        else:

            file = open(f"{self.path}/{self.run_date}/{self.name}.{self.ext}", self.mode)
            file.write(response_type)
            file.close()
        
        # Output to log
        log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

    def upload_blob(self):
        """
        Uploading file to Azure Blob storage
        """

        if self.run_date == None:

            blob = BlobClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name, blob_name=f"{self.path}/{self.name}.{self.ext}") 

            with open(f"{self.path}/{self.name}.{self.ext}", "rb") as data:
                blob.upload_blob(data)
        else:

            blob = BlobClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name, blob_name=f"{self.path}/{self.name}.{self.ext}") 

            with open(f"{self.path}/{self.run_date}/{self.name}.{self.ext}", "rb") as data:
                blob.upload_blob(data)

class Texas(CovidData):

    def __init__(self, path, name, ext, mode, run_date, url):
        super().__init__(path, name, ext, mode, run_date)
        super().get_response(url) 
        super().create_file(self.response.content)
        super().upload_blob()

class Florida(CovidData):

    def __init__(self, path, name, ext, mode, run_date):
        super().__init__(path, name, ext, mode, run_date)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_fl = []
        self.get_response()
        self.create_file()
        super().upload_blob()

    def get_response(self):
        """
        Submit Florida API request, add data to list, increase offset for pagination in next API request
        """

        if self.run_date == None:

            while True:

                url = "https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Case_Data_{}/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&resultOffset={}&resultRecordCount={}&f=json".format(self.name[-4:], self.offset, self.limit)
                response = requests.get(url)

                # Output to log
                log.logging.info("Request-Response {} submitted for {} with status code of {}".format(self.counter, type(self).__name__, response.status_code))

                # If API response returns data add it to list
                # If API response does not return any data break the loop construct
                if response.json()["features"]:
                    
                    current_response_data = [feature["attributes"] for feature in response.json()["features"]]
                    
                    for row in current_response_data:
                        self.data_fl.append(row)

                else:
                    break
                
                # Increase offset for pagination purposes
                self.offset += self.limit
                self.counter += 1

                # Pause program before submitting next API request
                time.sleep(15)
        else:

            url = f"https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Case_Data_2021/FeatureServer/0/query?where=EventDate%20%3E%3D%20TIMESTAMP%20'{run_date}%2000%3A00%3A00'%20AND%20EventDate%20%3C%3D%20TIMESTAMP%20'{run_date}%2000%3A00%3A00'&outFields=*&outSR=4326&f=json"
            response = requests.get(url)
                            
            # Output to log
            log.logging.info("Request-Response submitted for {} with status code of {}".format(type(self).__name__, response.status_code))

            current_response_data = [feature['attributes'] for feature in response.json()['features']]

            if current_response_data:
                for row in current_response_data:
                    self.data_fl.append(row)

    def create_file(self):
        """ 
        Creat JSON file for Florida
        """

        if self.run_date == None:

            with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as florida_file:
                json.dump(self.data_fl, florida_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

        else:

            with open("{}/{}/{}.{}".format(self.path, self.run_date, self.name, self.ext), self.mode) as florida_file:
                json.dump(self.data_fl, florida_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

class NewYork(CovidData):

    def __init__(self, path, name, ext, mode, run_date):
        super().__init__(path, name, ext, mode, run_date)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ny = []
        self.get_response()
        self.create_file()
        self.upload_blob()

    def get_response(self):
        """
        Submit New York API request, add data to list, increase offset for pagination in next API request
        """

        if self.run_date == None:

            while True:

                # Submit API request
                url = "https://health.data.ny.gov/resource/xdss-u53e.json?$limit={}&$offset={}".format(self.limit, self.offset)
                response = requests.get(url)

                # Output to log
                log.logging.info("Request-Response {} submitted for {} with status code of {}".format(self.counter, type(self).__name__, response.status_code))
                
                # If API response returns data add it to list
                # If API response does not return any data break the loop construct
                if response.text.strip() != "[]":
                    for row in response.json():
                        self.data_ny.append(row)
                else:
                    break
                
                # Increase offset for pagination purposes
                self.offset += self.limit
                self.counter += 1

                # Pause program before submitting next API request
                time.sleep(15)

        else:

            url = f'https://health.data.ny.gov/resource/xdss-u53e.json?test_date={run_date}'
            response = requests.get(url)

            # Output to log
            log.logging.info("Request-Response submitted for {} with status code of {}".format(type(self).__name__, response.status_code))

            if response.text.strip() != '[]':
                for row in response.json():
                    self.data_ny.append(row)

    def create_file(self):
        """ 
        Create JSON file for New York
        """

        if self.run_date == None:

            with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as new_york_file:
                json.dump(self.data_ny, new_york_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

        else:

            with open("{}/{}/{}.{}".format(self.path, self.run_date, self.name, self.ext), self.mode) as new_york_file:
                json.dump(self.data_ny, new_york_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

class Pennsylvania(CovidData):

    def __init__(self, path, name, ext, mode, run_date):
        super().__init__(path, name, ext, mode, run_date)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_pa = []
        self.get_response()
        self.create_file()
        super().upload_blob()

    def get_response(self):
        """
        Submit Pennsylvania API request, add data to list, increase offset for pagination in next API request
        """
        
        if self.run_date == None:

            while True:

                # Submit API request
                url = "https://data.pa.gov/resource/j72v-r42c.json?$limit={}&$offset={}".format(self.limit, self.offset)
                response = requests.get(url)

                # Output to log
                log.logging.info("Request-Response {} submitted for {} with status code of {}".format(self.counter, type(self).__name__, response.status_code))
                
                # If API response returns data add it to list
                # If API response does not return any data break the loop construct
                if response.text.strip() != "[]":
                    for row in response.json():
                        self.data_pa.append(row)
                else:
                    break
                
                # Increase offset for pagination purposes
                self.offset += self.limit
                self.counter += 1

                # Pause program before submitting next API request
                time.sleep(15)

        else:

            url = f'https://data.pa.gov/resource/j72v-r42c.json?date={run_date}'
            response = requests.get(url)

            # Output to log
            log.logging.info("Request-Response submitted for {} with status code of {}".format(type(self).__name__, response.status_code))

            if response.text.strip() != '[]':
                for row in response.json():
                    self.data_pa.append(row)

    def create_file(self):
        """
        Create JSON file for Pennsylvania
        """

        if self.run_date == None:

            with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as penn_file:
                json.dump(self.data_pa, penn_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

        else:

            with open("{}/{}/{}.{}".format(self.path, self.run_date, self.name, self.ext), self.mode) as penn_file:
                json.dump(self.data_pa, penn_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

class Illinois(CovidData):

    def __init__(self, path, name, ext, mode, run_date):
        super().__init__(path, name, ext, mode, run_date)
        self.data_il = []
        self.get_response()
        self.create_file()
        super().upload_blob()

    def get_response(self):
        """
        1. Scrape Wikipedia for list of Illinois counties and store in list
        2. Loop through county list including county name in each request submission
        """

        # Scrape Illinois counties in order to pass county name to URL
        url_counties = "https://en.wikipedia.org/wiki/List_of_counties_in_Illinois"
        response_url_counties = requests.get(url_counties)
        soup = BeautifulSoup(response_url_counties.text, "html.parser")

        counties = []

        for county in soup.select("tbody > tr > th > a"):
            counties.append(county.get_text())

        counties = [county.split(" ")[0] for county in counties]

        for index, county in enumerate(counties, start=1):

            # Submit API request
            url = "https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetCountyHistorical?countyName={}".format(county)
            response = requests.get(url)

            # Output to log
            log.logging.info("Request-Response {} submitted for {} with status code of {}".format(index, type(self).__name__, response.status_code))

            for row in response.json()["values"]:
                self.data_il.append(row)

            # Pause program before submitting next API request
            time.sleep(120)
        
    def create_file(self):
        """
        Create JSON file for Illinois
        """

        if self.run_date == None:

            with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as illinois_file:
                json.dump(self.data_il, illinois_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

        else:

            with open("{}/{}/{}.{}".format(self.path, self.run_date, self.name, self.ext), self.mode) as illinois_file:
                json.dump(self.data_il, illinois_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

class Ohio(CovidData):

    def __init__(self, path, name, ext, mode, run_date, url):
        super().__init__(path, name, ext, mode, run_date)
        super().get_response(url)
        super().create_file(self.response.text)
        super().upload_blob()

class Georgia(CovidData):

    def __init__(self, path, name, ext, mode, run_date):
        super().__init__(path, name, ext, mode, run_date)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ga = []
        self.get_response()
        self.create_file()
        super().upload_blob()

    def get_response(self):
        """
        Submit Georgia API request, add data to list, increase offset for pagination in next API request
        """

        if self.run_date == None:

            while True:

                # Submit API request
                url = "https://services7.arcgis.com/Za9Nk6CPIPbvR1t7/arcgis/rest/services/Georgia_PUI_Data_Download/FeatureServer/0/query?outFields=*&where=1%3D1&resultOffset={}&resultRecordCount={}&f=json".format(self.offset, self.limit)
                response = requests.get(url)

                # Output to log
                log.logging.info("Request-Response {} submitted for {} with status code of {}".format(self.counter, type(self).__name__, response.status_code))

                # If API response returns data add it to list
                # If API response does not return any data break the loop construct
                if response.json()["features"]:
                    
                    current_response_data = [feature["attributes"] for feature in response.json()["features"]]
                    
                    for row in current_response_data:
                        self.data_ga.append(row)
                else:
                    break
                
                # Increase offset for pagination purposes
                self.offset += self.limit
                self.counter += 1

                # Pause program before submitting next API request
                time.sleep(15)

        else:

            url = 'https://opendata.arcgis.com/datasets/d817e06e4b264905a075b9332cd41962_0.geojson'
            response = requests.get(url)

            # Output to log
            log.logging.info("Request-Response submitted for {} with status code of {}".format(type(self).__name__, response.status_code))

            for row in response.json()['features']:
                self.data_ga.append(row['properties'])

    def create_file(self):
        """
        Create JSON file for Georgia
        """

        if self.run_date == None:

            with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as georgia_file:
                json.dump(self.data_ga, georgia_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

        else:

            with open("{}/{}/{}.{}".format(self.path, self.run_date, self.name, self.ext), self.mode) as georgia_file:
                json.dump(self.data_ga, georgia_file)

            # Output to log
            log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))

class USAFacts(CovidData):

    def __init__(self, path, name, ext, mode, run_date, url):
        super().__init__(path, name, ext, mode, run_date)
        super().get_response(url)
        super().create_file(self.response.text)
        super().upload_blob()

class FinancialData:

    def __init__(self, path, name, ext, mode):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode

    def create_file(self, symbol, response, attribute):
        """
        Data transformation and JSON file creation
        """

        with open("{}/{}/{}.{}".format(self.path, symbol.lower(), self.name, self.ext), self.mode) as stock_file:

            # Create dataframe from API response
            df = pd.DataFrame.from_dict(response.json()[attribute], orient="index")

            # Reset index so that date treated as separate field and reassign column names
            df.reset_index(inplace=True)
            df.columns = ["date", "open", "high", "low", "close", "volume"]

            # Dataframe to dictionary before saving to json file
            stock_dict = df.to_dict(orient="records")
            json.dump(stock_dict, stock_file)

        # Output to log
        log.logging.info("{}.{} file created for {} for {}".format(self.name, self.ext, type(self).__name__, symbol))

    def upload_blob(self, symbol=None):
        """
        1. Upload file to Azure Blob storage
        2. If stock symbol exists, include in file path
        """

        if symbol:

            blob = BlobClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name, blob_name=f"{self.path}/{symbol.lower()}/{self.name}.{self.ext}") 

            with open(f"{self.path}/{symbol.lower()}/{self.name}.{self.ext}", "rb") as data:
                blob.upload_blob(data)

        else:

            blob = BlobClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name, blob_name=f"{self.path}/{self.name}.{self.ext}") 

            with open(f"{self.path}/{self.name}.{self.ext}", "rb") as data:
                blob.upload_blob(data)    

    @staticmethod
    def get_stock_symbols():
        """
        Scraper to retrieve stock symbols for S&P 500 to be used in API calls
        """
        
        url_stocks = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        stocks = requests.get(url_stocks)
        soup = BeautifulSoup(stocks.text, "html.parser")

        sp_stocks = BeautifulSoup(str(soup.find_all(id="constituents")), "html.parser")
        sp_stocks = BeautifulSoup(str(sp_stocks.select("tbody > tr > td > a")), "html.parser")

        stock_symbols = []

        for i in range(0, len(sp_stocks.find_all(class_="external text")), 2):
            symbol = sp_stocks.find_all(class_="external text")[i].get_text()
            if symbol.find(".") > 0:
                symbol = symbol.replace(".", "-")
            stock_symbols.append(symbol)

        return stock_symbols

    @staticmethod
    def create_directories(tmp_stock_path, stock_path, stock_symbols):
        """ 
        Stock directory creation
        """

        for stock_symbol in stock_symbols:
            os.makedirs("{}/{}".format(tmp_stock_path, stock_symbol.lower()))
            os.makedirs("{}/{}".format(stock_path, stock_symbol.lower()))

class StocksDaily(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.outputsize = "full"
        self.datatype = "json"
        self.attribute = "Time Series (Daily)"
        self.get_response()

    def get_response(self):
        """
        Submit API request for each stock, create file and upload file to Azure Blob storage
        """

        for index, symbol in enumerate(self.stock_symbols, start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "outputsize": self.outputsize,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info("Request-Response {} submitted for {} with status code of {}".format(index, type(self).__name__, response.status_code))
            
            super().create_file(symbol, response, self.attribute)

            super().upload_blob(symbol)
            
            # Pause program before submitting next API request
            time.sleep(2)

class StocksWeekly(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.datatype = "json"
        self.attribute = "Weekly Time Series"
        self.get_response()

    def get_response(self):
        """
        Submit API request for each stock, create file and upload file to Azure Blob storage
        """

        for index, symbol in enumerate(self.stock_symbols, start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info("Request-Response {} submitted for {} with status code of {}".format(index, type(self).__name__, response.status_code))

            super().create_file(symbol, response, self.attribute)

            super().upload_blob(symbol)
            
            # Pause program before submitting next API request
            time.sleep(2)

class StocksMonthly(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.datatype = "json"
        self.attribute = "Monthly Time Series"
        self.get_response()

    def get_response(self):
        """
        Submit API request for each stock, create file and upload file to Azure Blob storage
        """

        for index, symbol in enumerate(self.stock_symbols, start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info("Request-Response {} submitted for {} with status code of {}".format(index, type(self).__name__, response.status_code))

            super().create_file(symbol, response, self.attribute)

            super().upload_blob(symbol)
            
            # Pause program before submitting next API request
            time.sleep(2)

class Indicator(FinancialData):
    
    def __init__(self, path, name, ext, mode, url, headers_indicators, dict_key):
        super().__init__(path, name, ext, mode)
        self.url = url
        self.headers_indicators = headers_indicators
        self.get_response()
        self.create_file(dict_key)
        super().upload_blob()

    def get_response(self):

        # Submit API request
        self.response = requests.get(self.url, headers=self.headers_indicators)

        # Output to log
        log.logging.info("Request-Response submitted for {} {} with status code of {}".format(self.name, type(self).__name__, self.response.status_code))

    def create_file(self, dict_key):
        """
        Data transformation and JSON file creation
        """

        with open("{}/{}.{}".format(self.path, self.name, self.ext), self.mode) as indicator_file:

            # Create dataframe from API response
            df = pd.DataFrame.from_dict(self.response.json()[dict_key], orient="index")

            # Reset index so that date treated as separate field and reassign column names
            df.reset_index(inplace=True)
            df.columns = ["date", self.name]

            # Dataframe to dictionary before saving to json file
            indicator_dict = df.to_dict(orient="records")
            json.dump(indicator_dict, indicator_file)

        # Output to log
        log.logging.info("{}.{} file created for {}".format(self.name, self.ext, type(self).__name__))