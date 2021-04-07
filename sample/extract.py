import os
import requests
import time
import json
import warnings
import logging
from bs4 import BeautifulSoup
from logs import log

warnings.filterwarnings('ignore')

class CovidData:

    def __init__(self, path, name, ext, mode):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode

    def get_response(self, url):
        # Submit API request
        self.response = requests.get(url)

        # Output to log
        log.logging.info('Request-Response submitted for {} with status code of {}'.format(type(self).__name__, self.response.status_code))

        # Output to console
        print('Request-Response submitted for {} with status code of {}'.format(type(self).__name__, self.response.status_code))

    def create_file(self, response_type):
        file = open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode)
        file.write(response_type)
        file.close()

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))
        
        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

# class California(CovidData):

#     def __init__(self, path, name, ext, mode):
#         super().__init__(path, name, ext, mode)
#         self.offset = 0
#         self.limit = 2000
#         self.counter = 1
#         self.data_ca = []
#         self.get_response()
#         self.create_file()

#     def get_response(self):

#         while True:

#             # Submit API request
#             url = 'https://data.ca.gov/api/3/action/datastore_search?resource_id=1be1e43c-b4b2-4002-afb6-340bbcc85bbf&offset={}&limit={}'.format(self.offset, self.limit)
#             response = requests.get(url)

#             # Output to log
#             log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

#             # Output to console
#             print('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

#             # If API response returns data add it to list
#             # If API response does not return any data break the loop construct
#             if response.json()['result']['records']:
                
#                 for row in response.json()['result']['records']:
#                     self.data_ca.append(row)
                    
#             else:
#                 break
            
#             # Increase offset for pagination purposes
#             self.offset += self.limit
#             self.counter += 1

#             # Pause program before submitting next API request
#             time.sleep(10)

#     def create_file(self):
#         with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as california_file:
#             json.dump(self.data_ca, california_file)

#         # Output to log
#         log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

#         # Output to console
#         print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class Texas(CovidData):

    def __init__(self, path, name, ext, mode, url):
        super().__init__(path, name, ext, mode)
        super().get_response(url) 
        super().create_file(self.response.content)

class Florida(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 10000
        self.counter = 1
        self.data_fl = []
        self.get_response()
        self.create_file()

    def get_response(self):

        for i in range(20): #while True:

            # Submit API request
            url = 'https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_COVID19_Case_Line_Data_NEW/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&resultOffset={}&resultRecordCount={}&f=json'.format(self.offset, self.limit)
            response = requests.get(url)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # If API response returns data add it to list
            # If API response does not return any data break the loop construct
            if response.text.strip() != '[]':
                
                current_response_data = [feature['attributes'] for feature in response.json()['features']]
                
                for row in current_response_data:
                    self.data_fl.append(row)
            else:
                break
            
            # Increase offset for pagination purposes
            self.offset += self.limit
            self.counter += 1

            # Pause program before submitting next API request
            time.sleep(10)

    def create_file(self):
        with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as florida_file:
            json.dump(self.data_fl, florida_file)

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class NewYork(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ny = []
        self.get_response()
        self.create_file()

    def get_response(self):

        while True:

            # Submit API request
            url = 'https://health.data.ny.gov/resource/xdss-u53e.json?$limit={}&$offset={}'.format(self.limit, self.offset)
            response = requests.get(url)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))
            
            # If API response returns data add it to list
            # If API response does not return any data break the loop construct
            if response.text.strip() != '[]':
                for row in response.json():
                    self.data_ny.append(row)
            else:
                break
            
            # Increase offset for pagination purposes
            self.offset += self.limit
            self.counter += 1

            # Pause program before submitting next API request
            time.sleep(10)

    def create_file(self):
        with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as new_york_file:
            json.dump(self.data_ny, new_york_file)

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class Pennsylvania(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_pa = []
        self.get_response()
        self.create_file()

    def get_response(self):
        while True:

            # Submit API request
            url = 'https://data.pa.gov/resource/j72v-r42c.json?$limit={}&$offset={}'.format(self.limit, self.offset)
            response = requests.get(url)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))
            
            # If API response returns data add it to list
            # If API response does not return any data break the loop construct
            if response.text.strip() != '[]':
                for row in response.json():
                    self.data_pa.append(row)
            else:
                break
            
            # Increase offset for pagination purposes
            self.offset += self.limit
            self.counter += 1

            # Pause program before submitting next API request
            time.sleep(10)

    def create_file(self):
        with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as penn_file:
            json.dump(self.data_pa, penn_file)

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class Illinois(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.data_il = []
        self.get_response()
        self.create_file()

    def get_response(self):

        # Scrape Illinois counties in order to pass county name to URL
        url_counties = 'https://en.wikipedia.org/wiki/List_of_counties_in_Illinois'
        response_url_counties = requests.get(url_counties)
        soup = BeautifulSoup(response_url_counties.text, 'html.parser')

        counties = []

        for county in soup.select('tbody > tr > th > a'):
            counties.append(county.get_text())

        counties = [county.split(' ')[0] for county in counties]

        for index, county in enumerate(counties[:10], start=1):

            # Submit API request
            url = 'https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetCountyHistorical?countyName={}'.format(county)
            response = requests.get(url)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            for row in response.json()['values']:
                self.data_il.append(row)

            # Pause program before submitting next API request
            time.sleep(60)
        
    def create_file(self):
        with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as illinois_file:
            json.dump(self.data_il, illinois_file)

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class Ohio(CovidData):

    def __init__(self, path, name, ext, mode, url):
        super().__init__(path, name, ext, mode)
        super().get_response(url)
        super().create_file(self.response.text)

class Georgia(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ga = []
        self.get_response()
        self.create_file()

    def get_response(self):

        for i in range(20): #while True:

            # Submit API request
            url = 'https://services7.arcgis.com/Za9Nk6CPIPbvR1t7/arcgis/rest/services/Georgia_PUI_Data_Download/FeatureServer/0/query?outFields=*&where=1%3D1&resultOffset={}&resultRecordCount={}&f=json'.format(self.offset, self.limit)
            response = requests.get(url)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(self.counter, type(self).__name__, response.status_code))

            # If API response returns data add it to list
            # If API response does not return any data break the loop construct
            if response.json()['features']:
                
                current_response_data = [feature['attributes'] for feature in response.json()['features']]
                
                for row in current_response_data:
                    self.data_ga.append(row)
            else:
                break
            
            # Increase offset for pagination purposes
            self.offset += self.limit
            self.counter += 1

            # Pause program before submitting next API request
            time.sleep(10)

    def create_file(self):
        with open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode) as georgia_file:
            json.dump(self.data_ga, georgia_file)

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

class USAFacts(CovidData):

    def __init__(self, path, name, ext, mode, url):
        super().__init__(path, name, ext, mode)
        super().get_response(url)
        super().create_file(self.response.text)

class FinancialData:

    def __init__(self, path, name, ext, mode):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode

    def create_file(self, symbol, response, attribute):
        with open('{}/{}/{}.{}'.format(self.path, symbol.lower(), self.name, self.ext), self.mode) as stock_file:
            json.dump(response.json()[attribute], stock_file)

        # Output to log
        log.logging.info('{}.{} file created for {} for {}'.format(self.name, self.ext, type(self).__name__, symbol))

        # Output to console
        print('{}.{} file created for {} for {}'.format(self.name, self.ext, type(self).__name__, symbol))

    @staticmethod
    def get_stock_symbols():
        '''
        Scraper to retrieve stock symbols for S&P 500 to be used in API calls
        '''
        url_stocks = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        stocks = requests.get(url_stocks)
        soup = BeautifulSoup(stocks.text, 'html.parser')

        sp_stocks = BeautifulSoup(str(soup.find_all(id='constituents')), 'html.parser')
        sp_stocks = BeautifulSoup(str(sp_stocks.select('tbody > tr > td > a')), 'html.parser')

        stock_symbols = []

        for i in range(0, len(sp_stocks.find_all(class_='external text')), 2):
            stock_symbols.append(sp_stocks.find_all(class_='external text')[i].get_text())

        return stock_symbols

    @staticmethod
    def create_directories(stock_path, stock_symbols):
        for stock_symbol in stock_symbols[:5]:
            os.makedirs('{}/{}'.format(stock_path, stock_symbol.lower()))

class StocksDaily(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.outputsize = 'full'
        self.datatype = 'json'
        self.attribute = 'Time Series (Daily)'
        self.get_response()

    def get_response(self):

        for index, symbol in enumerate(self.stock_symbols[:5], start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "outputsize": self.outputsize,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            super().create_file(symbol, response, self.attribute)
            
            # Pause program before submitting next API request
            time.sleep(15)

class StocksWeekly(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.datatype = 'json'
        self.attribute = 'Weekly Time Series'
        self.get_response()

    def get_response(self):

        for index, symbol in enumerate(self.stock_symbols[:5], start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            super().create_file(symbol, response, self.attribute)
            
            # Pause program before submitting next API request
            time.sleep(15)

class StocksMonthly(FinancialData):

    def __init__(self, path, name, ext, mode, stock_symbols, function, url, headers_stocks):
        super().__init__(path, name, ext, mode)
        self.stock_symbols = stock_symbols
        self.function = function
        self.url = url
        self.headers_stocks = headers_stocks
        self.datatype = 'json'
        self.attribute = 'Monthly Time Series'
        self.get_response()

    def get_response(self):

        for index, symbol in enumerate(self.stock_symbols[:5], start=1):
        
            querystring = {"function": self.function,
                           "symbol": symbol,
                           "datatype": self.datatype}

            # Submit API request
            response = requests.get(self.url, headers=self.headers_stocks, params=querystring)

            # Output to log
            log.logging.info('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            # Output to console
            print('Request-Response {} submitted for {} with status code of {}'.format(index, type(self).__name__, response.status_code))

            super().create_file(symbol, response, self.attribute)
            
            # Pause program before submitting next API request
            time.sleep(15)

class Indicator(FinancialData):
    
    def __init__(self, path, name, ext, mode, url, headers_indicators):
        super().__init__(path, name, ext, mode)
        self.url = url
        self.headers_indicators = headers_indicators
        self.get_response()
        self.create_file()

    def get_response(self):

        # Submit API request
        self.response = requests.get(self.url, headers=self.headers_indicators)

        # Output to log
        log.logging.info('Request-Response submitted for {} {} with status code of {}'.format(self.name, type(self).__name__, self.response.status_code))

        # Output to console
        print('Request-Response submitted for {} {} with status code of {}'.format(self.name, type(self).__name__, self.response.status_code))

    def create_file(self):
        file = open('{}/{}.{}'.format(self.path, self.name, self.ext), self.mode)
        file.write(self.response.text)
        file.close()      

        # Output to log
        log.logging.info('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))

        # Output to console
        print('{}.{} file created for {}'.format(self.name, self.ext, type(self).__name__))