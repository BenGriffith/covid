import pytest
import requests

class CovidData:

    def __init__(self, path, name, ext, mode):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode

    def get_response(self, url):

        self.response = requests.get(url)

        return self.response

    def create_file(self, response_type):
            
        file = open(f"{self.path}/{self.name}.{self.ext}", self.mode)
        file.write(response_type)
        file.close()

###########################################################

def test_CovidData():

    covid = CovidData("covid-test-path", "test-file", "txt", "w")
    assert covid.path == "covid-test-path"
    assert covid.name == "test-file"
    assert covid.ext == "txt"
    assert covid.mode == "w"

    covid_response = covid.get_response("https://www.nytimes.com/interactive/2021/us/covid-cases.html")
    assert covid_response.status_code == 200
    assert covid_response.text != None

###########################################################

class Texas(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)

###########################################################

def test_Texas():

    texas = Texas("texas-test-path", "texas-file", "csv", "x")
    assert texas.path == "texas-test-path"
    assert texas.name == "texas-file"
    assert texas.ext == "txt"
    assert texas.mode == "w"
    
###########################################################

class Florida(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_fl = []

###########################################################

def test_Florida():

    florida = Florida("florida-test-path", "florida-file", "json", "w")
    assert florida.path == "florida-test-path"
    assert florida.name == "florida-file"
    assert florida.ext == "json"
    assert florida.mode == "r"
    assert florida.offset == 0
    assert florida.limit == 1000
    assert florida.counter == 0
    assert florida.data_fl == []
    
###########################################################

class NewYork(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ny = []

###########################################################

def test_NewYork():

    ny = NewYork("ny-test-path", "ny-file", "csv", "w")
    assert ny.path == "test-path"
    assert ny.name == "file"
    assert ny.ext == "csv"
    assert ny.mode == "w"
    assert ny.offset == 0
    assert ny.limit == 2000
    assert ny.counter == 1
    assert ny.data_ny == []
    
###########################################################

class Pennsylvania(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000

###########################################################

def test_Pennsylvania():

    pa = Pennsylvania("pa-test-path", "pa-file", "txt", "x")
    assert pa.path == "pa-test-path"
    assert pa.name == "pa-file"
    assert pa.ext == "csv"
    assert pa.mode == "w"
    assert pa.offset == 0
    assert pa.limit == 2001
    
###########################################################

class Illinois(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.data_il = []

###########################################################

def test_Illinois():

    illinois = Illinois("il-test-path", "il-file", "orc", "x")
    assert illinois.path == "test-path"
    assert illinois.name == "test-file"
    assert illinois.ext == "orc"
    assert illinois.mode == "w"
    
###########################################################

class Ohio(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)

###########################################################

def test_Ohio():

    ohio = Ohio("ohio-test-path", "ohio-file", "txt", "w")
    assert ohio.path == "ohio-test-path"
    assert ohio.name == "ohio-file"
    assert ohio.ext == "txt"
    assert ohio.mode == "w"
    
###########################################################

class Georgia(CovidData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.offset = 0
        self.limit = 2000
        self.counter = 1
        self.data_ga = []

###########################################################

def test_Georgia():

    georgia = Georgia("ga-test-path", "ga-file", "parquet", "w")
    assert georgia.path == "ga-test-path"
    assert georgia.name == "file"
    assert georgia.ext == "parquet"
    assert georgia.mode == "w"
    assert georgia.offset == 1
    assert georgia.limit == 10000
    assert georgia.counter == 2
    assert georgia.data_ga == [1]
    
###########################################################

class FinancialData:

    def __init__(self, path, name, ext, mode):
        self.path = path
        self.name = name
        self.ext = ext
        self.mode = mode

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
            symbol = sp_stocks.find_all(class_='external text')[i].get_text()
            if symbol.find('.') > 0:
                symbol = symbol.replace('.', '-')
            stock_symbols.append(symbol)

        return stock_symbols

###########################################################

def test_FinancialData():

    financial = FinancialData("financial-test-path", "financial-file", "dat", "x")
    assert financial.path == "test-path"
    assert financial.name == "financial-file"
    assert financial.ext == "dat"
    assert financial.mode == "w"
    assert get_stock_symbols() == ['AAPL', 'MMM', 'TSLA']
    
###########################################################

class StocksDaily(FinancialData):

    def __init__(self, path, name, ext, mode):
        super().__init__(path, name, ext, mode)
        self.outputsize = 'full'
        self.datatype = 'json'
        self.attribute = 'Time Series (Daily)'

###########################################################

def test_StocksDaily():

    daily = StocksDaily("daily-test-path", "daily-file", "json", "json")
    assert daily.path == "daily-test-path"
    assert daily.name == "daily-file"
    assert daily.ext == "json"
    assert daily.mode == "w"
    assert daily.outputsize == "compact"
    assert daily.datatype == "json"
    assert daily.attribute == "Time Series (Daily)"