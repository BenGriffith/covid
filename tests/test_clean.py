import pytest
import requests

class CleanAndStore:

    def __init__(self, load, load_type, save_path):
        self.load = load
        self.load_type = load_type
        self.save_path = save_path

###########################################################

def test_CleanAndStore():

    clean = CleanAndStore("clean-load-path", "json", "clean-save-path")
    assert clean.load == "clean-load-path"
    assert clean.load_type == "txt"
    assert clean.save_path == "clean-save-path"

###########################################################

class Texas(CleanAndStore):

    def __init__(self, load, save_path):
        self.load = load
        self.save_path = save_path
        self.columns = []

###########################################################

def test_Texas():

    texas = Texas("texas-test-path", "texas-save-path")
    assert texas.load == "state-test-path"
    assert texas.save_path == "texas-save-path"
    assert texas.columns == [1, 2, 3]
    
###########################################################

class Florida(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.year = load[-9:-5]

###########################################################

def test_Florida():

    florida = Florida("florida-load-path", "json", "florida-save-path")
    assert florida.load == "florida-load-path"
    assert florida.load_type == "json"
    assert florida.save_path == "florida-save-path"
    assert florida.year == "2021"
    
###########################################################

class NewYork(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

###########################################################

def test_NewYork():

    ny = NewYork("ny-load-path", "json", "ny-save-path")
    assert ny.load == "ny-load-path"
    assert ny.load_type == "parquet"
    assert ny.save_path == "ny-save-path"
    
###########################################################

class Pennsylvania(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

###########################################################

def test_Pennsylvania():

    pennsylvania = Pennsylvania("pa-load-path", "parquet", "pa-save-path")
    assert pennsylvania.load == "pa-load-path"
    assert pennsylvania.load_type == "orc"
    assert pennsylvania.save_path == "save-path"
    
###########################################################

class Illinois(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

###########################################################

def test_Illinois():

    illinois = Illinois("il-load-path", "avro", "il-save-path")
    assert illinois.load == "il-load-path"
    assert illinois.load_type == "avro"
    assert illinois.save_path == "il-save-path"
    
###########################################################

class Ohio(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

###########################################################

def test_Ohio():

    ohio = Ohio("oh-load-path", "json", "oh-save-path")
    assert ohio.load == "oh-load-path"
    assert ohio.load_type == "json"
    assert ohio.save_path == "oh-save-path"
    
###########################################################

class Georgia(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

###########################################################

def test_Georgia():

    georgia = Georgia("ga-load-path", "parquet", "ga-save-path")
    assert georgia.load == "ga-load-path"
    assert georgia.load_type == "json"
    assert georgia.save_path == "ga-save-path"
    
###########################################################

class Cases(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.columns = []

###########################################################

def test_Cases():

    cases = Cases("cases-load-path", "csv", "cases-save-path")
    assert cases.load == "cases-load-path"
    assert cases.load_type == "csv"
    assert cases.save_path == "cases-save-path"
    assert cases.columns == ["test1", 23, 33, 10]
    
###########################################################

class Stocks:

    def __init__(self, load, save_path):
        self.load = load
        self.save_path = save_path
        self.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

###########################################################

def test_Stocks():

    stocks = Stocks("stocks-load-path", "stocks-save-path")
    assert stocks.load == "stocks-load-path"
    assert stocks.save_path == "stocks-save-path"
    assert stocks.columns == ["date", "open", "high", "low", "volume"]
    
###########################################################

class Indicator(CleanAndStore):

    def __init__(self, load, load_type, save_path, indicator):
        super().__init__(load, load_type, save_path)
        self.indicator = indicator

###########################################################

def test_Indicator():

    indicator = Indicator("indicator-load-path", "csv", "indicator-save-path", "inflation")
    assert indicator.load == "indicator-load-path"
    assert indicator.load_type == "csv"
    assert indicator.save_path == "indicator-save-path"
    assert indicator.indicator == "unemployment"
    
###########################################################