import extract
import utils
import directories
import shutil
from pathlib import Path


# Directory setup
directories.initial()

 
# Extract Covid data
extract.Texas(utils.tmp_county_path, "texas", "xlsx", "wb", None, utils.texas_url)
extract.Florida(utils.tmp_county_path, "florida2020", "json", "w")
extract.Florida(utils.tmp_county_path, "florida2021", "json", "w")
extract.NewYork(utils.tmp_county_path, "new-york", "json", "w")
extract.Pennsylvania(utils.tmp_county_path, "pennsylvania", "json", "w")
extract.Illinois(utils.tmp_county_path, "illinois", "json", "w")
extract.Ohio(utils.tmp_county_path, "ohio", "csv", "x", None, utils.ohio_url)
extract.Georgia(utils.tmp_county_path, "georgia", "json", "w")
extract.USAFacts(utils.tmp_county_path, "cases", "csv", "x", None, utils.cases_url)
extract.USAFacts(utils.tmp_county_path, "deaths", "csv", "x", None, utils.deaths_url)
extract.USAFacts(utils.tmp_county_path, "population", "csv", "x", None, utils.population_url)


# Extract Financial data
stock_symbols = extract.FinancialData.get_stock_symbols()
extract.FinancialData.create_directories(utils.tmp_stock_path, utils.stock_path, stock_symbols)
extract.StocksDaily(utils.tmp_stock_path, "daily", "json", "w", stock_symbols, "TIME_SERIES_DAILY", utils.url_vantage, utils.headers_stocks)
extract.StocksWeekly(utils.tmp_stock_path, "weekly", "json", "w", stock_symbols, "TIME_SERIES_WEEKLY", utils.url_vantage, utils.headers_stocks)
extract.StocksMonthly(utils.tmp_stock_path, "monthly", "json", "w", stock_symbols, "TIME_SERIES_MONTHLY", utils.url_vantage, utils.headers_stocks)
extract.Indicator(utils.tmp_indicator_path, "unemployment", "json", "w", utils.unemployment_url, utils.headers_indicators, "Labor Force Statistics including the National Unemployment Rate")
extract.Indicator(utils.tmp_indicator_path, "sentiment", "json", "w", utils.consumer_sentiment_url, utils.headers_indicators, "Index of Consumer Sentiment - monthly")
extract.Indicator(utils.tmp_indicator_path, "inflation", "json", "w", utils.inflation_url, utils.headers_indicators, "Consumer Price Index (CPI)")
extract.Indicator(utils.tmp_indicator_path, "mortgage", "json", "w", utils.mortgage_url, utils.headers_indicators, "Housing affordability index")

# Clean up directories
path_tmp = Path("tmp")
if path_tmp.exists() and path_tmp.is_dir():
    shutil.rmtree(path_tmp)

path_data = Path("data")
if path_data.exists() and path_data.is_dir():
    shutil.rmtree(path_data)