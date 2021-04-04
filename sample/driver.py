import os
import extract
import clean
import utils

# Temporary
tmp_county_path = 'tmp/county'
tmp_stock_path = 'tmp/financial/stocks'
tmp_indicator_path = 'tmp/financial/indicators'


# Permanent
county_path = 'data/county'
stock_path = 'data/financial/stocks'
indicator_path = 'data/financial/indicators'


# Directory creation
os.makedirs(tmp_county_path)
os.makedirs(tmp_stock_path)
os.makedirs(tmp_indicator_path)
os.makedirs(county_path)
os.makedirs(stock_path)
os.makedirs(indicator_path)


# Extract Covid data
# extract.California(tmp_county_path, 'california', 'json', 'w')
extract.Texas(tmp_county_path, 'texas', 'xlsx', 'wb', utils.texas_url)
extract.Florida(tmp_county_path, 'florida', 'json', 'w')
extract.NewYork(tmp_county_path, 'new-york', 'json', 'w')
extract.Pennsylvania(tmp_county_path, 'pennsylvania', 'json', 'w')
extract.Illinois(tmp_county_path, 'illinois', 'json', 'w')
extract.Ohio(tmp_county_path, 'ohio', 'csv', 'x', utils.ohio_url)
extract.Georgia(tmp_county_path, 'georgia', 'json', 'w')
extract.USAFacts(tmp_county_path, 'cases', 'csv', 'x', utils.cases_url)
extract.USAFacts(tmp_county_path, 'deaths', 'csv', 'x', utils.deaths_url)
extract.USAFacts(tmp_county_path, 'population', 'csv', 'x', utils.population_url)


# Extract Financial data
stock_symbols = extract.FinancialData.get_stock_symbols()
extract.FinancialData.create_directories(tmp_stock_path, stock_symbols)
extract.StocksDaily(tmp_stock_path, 'daily', 'json', 'w', stock_symbols, 'TIME_SERIES_DAILY', utils.url_vantage, utils.headers_stocks)
extract.StocksWeekly(tmp_stock_path, 'weekly', 'json', 'w', stock_symbols, 'TIME_SERIES_WEEKLY', utils.url_vantage, utils.headers_stocks)
extract.StocksMonthly(tmp_stock_path, 'monthly', 'json', 'w', stock_symbols, 'TIME_SERIES_MONTHLY', utils.url_vantage, utils.headers_stocks)
extract.Sector(tmp_indicator_path, 'sector', 'json', 'w', 'SECTOR', utils.url_vantage, utils.headers_indicators)
extract.Indicator(tmp_indicator_path, 'unemployment', 'json', 'w', utils.unemployment_url, utils.headers_indicators)
extract.Indicator(tmp_indicator_path, 'sentiment', 'json', 'w', utils.consumer_sentiment_url, utils.headers_indicators)
extract.Indicator(tmp_indicator_path, 'inflation', 'json', 'w', utils.inflation_url, utils.headers_indicators)
extract.Indicator(tmp_indicator_path, 'mortgage', 'json', 'w', utils.mortgage_url, utils.headers_indicators)


# Clean Covid data
# clean.California(f'{tmp_county_path}/california.json', 'json', county_path)
clean.Texas(f'{tmp_county_path}/texas.xlsx', county_path)
clean.Florida(f'{tmp_county_path}/florida.json', 'json', county_path)
clean.NewYork(f'{tmp_county_path}/new-york.json', 'json', county_path)
clean.Pennsylvania(f'{tmp_county_path}/pennsylvania.json', 'json', county_path)
clean.Illinois(f'{tmp_county_path}/illinois.json', 'json', county_path)
clean.Ohio(f'{tmp_county_path}/ohio.csv', 'csv', county_path)
clean.Georgia(f'{tmp_county_path}/georgia.json', 'json', county_path)
clean.Cases(f'{tmp_county_path}/cases.csv', 'csv', county_path)
clean.Deaths(f'{tmp_county_path}/deaths.csv', 'csv', county_path)
clean.Population(f'{tmp_county_path}/population.csv', 'csv', county_path)