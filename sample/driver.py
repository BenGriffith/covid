import extract
import utils
import directories
import shutil
import sys
from pathlib import Path
from datetime import datetime, date, timedelta

if sys.argv[1] == "initial":

    # Directory setup
    directories.initial()

    
    # Extract Covid data
    extract.Texas(utils.tmp_county_path, "texas", "xlsx", "wb", None, utils.texas_url)
    extract.Florida(utils.tmp_county_path, "florida2021", "json", "w", None)
    extract.Florida(utils.tmp_county_path, "florida2020", "json", "w", None)
    extract.NewYork(utils.tmp_county_path, "new-york", "json", "w", None)
    extract.Pennsylvania(utils.tmp_county_path, "pennsylvania", "json", "w", None)
    extract.Ohio(utils.tmp_county_path, "ohio", "csv", "x", None, utils.ohio_url)
    extract.Georgia(utils.tmp_county_path, "georgia", "json", "w", None)
    extract.USAFacts(utils.tmp_county_path, "cases", "csv", "x", None, utils.cases_url)
    extract.USAFacts(utils.tmp_county_path, "deaths", "csv", "x", None, utils.deaths_url)
    extract.USAFacts(utils.tmp_county_path, "population", "csv", "x", None, utils.population_url)


    # Extract Financial data
    stock_symbols = extract.FinancialData.get_stock_symbols()
    extract.FinancialData.create_directories(utils.tmp_stock_path, utils.stock_path, stock_symbols)
    extract.StocksDaily(utils.tmp_stock_path, "daily", "json", "w", stock_symbols, "TIME_SERIES_DAILY", utils.url_vantage, utils.headers_stocks)
    extract.StocksWeekly(utils.tmp_stock_path, "weekly", "json", "w", stock_symbols, "TIME_SERIES_WEEKLY", utils.url_vantage, utils.headers_stocks)
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

else:

    # Yesterday's date and Georgia date
    run_date = date.today() - timedelta(days=1)
    ga_run_date = date.today() - timedelta(days=2)

    # Directory setup
    directories.incremental(run_date, ga_run_date)
    
    # Extract Covid data
    extract.Texas("tmp/incremental/county", "texas", "xlsx", "wb", run_date, utils.texas_url)
    extract.Florida("tmp/incremental/county", "florida2020", "json", "w", run_date)
    extract.Florida("tmp/incremental/county", "florida2021", "json", "w", run_date)
    extract.NewYork("tmp/incremental/county", "new-york", "json", "w", run_date)
    extract.Pennsylvania("tmp/incremental/county", "pennsylvania", "json", "w", run_date)
    extract.Ohio("tmp/incremental/county", "ohio", "csv", "x", run_date, utils.ohio_url)
    extract.Georgia("tmp/incremental/county", "georgia", "json", "w", ga_run_date)
    extract.USAFacts("tmp/incremental/county", "cases", "csv", "x", run_date, utils.cases_url)
    extract.USAFacts("tmp/incremental/county", "deaths", "csv", "x", run_date, utils.deaths_url)

    # Clean up directories
    path_tmp = Path("tmp")
    if path_tmp.exists() and path_tmp.is_dir():
        shutil.rmtree(path_tmp)