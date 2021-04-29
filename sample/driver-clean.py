import sys
import clean
from datetime import datetime, date, timedelta

if sys.argv[1] == "initial":

    # Populate county table
    clean.Population("tmp/initial/county/population.csv", "csv", "data/initial/county", 1)

    # Clean
    clean.Texas("tmp/initial/county/texas.xlsx", "data/initial/county", None)
    clean.Florida("tmp/initial/county/florida2020.json", "json", "data/initial/county", None)
    clean.Florida("tmp/initial/county/florida2021.json", "json", "data/initial/county", None)
    clean.NewYork("tmp/initial/county/new-york.json", "json", "data/initial/county", None)
    clean.Pennsylvania("tmp/initial/county/pennsylvania.json", "json", "data/initial/county", None)
    clean.Illinois("tmp/initial/county/illinois.json", "json", "data/initial/county", None)
    clean.Ohio("tmp/initial/county/ohio.csv", "csv", "data/initial/county", None)
    clean.Georgia("tmp/initial/county/georgia.json", "json", "data/initial/county", None)
    clean.Cases("tmp/initial/county/cases.csv", "csv", "data/initial/county", None)
    clean.Deaths("tmp/initial/county/deaths.csv", "csv", "data/initial/county", None)
    clean.Population("tmp/initial/county/population.csv", "csv", "data/initial/county", 0)
    clean.Stocks("tmp/initial/financial/stocks", "data/initial/financial/stocks")
    clean.Indicator("tmp/initial/financial/indicators/unemployment.json", "json", "data/initial/financial/indicators", "unemployment")
    clean.Indicator("tmp/initial/financial/indicators/inflation.json", "json", "data/initial/financial/indicators", "inflation")
    clean.Indicator("tmp/initial/financial/indicators/sentiment.json", "json", "data/initial/financial/indicators", "sentiment")
    clean.Indicator("tmp/initial/financial/indicators/mortgage.json", "json", "data/initial/financial/indicators", "mortgage")

else:

    # Yesterday's date and Georgia date
    run_date = date.today() - timedelta(days=1)
    ga_run_date = date.today() - timedelta(days=2)

    # Clean Covid data
    clean.Texas(f"tmp/incremental/county/{run_date}/texas.xlsx", f"data/incremental/county/{run_date}", run_date)
    clean.Florida(f"tmp/incremental/county/{run_date}/florida2020.json", "json", f"data/incremental/county/{run_date}", run_date)
    clean.Florida(f"tmp/incremental/county/{run_date}/florida2021.json", "json", f"data/incremental/county/{run_date}", run_date)
    clean.NewYork(f"tmp/incremental/county/{run_date}/new-york.json", "json", f"data/incremental/county/{run_date}", run_date)
    clean.Pennsylvania(f"tmp/incremental/county/{run_date}/pennsylvania.json", "json", f"data/incremental/county/{run_date}", run_date)
    clean.Illinois(f"tmp/incremental/county/{run_date}/illinois.json", "json", f"data/incremental/county/{run_date}", run_date)
    clean.Ohio(f"tmp/incremental/county/{run_date}/ohio.csv", "csv", f"data/incremental/county/{run_date}", run_date)
    clean.Georgia(f"tmp/incremental/county/{ga_run_date}/georgia.json", "json", f"data/incremental/county/{ga_run_date}", ga_run_date)
    clean.Cases(f"tmp/incremental/county/{run_date}/cases.csv", "csv", f"data/incremental/county/{run_date}", run_date)
    clean.Deaths(f"tmp/incremental/county/{run_date}/deaths.csv", "csv", f"data/incremental/county/{run_date}", run_date)