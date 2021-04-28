import extract
import clean
import utils
import directories
import shutil
from datetime import datetime, date, timedelta
from pathlib import Path

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
extract.Illinois("tmp/incremental/county", "illinois", "json", "w", run_date)
extract.Ohio("tmp/incremental/county", "ohio", "csv", "x", run_date, utils.ohio_url)
extract.Georgia("tmp/incremental/county", "georgia", "json", "w", ga_run_date)
extract.USAFacts("tmp/incremental/county", "cases", "csv", "x", run_date, utils.cases_url)
extract.USAFacts("tmp/incremental/county", "deaths", "csv", "x", run_date, utils.deaths_url)

# Clean up directories
path_tmp = Path("tmp")
if path_tmp.exists() and path_tmp.is_dir():
    shutil.rmtree(path_tmp)

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