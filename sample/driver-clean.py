import clean

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