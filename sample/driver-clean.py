import clean
import utils
 
clean.Texas(f'{utils.tmp_county_path}/texas.xlsx', utils.county_path)
clean.Florida(f'{utils.tmp_county_path}/florida2020.json', 'json', utils.county_path)
clean.Florida(f'{utils.tmp_county_path}/florida2021.json', 'json', utils.county_path)
clean.NewYork(f'{utils.tmp_county_path}/new-york.json', 'json', utils.county_path)
clean.Pennsylvania(f'{utils.tmp_county_path}/pennsylvania.json', 'json', utils.county_path)
clean.Illinois(f'{utils.tmp_county_path}/illinois.json', 'json', utils.county_path)
clean.Ohio(f'{utils.tmp_county_path}/ohio.csv', 'csv', utils.county_path)
clean.Georgia(f'{utils.tmp_county_path}/georgia.json', 'json', utils.county_path)
clean.Cases(f'{utils.tmp_county_path}/cases.csv', 'csv', utils.county_path)
clean.Deaths(f'{utils.tmp_county_path}/deaths.csv', 'csv', utils.county_path)
clean.Population(f'{utils.tmp_county_path}/population.csv', 'csv', utils.county_path)
clean.Stocks(utils.tmp_stock_path, utils.stock_path)
clean.Indicator(f'{utils.tmp_indicator_path}/unemployment.json', 'json', utils.indicator_path, 'unemployment')
clean.Indicator(f'{utils.tmp_indicator_path}/inflation.json', 'json', utils.indicator_path, 'inflation')
clean.Indicator(f'{utils.tmp_indicator_path}/sentiment.json', 'json', utils.indicator_path, 'sentiment')
clean.Indicator(f'{utils.tmp_indicator_path}/mortgage.json', 'json', utils.indicator_path, 'mortgage')