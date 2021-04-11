import directories
import pandas as pd
import requests
import os
import time
import json
import pyspark
import warnings
from bs4 import BeautifulSoup
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta

warnings.filterwarnings('ignore')

# Yesterday's date and Georgia date
run_date = date.today() - timedelta(days=1)
ga_run_date = date.today() - timedelta(days=2)

# Directory creation
directories.incremental(run_date)

def create_file(response, name, ext, mode, action, run_date):
    
    county_path = 'tmp/incremental/county/{}/'.format(run_date)
    indicators_path = 'tmp/incremental/financial/indicators/{}/'.format(run_date)
    
    if action == 1:
        path = county_path
    elif action == 2:
        path = indicators_path
    
    file = open('{}{}.{}'.format(path, name, ext), mode)
    file.write(response)
    file.close()

def save_file(df, option, run_date, name, ext):
    df.write.format(ext).save(f'data/incremental/county/{option}/{run_date}/{name}')


# Spark Session
spark = SparkSession.builder.getOrCreate()

# Texas Covid Data Extraction
url_tx = 'https://dshs.texas.gov/coronavirus/TexasCOVID19DailyCountyCaseCountData.xlsx'
response_tx = requests.get(url_tx)

create_file(response_tx.content, 'texas', 'xlsx', 'wb', 1, run_date)

# Texas Covid Data Cleaning
df_tx_get_columns = pd.read_excel(f'tmp/incremental/county/{run_date}/texas.xlsx', skiprows=2)

columns = []
columns.append('County Name')

for column in df_tx_get_columns.columns[1:]:
        columns.append(datetime.strptime(column.replace('Cases', ' ').lstrip(), '%m-%d-%Y'))
        
df_tx = pd.read_excel(f'tmp/incremental/county/{run_date}/texas.xlsx', names=columns, skiprows=2)

drop_rows = df_tx.iloc[254:]

df_tx = df_tx.drop(drop_rows.index, axis=0)

counties = df_tx['County Name'].tolist()

records = []

for county in counties:
    for date in columns[1:]:
        records.append([date, county])
        
county_cases = []

for row in df_tx.itertuples(index=True):
    county_cases.append(row[2:])
    
case_count = []

for county in county_cases:
    for cases in county:
        case_count.append(cases)
        
for i in range(len(records)):
    records[i].append(case_count[i])
    
final_list = []

for row in records:
    final_dict = {}
    final_dict['date'] = row[0]
    final_dict['county'] = row[1]
    final_dict['case_total'] = row[2]
    final_list.append(final_dict)
    
final_df = pd.DataFrame(final_list)

df_tx = pd.DataFrame(final_list)

df_tx = spark.createDataFrame(df_tx)

df_tx = df_tx.select("date", 
                    upper(col("county")).alias("county"), 
                    col("case_total").cast("int"))

df_tx = df_tx.withColumn("state", lit("TX"))

df_tx = df_tx.filter(df_tx.county != "UNKNOWN")

windowSpec = Window.partitionBy("county").orderBy("date")

df_tx = df_tx.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

df_tx = df_tx.withColumn("new_cases", (df_tx.case_total - df_tx.previous_day))

df_tx.write.format('parquet').save('data/incremental/county/preprocessed/{}/texas'.format(run_date))

save_file(df_tx, 'preprocessed', run_date, 'texas', 'parquet')

df_tx_final = df_tx.select("date", "county", "state", "new_cases").filter(df_tx.date == run_date).orderBy("date", "county")

save_file(df_tx_final, 'final', run_date, 'texas', 'parquet')
    

# Florida Covid Data Extraction
data_fl = []

url_fl = f"https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Case_Data_2021/FeatureServer/0/query?where=EventDate%20%3E%3D%20TIMESTAMP%20'{run_date}%2000%3A00%3A00'%20AND%20EventDate%20%3C%3D%20TIMESTAMP%20'{run_date}%2000%3A00%3A00'&outFields=*&outSR=4326&f=json"
response_fl = requests.get(url_fl)

current_response_data = [feature['attributes'] for feature in response_fl.json()['features']]

if current_response_data:
    for row in current_response_data:
        data_fl.append(row)
    
if data_fl:
    with open(f'tmp/incremental/county/{run_date}/florida.json', 'w') as florida_file:
        json.dump(data_fl, florida_file)

# Florida Covid Data Cleaning
df_fl = spark.read.format("json").load(f"tmp/incremental/county/{run_date}/florida.json")

df_fl = df_fl.select(col("Age").cast("int").alias("age"),
                     when(col("Case_") == "Yes", 1).alias("case"),
                     upper(col("Contact")).alias("contact"),
                     upper(col("County")).alias("county"),
                     upper(col("Died")).alias("died"),
                     upper(col("EDvisit")).alias("ed_visit"),
                     to_timestamp(from_unixtime(substring(col("EventDate").cast("string"), 1, 10))).alias("date"),
                     upper(col("Gender")).alias("gender"),
                     upper(col("Origin")).alias("origin")).orderBy("date")

df_fl = df_fl.withColumn("state", lit("FL"))

df_fl = df_fl.filter(df_fl.county != "UNKNOWN")

save_file(df_fl, 'preprocessed', run_date, 'florida', 'parquet')

df_fl_final = df_fl.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).orderBy("date", "county")

save_file(df_fl_final, 'final', run_date, 'florida', 'parquet')

# New York Covid Data Extraction
data_ny = []

url_ny = f'https://health.data.ny.gov/resource/xdss-u53e.json?test_date={run_date}'
response_ny = requests.get(url_ny)

if response_ny.text.strip() != '[]':
    for row in response_ny.json():
        data_ny.append(row)
        
if data_ny:
    with open(f'tmp/incremental/county/{run_date}/new_york.json', 'w') as ny_file:
        json.dump(data_ny, ny_file)

# New York Covid Data Cleaning
df_ny = spark.read.format('json').load(f'tmp/incremental/county/{run_date}/new_york.json')

df_ny = df_ny.select(upper(col("county")).alias("county"), 
                    col("cumulative_number_of_positives").cast("int").alias("total_cases"),
                    col("cumulative_number_of_tests").cast("int").alias("total_tests"),
                    col("new_positives").cast("int").alias("new_cases"),
                    to_timestamp(col("test_date")).alias("date"),
                    col("total_number_of_tests").cast("int").alias("new_tests")).orderBy("test_date")

df_ny = df_ny.withColumn("state", lit("NY"))

df_ny = df_ny.filter("county != 'UNKNOWN'")

save_file(df_ny, 'preprocessed', run_date, 'new-york', 'parquet')

df_ny_final = df_ny.select("date", "county", "state", "new_cases").orderBy("date", "county")
df_ny_final.show(1000)
save_file(df_ny_final, 'final', run_date, 'new-york', 'parquet')

# Pennsylvania Covid Data Extraction
data_pa = []

url_pa = f'https://data.pa.gov/resource/j72v-r42c.json?date={run_date}'
response_pa = requests.get(url_pa)

if response_pa.text.strip() != '[]':
    for row in response_pa.json():
        data_pa.append(row)
        
if data_pa:
    with open(f'tmp/incremental/county/{run_date}/pennsylvania.json', 'w') as penn_file:
        json.dump(data_pa, penn_file)

# Pennsylvania Covid Data Cleaning
df_pa = spark.read.json(f'tmp/incremental/county/{run_date}/pennsylvania.json')

df_pa = df_pa.drop('georeferenced_lat__long', ':@computed_region_nmsq_hqvv', ':@computed_region_d3gw_znnf', ':@computed_region_amqz_jbr4', ':@computed_region_r6rf_p9et', ':@computed_region_rayf_jjgk')

df_pa = df_pa.select(col("cases").cast("int").alias("new_cases"), 
                    col("cases_avg_new").cast("float").alias("cases_avg_new"), 
                    col("cases_avg_new_rate").cast("float").alias("cases_avg_new_rate"), 
                    col("cases_cume").cast("int").alias("cases_total"),
                    col("cases_cume_rate").cast("float").alias("cases_total_rate"), 
                    upper(col("county")).alias("county"), 
                    col("latitude").cast("float").alias("latitude"), 
                    col("longitude").cast("float").alias("longitude"), 
                    col("population").cast("int").alias("population"), 
                    to_timestamp(col("date")).alias("date")).orderBy("date")

df_pa = df_pa.withColumn("state", lit("PA"))

df_pa = df_pa.filter(df_pa.county != "UNKNOWN")

save_file(df_pa, 'preprocessed', run_date, 'pennsylvania', 'parquet')

df_pa_final = df_pa.select("date", "county", "state", "new_cases").orderBy("date", "county")
df_pa_final.show(1000)
save_file(df_pa_final, 'final', run_date, 'pennsylvania', 'parquet')


# Illinois Covid Data Extraction
url_counties_il = 'https://en.wikipedia.org/wiki/List_of_counties_in_Illinois'
response_url_counties_il = requests.get(url_counties_il)
soup = BeautifulSoup(response_url_counties_il.text, 'html.parser')

counties_il = []
for county in soup.select('tbody > tr > th > a'):
    counties_il.append(county.get_text())

counties_il = [county.split(' ')[0] for county in counties_il]

data_il = []

for county in counties_il[:3]:
    url_il = 'https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetCountyHistorical?countyName={}'.format(county)
    response_il = requests.get(url_il)
    
    for row in response_il.json()['values']:
        data_il.append(row)
        
    time.sleep(60)
        
with open(f'tmp/incremental/county/{run_date}/illinois.json', 'w') as illinois_file:
    json.dump(data_il, illinois_file)  

# Illinois Covid Data Cleaning
df_il = spark.read.format('json').load(f'tmp/incremental/county/{run_date}/illinois.json')

df_il = df_il.select(upper(col("CountyName")).alias("county"),
                    col("confirmed_cases").cast("int").alias("case_total"),
                    col("deaths").cast("int").alias("deaths"),
                    col("latitude").cast("float").alias("latitude"),
                    col("longitude").cast("float").alias("longitude"),
                    to_timestamp(col("reportDate")).alias("date"),
                    col("tested").cast("int").alias("tested")).orderBy("date")

df_il = df_il.withColumn("state", lit("IL"))

df_il = df_il.filter(df_il.county != "UNKNOWN")

windowSpec = Window.partitionBy("county").orderBy("date")

df_il = df_il.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

df_il = df_il.withColumn("new_cases", (df_il.case_total - df_il.previous_day))

save_file(df_il, 'preprocessed', run_date, 'illinois', 'parquet')

df_il_final = df_il.select("date", "county", "state", "new_cases").filter(df_il.date == run_date).orderBy("date", "county")
df_il_final.show(1000)
save_file(df_il_final, 'final', run_date, 'illinois', 'parquet')


# Ohio Covid Data Extraction
url_oh = 'https://coronavirus.ohio.gov/static/dashboards/COVIDDeathData_CountyOfResidence.csv'
response_oh = requests.get(url_oh)

create_file(response_oh.text, 'ohio', 'csv', 'x', 1, run_date)

# Ohio Covid Data Cleaning
df_oh = spark.read.format('csv').option("header", True).load(f'tmp/incremental/county/{run_date}/ohio.csv')

df_oh = df_oh.drop("Admission Date", "Date of Death")

df_oh = df_oh.select(upper(col("County")).alias("county"),
                    upper(col("Sex")).alias("sex"),
                    col("Age Range").alias("age"),
                    to_timestamp(col("Onset Date")).alias("date"),
                    col("Case Count").cast("int").alias("case"),
                    col("Hospitalized Count").cast("int").alias("hospitalized"),
                    col("Death Due To Illness Count - County Of Residence").cast("int").alias("death")).filter("date IS NOT NULL").orderBy("date")

df_oh = df_oh.withColumn("state", lit("OH"))

df_oh = df_oh.filter("County != 'UNKNOWN'")

save_file(df_oh, 'preprocessed', run_date, 'ohio', 'parquet')

df_oh_final = df_oh.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).filter(df_oh.date == run_date).orderBy("date", "county")

df_oh_final.show(1000)
save_file(df_oh_final, 'final', run_date, 'ohio', 'parquet')

# Georgia Covid Data Extraction

data_ga = []

url_ga = 'https://opendata.arcgis.com/datasets/d817e06e4b264905a075b9332cd41962_0.geojson'
response_ga = requests.get(url_ga)

for row in response_ga.json()['features']:
    data_ga.append(row['properties'])
    
if data_ga:
    with open(f'tmp/incremental/county/{ga_run_date}/georgia.json', 'w') as georgia_file:
        json.dump(data_ga, georgia_file)

# Georgia Covid Data Cleaning
df_ga = spark.read.format('json').option("header", True).load(f'tmp/incremental/county/{ga_run_date}/georgia.json')

df_ga = df_ga.drop('OBJECTID', 'C_NEW_PERCT_CHG', 'D_NEW_PERCT_CHG', 'C_NEW_7D_MEAN', 'D_NEW_7D_MEAN', 'C_NEW_7D_PERCT_CHG', 'D_NEW_7D_PERCT_CHG', 'GlobalID')

df_ga = df_ga.filter("COUNTY != 'UNKNOWN'")

df_ga = df_ga.withColumn("date", to_timestamp("DATESTAMP"))

df_ga = df_ga.drop("DATESTAMP")

df_ga = df_ga.select(col("CNTY_FIPS").cast("int").alias("county_fips"),
                    upper(col("COUNTY")).alias("county"),
                    "date",
                    col("C_Age_0").cast("int").alias("cases_age_0"),
                    col("C_Age_0_4").cast("int").alias("cases_age_0_4"),
                    col("C_Age_15_24").cast("int").alias("cases_age_15_24"),
                    col("C_Age_20").cast("int").alias("cases_age_20"),
                    col("C_Age_25_34").cast("int").alias("cases_age_25_34"),
                    col("C_Age_35_44").cast("int").alias("cases_age_35_44"),
                    col("C_Age_45_54").cast("int").alias("cases_age_45_54"),
                    col("C_Age_55_64").cast("int").alias("cases_age_55_64"),
                    col("C_Age_5_14").cast("int").alias("cases_age_5_14"),
                    col("C_Age_65_74").cast("int").alias("cases_age_65_74"),
                    col("C_Age_75_84").cast("int").alias("cases_age_75_84"),
                    col("C_Age_85plus").cast("int").alias("cases_age_85plus"),
                    col("C_Age_Unkn").cast("int").alias("cases_age_unknown"),
                    col("C_Cum").cast("int").alias("cases_cumulative"),
                    col("C_EthUnk").cast("int").alias("cases_ethnicity_unknown"),
                    col("C_Female").cast("int").alias("cases_female"),
                    col("C_His").cast("int").alias("cases_hispanic"),
                    col("C_Male").cast("int").alias("cases_male"),
                    col("C_New").cast("int").alias("new_cases"),
                    col("C_NonHis").cast("int").alias("cases_nonhispanic"),
                    col("C_RaceAs").cast("int").alias("cases_asian"),
                    col("C_RaceBl").cast("int").alias("cases_black"),
                    col("C_RaceOth").cast("int").alias("cases_other"),
                    col("C_RaceUnk").cast("int").alias("cases_unknown"),
                    col("C_RaceWh").cast("int").alias("cases_white"),
                    col("C_SexUnkn").cast("int").alias("cases_sex_unknown"),
                    col("C_UCon_No").cast("int").alias("cases_condition_no"),
                    col("C_UCon_Unk").cast("int").alias("cases_condition_unknown"),
                    col("C_UCon_Yes").cast("int").alias("cases_condition_yes"),
                    col("D_Cum").cast("int").alias("deaths_cumulative"),
                    col("D_New").cast("int").alias("deaths_new"),
                    col("H_Cum").cast("int").alias("hospital_cumulative"),
                    col("H_New").cast("int").alias("hospital_new")).filter(to_date(df_ga.date) == ga_run_date).orderBy("date")

df_ga = df_ga.withColumn("state", lit("GA"))

df_ga = df_ga.filter("county != 'UNKNOWN'")

save_file(df_ga, 'preprocessed', ga_run_date, 'georgia', 'parquet')

df_ga_final = df_ga.select("date", "county", "state", "new_cases").orderBy("date", "county")

save_file(df_ga_final, 'final', ga_run_date, 'georgia', 'parquet')

# Cases Covid Data Extraction
url_usafacts_cases = 'https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv?_ga=2.249922758.1108579064.1615472677-188720245.1615472677'
response_cases = requests.get(url_usafacts_cases)

create_file(response_cases.text, 'cases', 'csv', 'x', 1, run_date)

# Cases Covid Data Cleaning
cases = pd.read_csv(f'tmp/incremental/county/{run_date}/cases.csv')

columns = []

for i in ['countyFIPS', 'County Name', 'State', 'StateFIPS']:
    columns.append(i)

for column in cases.columns[4:]:
        columns.append(datetime.strptime(column, '%Y-%m-%d'))
        
counties = cases[['countyFIPS', 'County Name', 'State', 'StateFIPS']].values

counties_converted = []

for i in range(len(counties)):
    counties_converted.append(list(counties[i]))
    
for i in counties_converted:
    i[1] = i[1].strip()
    i[2] = i[2].strip()
    
records = []

for county in counties_converted:
    for date in columns[4:]:
        records.append([county[0], county[1], county[2], county[3], date])
        
county_cases = []

for row in cases.itertuples(index=True):
    county_cases.append(row[5:])
    
case_count = []

for county in county_cases:
    for cases in county:
        case_count.append(cases)
        
for i in range(len(records)):
    records[i].append(case_count[i])
    
final_list = []

for row in records:
    final_dict = {}
    final_dict['countyFIPS'] = row[0]
    final_dict['County Name'] = row[1]
    final_dict['State'] = row[2]
    final_dict['StateFIPS'] = row[3]
    final_dict['Date'] = row[4]
    final_dict['Cases'] = row[5]
    final_list.append(final_dict)
    
cases_df = pd.DataFrame(final_list)

df_cases = spark.createDataFrame(cases_df)

df_cases = df_cases.select(col("countyFIPS").cast("int").alias("county_fips"),
                          upper(col("County Name")).alias("county"),
                          col("State").alias("state"),
                          col("StateFIPS").cast("int").alias("state_fips"),
                          col("Date").alias("date"),
                          col("Cases").cast("int").alias("case_total")).filter("state NOT IN ('FL', 'TX', 'NY', 'PA', 'IL', 'OH', 'GA')")

windowSpec = Window.partitionBy("county", "state").orderBy("date")

df_cases = df_cases.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

df_cases = df_cases.withColumn("new_cases", (df_cases.case_total - df_cases.previous_day))

df_cases = df_cases.filter(df_cases.date == run_date)

save_file(df_cases, 'preprocessed', run_date, 'cases', 'parquet')

df_cases_final = df_cases.select("date", "county", "state", "new_deaths")

save_file(df_cases_final, 'final', run_date, 'cases', 'parquet')

# Deaths Covid Data Extraction
url_usafacts_deaths = 'https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv?_ga=2.247303236.1108579064.1615472677-188720245.1615472677'
response_deaths = requests.get(url_usafacts_deaths)

create_file(response_deaths.text, 'deaths', 'csv', 'x', 1, run_date)

# Deaths Covid Data Cleaning
deaths = pd.read_csv(f'tmp/incremental/county/{run_date}/deaths.csv')

columns = []

for i in ['countyFIPS', 'County Name', 'State', 'StateFIPS']:
    columns.append(i)

for column in deaths.columns[4:]:
        columns.append(datetime.strptime(column, '%Y-%m-%d'))
        
counties = deaths[['countyFIPS', 'County Name', 'State', 'StateFIPS']].values

counties_converted = []

for i in range(len(counties)):
    counties_converted.append(list(counties[i]))
    
for i in counties_converted:
    i[1] = i[1].strip()
    i[2] = i[2].strip()
    
records = []

for county in counties_converted:
    for date in columns[4:]:
        records.append([county[0], county[1], county[2], county[3], date])
        
county_deaths = []

for row in deaths.itertuples(index=True):
    county_deaths.append(row[5:])
    
death_count = []

for county in county_deaths:
    for deaths in county:
        death_count.append(deaths)
        
for i in range(len(records)):
    records[i].append(death_count[i])
    
final_list = []

for row in records:
    final_dict = {}
    final_dict['countyFIPS'] = row[0]
    final_dict['County Name'] = row[1]
    final_dict['State'] = row[2]
    final_dict['StateFIPS'] = row[3]
    final_dict['Date'] = row[4]
    final_dict['Deaths'] = row[5]
    final_list.append(final_dict)
    
deaths_df = pd.DataFrame(final_list)

df_deaths = spark.createDataFrame(deaths_df)

df_deaths = df_deaths.select(col("countyFIPS").cast("int").alias("county_fips"),
                            upper(col("County Name")).alias("county"),
                            col("State").alias("state"),
                            col("StateFIPS").cast("int").alias("state_fips"),
                            col("Date").alias("date"),
                            col("Deaths").cast("int").alias("death_total"))

windowSpec = Window.partitionBy("county", "state").orderBy("date")

df_deaths = df_deaths.withColumn("previous_day", lag("death_total", 1).over(windowSpec))

df_deaths = df_deaths.withColumn("new_deaths", (df_deaths.death_total - df_deaths.previous_day))

df_deaths = df_deaths.filter(df_deaths.date == run_date)

save_file(df_deaths, 'preprocessed', run_date, 'deaths', 'parquet')

df_deaths_final = df_deaths.select("date", "county", "state", "new_deaths")

save_file(df_deaths_final, 'final', run_date, 'deaths', 'parquet')