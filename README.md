## Table of Contents
- [General Info](#general-info)
- [Technologies](#technologies)
- [Setup](#setup)

## General Info
In order to properly deploy resources (vaccines, medical supplies, testing / contact tracing, staffing) in response to the Covid Pandemic, my Data Engineering project will focus on tracking Covid cases across the United States at the country, state and county level. Additionally, I plan to gather and show the impact Covid has had on financial markets (individual stocks) and the United States economy.

For the 8 most populous states, I relied on state provided data. For the remaining states, I relied on third-party provided data. 

- California (deprecated)
- Texas (https://dshs.texas.gov/coronavirus/AdditionalData.aspx)
- Florida (https://open-fdoh.hub.arcgis.com/datasets/florida-covid19-case-line-data-2020 and https://open-fdoh.hub.arcgis.com/datasets/florida-covid19-case-line-data-2021-1)
- New York (https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Testing/xdss-u53e/data)
- Pennsylvania (https://data.pa.gov/Health/COVID-19-Aggregate-Cases-Current-Daily-County-Heal/j72v-r42c/data)
- Illinois (http://dph.illinois.gov/content/covid-19-county-cases-tests-and-deaths-day)
- Ohio (https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards)
- Georgia (https://covid-hub.gio.georgia.gov/datasets/georgia-covid-19-case-data)
- Remaining Stats (https://usafacts.org/visualizations/coronavirus-covid-19-spread-map/)

For stock market and economic indicator data, I used:

- AlphaVantage (https://rapidapi.com/alphavantage/api/Alpha%20Vantage)
- U.S. Economic Indicators (https://rapidapi.com/alphawave/api/U.S.%20Economic%20Indicators)

I started by prototyping my data pipeline using one master OOP script invoking each activity of the pipeline without user intervention. I relied heavily on Python to facilitate data extraction and Pandas for data cleaning and processing. In the next iteration, I updated my data pipeline to primarily use PySpark for data cleaning and processing. In the most recent iteration, I performed the following:

- Created an Apache Airflow DAG for data pipeline facilitation
- Created and configured Azure VM to handle data extration and ingestion into Azure Blob
- Created and configured Azure HDInsight Spark cluster using script actions and Ambari
- Performed data cleaning and processing on HDInsight Spark cluster
- Write processed data to MySQL instance on Azure

## Technologies
Project is created with: 
* Python (supporting libraries)
* Pandas
* PySpark
* Pytest
* Azure Blob Storage
* Azure Databricks
* Azure HDInsight
* Apache Airflow
* MySQL on Azure
* cloudpathlib