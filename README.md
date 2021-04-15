## Table of Contents
- [General Info](#general-info)
- [Technologies](#technologies)
- [Setup](#setup)

## General Info
In order to properly deploy resources (vaccines, medical supplies, testing / contact tracing, staffing) in response to the Covid Pandemic, my Data Engineering project will focus on tracking Covid cases across the United States at the country, state and county level. Depending on available datasets, county level would apply to all 50 states.

Additionally, I plan to show the impact Covid has had on financial markets (individual stocks) and the United States economy.

I started by prototyping my data pipeline using one master OOP script invoking each activity of the pipeline without user intervention. I relied heavily on Python to facilitate data extraction and Pandas for data cleaning and processing.

I then went through the process of updating my data pipeline to use PySpark so that PySpark would primarily be responsible for data cleaning and processing instead of Pandas. At a high level, my project consists of the following stages:

- Extract Covid Case data and S&P 500 stock market data from 18 different sources
- Temporarily store created file before uploading to Azure Blob Storage
- Created Spark Cluster
- Read from Azure Blob Storage
- Perform data cleaning and processing
- Write clean data to Azure Blob Storage in Parquet format

## Technologies
Project is created with: 
* Python (supporting libraries)
* Pandas
* PySpark
* Azure Blob Storage
* Azure Databricks
* cloudpathlib

## Setup
To run this project, follow the steps below:

```
$ git clone https://github.com/BenGriffith/covid.git
$ cd sample
$ python3 driver.py