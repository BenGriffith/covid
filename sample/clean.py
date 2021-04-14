import pandas as pd
import numpy as np
import pyspark
import warnings
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from logs import log
from pathlib import Path

warnings.filterwarnings('ignore')

class CleanAndStore:

    def __init__(self, load, load_type, save_path):
        self.df = self.load_file(load, load_type)
        self.save_path = save_path

    def load_file(self, load, load_type):
        """
        Load file from tmp storage
        """
        if load_type == 'csv':
            self.df = spark.read.format(load_type).option("header", True).load(load)
            
            # Output to log
            log.logging.info('{} file loaded for cleaning'.format(load))

            return self.df

        elif load_type == 'json':
            self.df = spark.read.format(load_type).load(load)

            # Output to log
            log.logging.info('{} file loaded for cleaning'.format(load))

            return self.df

    def load_excel(self, load, columns, rows):
        self.df = pd.read_excel(load, names=columns, skiprows=rows)

        # Output to log
        log.logging.info('{} file loaded for cleaning'.format(load))

        return self.df

    def save_file(self, option, name, ext):
        self.df.write.format(ext).save(f'{self.save_path}/{option}/{name}')

        # Output to log
        log.logging.info(f'Cleaning and File Creation for {type(self).__name__} complete: {self.df.count()} records and {len(self.df.columns)} fields')


class Florida(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.year = load[-9:-5]
        self.wrangle()

    def wrangle(self):

        self.df = self.df.select(col("Age").cast("int").alias("age"),
                                when(col("Case_") == "Yes", 1).alias("case"),
                                upper(col("Contact")).alias("contact"),
                                upper(col("County")).alias("county"),
                                upper(col("Died")).alias("died"),
                                upper(col("EDvisit")).alias("ed_visit"),
                                to_date(to_timestamp(from_unixtime(substring(col("EventDate").cast("string"), 1, 10)))).alias("date"),
                                upper(col("Gender")).alias("gender"),
                                upper(col("Origin")).alias("origin")).distinct().orderBy("date")

        self.df = self.df.withColumn("state", lit("FL"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        # Write preprocessed
        super().save_file('preprocessed', f'florida-{self.year}', 'parquet')

        self.df = self.df.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).orderBy("date", "county")

        # Write final
        super().save_file('final', f'florida-{self.year}', 'parquet')

class Texas(CleanAndStore):

    def __init__(self, load, save_path):
        self.save_path = save_path
        self.columns = []
        self.set_column_names(load)
        super().load_excel(load, self.columns, 2)
        self.wrangle()

    def set_column_names(self, load):
        """
        Transform column names so that the names can be referenced in each row
        """

        df_columns = pd.read_excel(load, skiprows=2)

        self.columns = []
        self.columns.append('County Name')

        for column in df_columns.columns[1:]:
            self.columns.append(datetime.strptime(column.replace('Cases', ' ').lstrip(), '%m-%d-%Y'))

    def wrangle(self):
        """
        Convert data frame sourced from Excel into JSON format
        """

        drop_rows = self.df.iloc[254:]

        self.df = self.df.drop(drop_rows.index, axis=0)

        counties = self.df['County Name'].tolist()

        records = []

        for county in counties:
            for date in self.columns[1:]:
                records.append([date, county])

        county_cases = []

        for row in self.df.itertuples(index=True):
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

        self.df = pd.DataFrame(final_list)

        self.df = spark.createDataFrame(self.df)

        self.df = self.df.select(to_date("date").alias("date"), 
                                upper(col("county")).alias("county"), 
                                col("case_total").cast("int")).distinct()

        self.df = self.df.withColumn("state", lit("TX"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        windowSpec = Window.partitionBy("county").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_cases", (self.df.case_total - self.df.previous_day))

        # Write preprocessed
        super().save_file('preprocessed', 'texas', 'parquet')

        self.df = self.df.select("date", "county", "state", "new_cases")

        # Write final
        super().save_file('final', 'texas', 'parquet')
        

class NewYork(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.wrangle()

    def wrangle(self):

        self.df = self.df.select(upper(col("county")).alias("county"), 
                                col("cumulative_number_of_positives").cast("int").alias("total_cases"),
                                col("cumulative_number_of_tests").cast("int").alias("total_tests"),
                                col("new_positives").cast("int").alias("new_cases"),
                                to_date(to_timestamp(col("test_date"))).alias("date"),
                                col("total_number_of_tests").cast("int").alias("new_tests")).distinct().orderBy("test_date")

        self.df = self.df.withColumn("state", lit("NY"))

        self.df = self.df.filter("county != 'UNKNOWN'")

        # Write preprocessed
        super().save_file('preprocessed', 'new-york', 'parquet')

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        # Write final
        super().save_file('final', 'new-york', 'parquet')

class Pennsylvania(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.wrangle()

    def wrangle(self):

        self.df = self.df.drop('georeferenced_lat__long', ':@computed_region_nmsq_hqvv', ':@computed_region_d3gw_znnf', ':@computed_region_amqz_jbr4', ':@computed_region_r6rf_p9et', ':@computed_region_rayf_jjgk')

        self.df = self.df.select(col("cases").cast("int").alias("new_cases"), 
                     col("cases_avg_new").cast("float").alias("cases_avg_new"), 
                     col("cases_avg_new_rate").cast("float").alias("cases_avg_new_rate"), 
                     col("cases_cume").cast("int").alias("cases_total"),
                     col("cases_cume_rate").cast("float").alias("cases_total_rate"), 
                     upper(col("county")).alias("county"), 
                     col("latitude").cast("float").alias("latitude"), 
                     col("longitude").cast("float").alias("longitude"), 
                     col("population").cast("int").alias("population"), 
                     to_date(to_timestamp(col("date"))).alias("date")).distinct()

        self.df = self.df.withColumn("state", lit("PA"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        # Write preprocessed
        super().save_file('preprocessed', 'pennsylvania', 'parquet')

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        # Write final
        super().save_file('final', 'pennsylvania', 'parquet')

class Illinois(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.wrangle()

    def wrangle(self):

        self.df = self.df.select(upper(col("CountyName")).alias("county"),
                                col("confirmed_cases").cast("int").alias("case_total"),
                                col("deaths").cast("int").alias("deaths"),
                                col("latitude").cast("float").alias("latitude"),
                                col("longitude").cast("float").alias("longitude"),
                                to_date(to_timestamp(col("reportDate"))).alias("date"),
                                col("tested").cast("int").alias("tested")).distinct().orderBy("date")

        self.df = self.df.withColumn("state", lit("IL"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        windowSpec = Window.partitionBy("county").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_cases", (self.df.case_total - self.df.previous_day))
        
        # Write preprocessed
        super().save_file('preprocessed', 'illinois', 'parquet')

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        # Write final
        super().save_file('final', 'illinois', 'parquet')

class Ohio(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.wrangle()

    def wrangle(self):

        self.df = self.df.drop("Admission Date", "Date of Death")

        self.df = self.df.select(upper(col("County")).alias("county"),
                                upper(col("Sex")).alias("sex"),
                                col("Age Range").alias("age"),
                                to_date(to_timestamp(col("Onset Date"))).alias("date"),
                                col("Case Count").cast("int").alias("case"),
                                col("Hospitalized Count").cast("int").alias("hospitalized"),
                                col("Death Due To Illness Count - County Of Residence").cast("int").alias("death")).filter("date IS NOT NULL").distinct().orderBy("date")

        self.df = self.df.withColumn("state", lit("OH"))

        self.df = self.df.filter("County != 'UNKNOWN'")

        # Write preprocessed
        super().save_file('preprocessed', 'ohio', 'parquet')

        self.df = self.df.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).orderBy("date", "county")

        # Write final
        super().save_file('final', 'ohio', 'parquet')

class Georgia(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.wrangle()

    def wrangle(self):

        self.df = self.df.drop('OBJECTID', 'C_NEW_PERCT_CHG', 'D_NEW_PERCT_CHG', 'C_NEW_7D_MEAN', 'D_NEW_7D_MEAN', 'C_NEW_7D_PERCT_CHG', 'D_NEW_7D_PERCT_CHG', 'GlobalID')
        
        self.df = self.df.filter("COUNTY != 'UNKNOWN'")

        self.df = self.df.withColumn("date", col("DATESTAMP").cast("string"))
        self.df = self.df.drop("DATESTAMP")

        self.df = self.df.select(col("CNTY_FIPS").cast("int").alias("county_fips"),
                                upper(col("COUNTY")).alias("county"),
                                to_date(from_unixtime(col("date")[1:10])).alias("date"),
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
                                col("H_New").cast("int").alias("hospital_new")).distinct().orderBy("date")

        self.df = self.df.withColumn("state", lit("GA"))

        self.df = self.df.filter("county != 'UNKNOWN'")

        # Write preprocessed
        super().save_file('preprocessed', 'georgia', 'parquet')        

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        # Write final
        super().save_file('final', 'georgia', 'parquet')

class Cases(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.columns = []
        self.set_column_names()
        self.wrangle()

    def load_file(self, load, load_type):
        """
        Load file from tmp storage
        """
        self.df = pd.read_csv(load)
            
        # Output to log
        log.logging.info('{} file loaded for cleaning'.format(load))

        return self.df   

    def set_column_names(self):
        """
        Transform column names so that the names can be referenced in each row
        """

        for i in ['countyFIPS', 'County Name', 'State', 'StateFIPS']:
            self.columns.append(i)

        for column in self.df.columns[4:]:
            self.columns.append(datetime.strptime(column, '%Y-%m-%d'))

    def wrangle(self):
        """
        Convert data frame sourced from Excel into JSON format
        """

        counties = self.df[['countyFIPS', 'County Name', 'State', 'StateFIPS']].values

        counties_converted = []

        for i in range(len(counties)):
            counties_converted.append(list(counties[i]))

        for i in counties_converted:
            i[1] = i[1].strip()
            i[2] = i[2].strip()

        records = []

        for county in counties_converted:
            for date in self.columns[4:]:
                records.append([county[0], county[1], county[2], county[3], date])

        county_cases = []

        for row in self.df.itertuples(index=True):
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
            final_dict['CountyName'] = row[1]
            final_dict['State'] = row[2]
            final_dict['StateFIPS'] = row[3]
            final_dict['Date'] = row[4]
            final_dict['Cases'] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

        self.df = spark.createDataFrame(self.df)

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_fips"),
                                upper(col("CountyName")).alias("county"),
                                col("State").alias("state"),
                                col("StateFIPS").cast("int").alias("state_fips"),
                                to_date(col("Date")).alias("date"),
                                col("Cases").cast("int").alias("case_total")).filter("state NOT IN ('FL', 'TX', 'NY', 'PA', 'IL', 'OH', 'GA')").distinct()

        # Write preprocessed
        super().save_file('preprocessed', 'cases', 'parquet')  

        windowSpec = Window.partitionBy("county", "state").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_cases", (self.df.case_total - self.df.previous_day))    

        self.df = self.df.select("date", "county", "state", "new_cases")                  

        # Write final
        super().save_file('final', 'cases', 'parquet')

class Deaths(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.columns = []
        self.set_column_names()
        self.wrangle()

    def load_file(self, load, load_type):
        """
        Load file from tmp storage
        """
        self.df = pd.read_csv(load)
            
        # Output to log
        log.logging.info('{} file loaded for cleaning'.format(load))

        return self.df   

    def set_column_names(self):
        """
        Transform column names so that the names can be referenced in each row
        """

        for i in ['countyFIPS', 'County Name', 'State', 'StateFIPS']:
            self.columns.append(i)

        for column in self.df.columns[4:]:
            self.columns.append(datetime.strptime(column, '%Y-%m-%d'))

    def wrangle(self):
        """
        Convert data frame sourced from Excel into JSON format
        """

        counties = self.df[['countyFIPS', 'County Name', 'State', 'StateFIPS']].values

        counties_converted = []

        for i in range(len(counties)):
            counties_converted.append(list(counties[i]))

        for i in counties_converted:
            i[1] = i[1].strip()
            i[2] = i[2].strip()

        records = []

        for county in counties_converted:
            for date in self.columns[4:]:
                records.append([county[0], county[1], county[2], county[3], date])

        county_deaths = []

        for row in self.df.itertuples(index=True):
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
            final_dict['CountyName'] = row[1]
            final_dict['State'] = row[2]
            final_dict['StateFIPS'] = row[3]
            final_dict['Date'] = row[4]
            final_dict['Deaths'] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

        self.df = spark.createDataFrame(self.df)

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_fips"),
                                upper(col("CountyName")).alias("county"),
                                col("State").alias("state"),
                                col("StateFIPS").cast("int").alias("state_fips"),
                                to_date(col("Date")).alias("date"),
                                col("Deaths").cast("int").alias("death_total")).distinct()

        # Write preprocessed
        super().save_file('preprocessed', 'deaths', 'parquet')   

        windowSpec = Window.partitionBy("county", "state").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("death_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_deaths", (self.df.death_total - self.df.previous_day))         

        self.df = self.df.select("date", "county", "state", "new_deaths")

        # Write final
        super().save_file('final', 'deaths', 'parquet')

class Population(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_fips"),
                                upper(col("County Name")).alias("county"),
                                "state",
                                col("population").cast("int").alias("population")).distinct()

        # Write final
        super().save_file('final', 'population', 'parquet')

class Stocks:

    def __init__(self, load, save_path):
        self.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        self.wrangle(load, save_path)

    def wrangle(self, load, save_path):

        for stock in Path(load).glob('*/*.json'):

            self.df = pd.read_json(stock, orient='index')

            self.df.reset_index(inplace=True)

            self.df = spark.createDataFrame(self.df, self.columns)

            self.df = self.df.select(to_date("date").alias("date"), 
                                    col("open").cast("float").alias("open"),
                                    col("high").cast("float").alias("high"),
                                    col("low").cast("float").alias("low"),
                                    col("close").cast("float").alias("close"),
                                    col("volume").cast("float").alias("volume")).orderBy("date")

            # Write final
            self.df.write.format('parquet').save(f'{save_path}/{stock.parent.name}/final/{stock.stem}')

class Indicator(CleanAndStore):

    def __init__(self, load, load_type, save_path, indicator):
        super().__init__(load, load_type, save_path)
        self.indicator = indicator
        self.wrangle()

    def load_file(self, load, load_type):
        """
        Load file from tmp storage
        """
        self.df = pd.read_json(load)
            
        # Output to log
        log.logging.info('{} file loaded for cleaning'.format(load))

        return self.df 

    def wrangle(self):

        self.df.reset_index(inplace=True)

        self.df = spark.createDataFrame(self.df, ["date", self.indicator])

        self.df = self.df.select(to_date("date").alias("date"), col(self.indicator).cast("float").alias(self.indicator)).orderBy("date")

        # Write final
        super().save_file('final', self.indicator, 'parquet')