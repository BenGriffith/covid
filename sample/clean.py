import pandas as pd
import numpy as np
import pyspark
import warnings
import logging
import utils
import os
import shutil
import mysql.connector
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from cloudpathlib import CloudPath, AzureBlobClient
from io import StringIO
from datetime import datetime
from logs import log
from pathlib import Path

warnings.filterwarnings("ignore")

spark = SparkSession.builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.24").getOrCreate()
spark.conf.set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/anaconda/envs/py36booyah/bin/python3")
spark.conf.set("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/anaconda/envs/py36booyah/bin/python3")
spark.conf.set(f"fs.azure.account.key.{utils.storage_account}.blob.core.windows.net", utils.key)

class CleanAndStore:

    def __init__(self, load, load_type, save_path, run_date=None):
        self.df = self.load_file(load, load_type)
        self.save_path = save_path
        self.run_date = run_date
        
    def load_file(self, load, load_type):
        """
        Retrieve CSV or JSON file from Azure
        """

        if load_type == "csv":

            self.df = spark.read.format(load_type).option("header", True).load(f"wasbs://{utils.container_name}@{utils.storage_account}.blob.core.windows.net/{load}")
            
            # Output to log
            log.logging.info("{} file loaded for cleaning".format(load))

            return self.df

        elif load_type == "json":

            self.df = spark.read.format(load_type).option("header", True).load(f"wasbs://{utils.container_name}@{utils.storage_account}.blob.core.windows.net/{load}")

            # Output to log
            log.logging.info("{} file loaded for cleaning".format(load))

            return self.df

    def load_excel(self, load, rows, columns=None):
        """
        1. Establish connection
        2. Download blob
        3. Create Pandas data frame from Excel file
        """

        # Establish connection
        blob_service_client = BlobServiceClient(account_url=utils.blob_url, credential=utils.key)

        # Download blob
        blob_client = blob_service_client.get_blob_client(container=utils.container_name, blob=load)
        downloader = blob_client.download_blob()
        
        # Create Pandas data frame
        if columns:
            self.df = pd.read_excel(downloader.readall(), engine="openpyxl", names=columns, skiprows=rows)
            
            # Output to log
            log.logging.info("{} file loaded for cleaning".format(load))

            return self.df
        else:
            df = pd.read_excel(downloader.readall(), engine="openpyxl", skiprows=rows)

            # Output to log
            log.logging.info("{} file loaded for cleaning".format(load))
            
            return df

    def save_file(self, name):
        """ 
        1. Write parquet files to Azure blob
        """

        save_path = f"{self.save_path}/{name}"

        # Create parquet files
        self.df.write.parquet(f"wasbs://{utils.container_name}@{utils.storage_account}.blob.core.windows.net/{save_path}")

        # Output to log
        log.logging.info(f"Cleaning and File Creation for {type(self).__name__} complete: {self.df.count()} records and {len(self.df.columns)} fields")

    @staticmethod
    @udf(returnType=IntegerType())
    def get_county_id(county, state):
    
        try:
            connection = mysql.connector.connect(
                user=utils.db_user, 
                password=utils.db_pw, 
                host=utils.db_host, 
                port=utils.db_port, 
                database=utils.db_name)

            cursor = connection.cursor()
            cursor.execute("SELECT county_id FROM county WHERE county = %s and state = %s;", (county, state))
            rows = cursor.fetchall()
            county_id = rows[0]

            cursor.close()
            connection.close()
            
            return county_id[0]

        except mysql.connector.Error as err:

            # Output to log
            log.logging.info(f"CONNECTION ERROR: {err}")

    def replace_county_name_with_id(self):
        
        self.df = self.df.withColumn("county_id", self.get_county_id(col("county"), col("state")))
        self.df = self.df.select("date", "county_id", "new_cases")

        return self.df

    def write_to_mysql(self, table):
        """
        Write data to specified DB table
        """

        self.df.write.format('jdbc').options(
            url=utils.db_url, 
            driver=utils.db_driver, 
            dbtable=table, 
            user=utils.db_user, 
            password=utils.db_pw).mode("append").save()

class Florida(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.year = load[-9:-5]
        self.wrangle()

    def wrangle(self):
        """
        Florida data cleaning, transformation and file creation
        """

        self.df = self.df.select(col("Age").cast("int").alias("age"),
                                when(col("Case_") == "Yes", 1).alias("case"),
                                upper(col("Contact")).alias("contact"),
                                when(upper(col("County")) == "DADE", "MIAMI-DADE").otherwise(upper(self.df.County)).alias("county"),
                                upper(col("Died")).alias("died"),
                                upper(col("EDvisit")).alias("ed_visit"),
                                to_date(to_timestamp(from_unixtime(substring(col("EventDate").cast("string"), 1, 10)))).alias("date"),
                                upper(col("Gender")).alias("gender"),
                                upper(col("Origin")).alias("origin")).distinct().orderBy("date")

        self.df = self.df.withColumn("state", lit("FL"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        # Write preprocessed
        super().save_file(f"florida{self.year}")

        self.df = self.df.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Texas(CleanAndStore):

    def __init__(self, load, save_path, run_date):
        self.save_path = save_path
        self.run_date = run_date
        self.columns = []
        self.set_column_names(load)
        super().load_excel(load, 2, self.columns)
        self.wrangle()

    def set_column_names(self, load):
        """
        1. Create data frame in order to get column names
        2. Remove Cases from column name and apply date format
        """

        df_columns = super().load_excel(load, 2)

        self.columns = []
        self.columns.append("County Name")

        for column in df_columns.columns[1:]:
            self.columns.append(datetime.strptime(column.replace("Cases", " ").lstrip(), "%m-%d-%Y"))

    def wrangle(self):
        """
        1. Convert data frame sourced from Excel into JSON format
        2. Create PySpark data fram, data cleaning, transformation and file creation
        """

        drop_rows = self.df.iloc[254:]

        self.df = self.df.drop(drop_rows.index, axis=0)

        counties = self.df["County Name"].tolist()

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
            final_dict["date"] = row[0]
            final_dict["county"] = row[1]
            final_dict["case_total"] = row[2]
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

        if self.run_date == None:

            self.df = self.df.select("date", "county", "state", "new_cases")

        else:

            self.df = self.df.select("date", "county", "state", "new_cases").filter(self.df.date == self.run_date)

        super().replace_county_name_with_id()
        
        # Write to MySQL
        super().write_to_mysql("cases")
        

class NewYork(CleanAndStore):
    
    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.wrangle()

    def wrangle(self):
        """
        New York data cleaning, transformation and file creation
        """

        self.df = self.df.select(upper(col("county")).alias("county"), 
                                col("cumulative_number_of_positives").cast("int").alias("total_cases"),
                                col("cumulative_number_of_tests").cast("int").alias("total_tests"),
                                col("new_positives").cast("int").alias("new_cases"),
                                to_date(to_timestamp(col("test_date"))).alias("date"),
                                col("total_number_of_tests").cast("int").alias("new_tests")).distinct().orderBy("test_date")

        self.df = self.df.withColumn("state", lit("NY"))

        self.df = self.df.filter("county != 'UNKNOWN'")

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Pennsylvania(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        #self.wrangle()

    def wrangle(self):
        """
        Pennsylvania data cleaning, transformation and file creation
        """

        self.df = self.df.drop("georeferenced_lat__long", ":@computed_region_nmsq_hqvv", ":@computed_region_d3gw_znnf", ":@computed_region_amqz_jbr4", ":@computed_region_r6rf_p9et", ":@computed_region_rayf_jjgk")

        self.df = self.df.select(col("cases").cast("int").alias("new_cases"), 
                     col("cases_avg_new").cast("float").alias("cases_avg_new"), 
                     col("cases_avg_new_rate").cast("float").alias("cases_avg_new_rate"), 
                     col("cases_cume").cast("int").alias("cases_total"),
                     col("cases_cume_rate").cast("float").alias("cases_total_rate"), 
                     when(upper(col("county")) == "PENNSYLVANIA", "PHILADELPHIA").otherwise(upper(self.df.county)).alias("county"),
                     col("latitude").cast("float").alias("latitude"), 
                     col("longitude").cast("float").alias("longitude"), 
                     col("population").cast("int").alias("population"), 
                     to_date(to_timestamp(col("date"))).alias("date")).distinct()

        self.df = self.df.withColumn("state", lit("PA"))

        self.df = self.df.filter(self.df.county != "UNKNOWN")

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Illinois(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.wrangle()

    def wrangle(self):
        """
        Illinois data cleaning, transformation and file creation
        """

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

        if self.run_date == None:

            self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        else:

            self.df = self.df.select("date", "county", "state", "new_cases").filter(self.df.date == self.run_date).orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Ohio(CleanAndStore):
    
    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.wrangle()

    def wrangle(self):
        """
        Ohio data cleaning, transformation and file creation
        """

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

        if self.run_date == None:

            self.df = self.df.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).orderBy("date", "county")

        else:

            self.df = self.df.select("date", "county", "state", "case").groupBy("date", "county", "state").agg(count("case").cast("int").alias("new_cases")).filter(self.df.date == self.run_date).orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Georgia(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.wrangle()

    def wrangle(self):
        """
        Georgia data cleaning, transformation and file creation
        """

        self.df = self.df.drop("OBJECTID", "C_NEW_PERCT_CHG", "D_NEW_PERCT_CHG", "C_NEW_7D_MEAN", "D_NEW_7D_MEAN", "C_NEW_7D_PERCT_CHG", "D_NEW_7D_PERCT_CHG", "GlobalID")
        
        self.df = self.df.filter("COUNTY NOT IN ('UNKNOWN', 'ALL GEORGIA', 'NON-GEORGIA RESIDENT')")

        self.df = self.df.withColumn("date", col("DATESTAMP").cast("string"))
        self.df = self.df.drop("DATESTAMP")

        if self.run_date == None:

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

        else:

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
                                    col("H_New").cast("int").alias("hospital_new")).filter(to_date(self.df.date) == self.run_date).orderBy("date")

        self.df = self.df.withColumn("state", lit("GA"))

        self.df = self.df.filter("county != 'UNKNOWN'")   

        self.df = self.df.select("date", "county", "state", "new_cases").orderBy("date", "county")

        super().replace_county_name_with_id()

        # Write to MySQL
        super().write_to_mysql("cases")

class Cases(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.columns = []
        self.set_column_names()
        self.wrangle()

    def load_file(self, load, load_type):
        """
        1. Establish connection
        2. Download blob
        3. Create Pandas data frame
        """
        container_client = ContainerClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name)
        
        # Download blob as StorageStreamDownloader object (stored in memory)
        downloaded_blob = container_client.download_blob(f"{load}")

        self.df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            
        # Output to log
        log.logging.info("{} file loaded for cleaning".format(load))

        return self.df   

    def set_column_names(self):
        """
        Transform column names so that the names can be referenced in each row
        """

        for i in ["countyFIPS", "County Name", "State", "StateFIPS"]:
            self.columns.append(i)

        for column in self.df.columns[4:]:
            self.columns.append(datetime.strptime(column, "%Y-%m-%d"))

    def wrangle(self):
        """
        1. Convert data frame sourced from Excel into JSON format
        2. Create PySpark data frame, data cleaning, transformation and file creation
        """

        counties = self.df[["countyFIPS", "County Name", "State", "StateFIPS"]].values

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
            final_dict["countyFIPS"] = row[0]
            final_dict["CountyName"] = row[1]
            final_dict["State"] = row[2]
            final_dict["StateFIPS"] = row[3]
            final_dict["Date"] = row[4]
            final_dict["Cases"] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

        self.df = spark.createDataFrame(self.df)

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_id"),
                                upper(trim(regexp_replace(col("CountyName"), "County", ""))).alias("county"),
                                col("State").alias("state"),
                                col("StateFIPS").cast("int").alias("state_fips"),
                                to_date(col("Date")).alias("date"),
                                col("Cases").cast("int").alias("case_total")).filter("state NOT IN ('FL', 'TX', 'NY', 'PA', 'OH', 'GA')").distinct()

        windowSpec = Window.partitionBy("county", "state").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("case_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_cases", (self.df.case_total - self.df.previous_day))    

        if self.run_date == None:
        
            self.df = self.df.select("date", "county_id", "county", "state", "new_cases")                  

        else:

            self.df = self.df.select("date", "county_id", "county", "state", "new_cases").filter(self.df.date == self.run_date)            

        self.df = self.df.select("date", "county_id", "new_cases")

        # Write to MySQL
        super().write_to_mysql("cases")

class Deaths(CleanAndStore):

    def __init__(self, load, load_type, save_path, run_date):
        super().__init__(load, load_type, save_path, run_date)
        self.columns = []
        self.set_column_names()
        self.wrangle()

    def load_file(self, load, load_type):
        """
        1. Establish connection
        2. Download blob
        3. Create Pandas data frame
        """

        container_client = ContainerClient.from_connection_string(conn_str=utils.connection_string, container_name=utils.container_name)
        
        # Download blob as StorageStreamDownloader object (stored in memory)
        downloaded_blob = container_client.download_blob(f"{load}")

        self.df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            
        # Output to log
        log.logging.info("{} file loaded for cleaning".format(load))

        return self.df   

    def set_column_names(self):
        """
        Transform column names so that the names can be referenced in each row
        """

        for i in ["countyFIPS", "County Name", "State", "StateFIPS"]:
            self.columns.append(i)

        for column in self.df.columns[4:]:
            self.columns.append(datetime.strptime(column, "%Y-%m-%d"))

    def wrangle(self):
        """
        1. Convert data frame sourced from Excel into JSON format
        2. Create PySpark data frame, data cleaning, transformatio and file creation
        """

        counties = self.df[["countyFIPS", "County Name", "State", "StateFIPS"]].values

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
            final_dict["countyFIPS"] = row[0]
            final_dict["CountyName"] = row[1]
            final_dict["State"] = row[2]
            final_dict["StateFIPS"] = row[3]
            final_dict["Date"] = row[4]
            final_dict["Deaths"] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

        self.df = spark.createDataFrame(self.df)

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_id"),
                                upper(trim(regexp_replace(col("CountyName"), "County", ""))).alias("county"),
                                col("State").alias("state"),
                                col("StateFIPS").cast("int").alias("state_fips"),
                                to_date(col("Date")).alias("date"),
                                col("Deaths").cast("int").alias("death_total")).distinct()

        windowSpec = Window.partitionBy("county", "state").orderBy("date")

        self.df = self.df.withColumn("previous_day", lag("death_total", 1).over(windowSpec))

        self.df = self.df.withColumn("new_deaths", (self.df.death_total - self.df.previous_day))     

        if self.run_date == None:    

            self.df = self.df.select("date", "county_id", "county", "state", "new_deaths")

        else:

            self.df = self.df.select("date", "county_id", "county", "state", "new_deaths").filter(self.df.date == self.run_date)

        self.df = self.df.select("date", "county_id", "new_deaths")

        # Write to MySQL
        super().write_to_mysql("deaths")

class Population(CleanAndStore):

    def __init__(self, load, load_type, save_path, action=0):
        super().__init__(load, load_type, save_path)
        self.wrangle(action)
        
    def wrangle(self, action):

        self.df = self.df.select(col("countyFIPS").cast("int").alias("county_id"),
                                upper(col("County Name")).alias("county"),
                                "state",
                                col("population").cast("int").alias("population")).distinct()

        if action == 1:

            self.df = self.df.select(col("county_id"), trim(regexp_replace(col("county"), "COUNTY", "")).alias("county"), "state")
            super().write_to_mysql("county")

        else:

            self.df = self.df.select("county_id", "population")

            # Write to MySQL
            super().write_to_mysql("population")

class Stocks:

    def __init__(self, load, save_path):
        self.columns = ["date", "open", "high", "low", "close", "volume"]
        self.wrangle(load, save_path)

    def write_to_mysql(self, table):
        """
        Write data to specified DB table
        """

        self.df.write.format('jdbc').options(
            url=utils.db_url, 
            driver=utils.db_driver, 
            dbtable=table, 
            user=utils.db_user, 
            password=utils.db_pw).mode("append").save()

    def wrangle(self, load, save_path):
        """
        1. Retrieve path
        2. Loop through all .json files (daily and weekly) for each stock
        3. Create PySpark data frame from .json file
        4. Data cleaning and manipulation
        5. Establish connection and write to DB
        """

        client = AzureBlobClient(connection_string=utils.connection_string)
        root = client.CloudPath(f"az://{utils.container_name}/{load}")
        
        for stock in root.glob("**/*.json"):

            stock = str(stock)[15:]
            end = stock.rfind("/")
            start = stock[:end].rfind("/")
            stock_symbol = stock[start+1:end].upper()

            self.df = spark.read.json(f"wasbs://{utils.container_name}@{utils.storage_account}.blob.core.windows.net/{stock}")
            
            df_filename = self.df.withColumn("filename", input_file_name())
            filename = df_filename.select(col("filename")).first()
            temp = filename[0][filename[0].rfind("/") + 1: filename[0].rfind(".")]

            self.df = self.df.select(to_date("date").alias("date"), 
                                    col("open").cast("float").alias("open"),
                                    col("high").cast("float").alias("high"),
                                    col("low").cast("float").alias("low"),
                                    col("close").cast("float").alias("close"),
                                    col("volume").cast("float").alias("volume")).orderBy("date")
            
            self.df = self.df.withColumn("stock", lit(stock_symbol))

            if temp == "daily":
                self.write_to_mysql("stocks_daily")
            elif temp == "weekly":
                self.write_to_mysql("stocks_weekly")

class Indicator(CleanAndStore):

    def __init__(self, load, load_type, save_path, indicator):
        super().__init__(load, load_type, save_path)
        self.indicator = indicator
        self.wrangle()

    def load_file(self, load, load_type):
        """
        Retrieve file from Azure
        """
        self.df = spark.read.format(load_type).option("header", True).load(f"wasbs://{utils.container_name}@{utils.storage_account}.blob.core.windows.net/{load}")

        # Output to log
        log.logging.info("{} file loaded for cleaning".format(load))

        return self.df

    def wrangle(self):

        self.df = self.df.select(to_date("date").alias("date"), col(self.indicator).cast("float").alias(self.indicator)).orderBy("date")

        # Write final
        super().write_to_mysql(self.indicator)