import pandas as pd
import numpy as np
import json
import warnings
import logging
from datetime import datetime
from logs import log

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
            self.df = pd.read_csv(load)
            
            # Output to log
            log.logging.info('{} file loaded for cleaning'.format(load))

            # Output to console
            print('{} file loaded for cleaning'.format(load))
            return self.df
        elif load_type == 'json':
            self.df = pd.read_json(load)

            # Output to log
            log.logging.info('{} file loaded for cleaning'.format(load))

            # Output to console
            print('{} file loaded for cleaning'.format(load))
            return self.df

    def load_excel(self, load, columns, rows):
        self.df = pd.read_excel(load, names=columns, skiprows=rows)

        # Output to log
        log.logging.info('{} file loaded for cleaning'.format(load))

        # Output to console
        print('{} file loaded for cleaning'.format(load))
        return self.df

    def save_file(self, name, ext):
        self.df.to_json(f'{self.save_path}/{name}.{ext}', orient='records')

        # Output to log
        log.logging.info(f'Cleaning and File Creation for {type(self).__name__} complete: {self.df.shape[0]} records and {self.df.shape[1]} fields')

        # Output to console
        print(f'Cleaning and File Creation for {type(self).__name__} complete: {self.df.shape[0]} records and {self.df.shape[1]} fields')

    def upper_case(self):
        for row in self.df.columns:
            if self.df[row].dtype == 'object':
                self.df[row] = self.df[row].str.upper()

    def drop_duplicates(self):
        if len(self.df[self.df.duplicated()]) != 0:
            self.df.drop_duplicates()

# class California(CleanAndStore):

#     def __init__(self, load, load_type, save_path):
#         super().__init__(load, load_type, save_path)
#         super().upper_case()
#         super().drop_duplicates()
#         self.wrangle()
#         super().save_file('california', 'json')

#     def wrangle(self):

#         # Drop records
#         self.df.drop(self.df[self.df.date.isnull()].index, inplace=True)
#         self.df.drop(self.df[self.df.area == "UNKNOWN"].index, inplace=True)
#         self.df.drop(self.df[self.df.area == "OUT OF STATE"].index, inplace=True)

#         # Convert type
#         self.df = self.df.astype({'date': 'datetime64[ms]'})

class Florida(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('florida', 'json')

    def wrangle(self):

        # Drop records
        self.df.drop(['Case1', 'ChartDate', 'ObjectId'], axis=1, inplace=True)
        self.df.drop(self.df[self.df.Gender == 'Unknown'].index, inplace=True)

        # Convert type
        self.df = self.df.astype({'EventDate': 'object'})
        pd.to_datetime(self.df['EventDate'], unit='ms')

class Texas(CleanAndStore):

    def __init__(self, load, save_path):
        self.save_path = save_path
        self.columns = []
        self.set_column_names(load)
        super().load_excel(load, self.columns, 2)
        self.wrangle()
        super().upper_case()
        super().drop_duplicates()
        super().save_file('texas', 'json')

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
        

class NewYork(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('new-york', 'json')

    def wrangle(self):

        # Convert type
        self.df = self.df.astype({'test_date': 'datetime64[ms]',
                                  'new_positives': 'float',
                                  'cumulative_number_of_positives': 'float',
                                  'total_number_of_tests': 'float',
                                  'cumulative_number_of_tests': 'float'})

class Pennsylvania(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('pennsylvania', 'json')

    def wrangle(self):

        # Drop records
        self.df.drop(['georeferenced_lat__long', ':@computed_region_nmsq_hqvv', ':@computed_region_d3gw_znnf',
            ':@computed_region_amqz_jbr4', ':@computed_region_r6rf_p9et', ':@computed_region_rayf_jjgk'], axis=1, inplace=True)

        # Convert type
        self.df = self.df.astype({'date': 'datetime64[ms]',
                                  'cases': 'float',
                                  'cases_avg_new': 'float',
                                  'cases_cume': 'float',
                                  'population': 'float'})

class Illinois(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('illinois', 'json')

    def wrangle(self):

        # Convert type
        self.df = self.df.astype({'reportDate': 'datetime64[ms]',
                                  'tested': 'float',
                                  'confirmed_cases': 'float',
                                  'deaths': 'float'})

class Ohio(CleanAndStore):
    
    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('ohio', 'json')

    def wrangle(self):

        # Drop records
        self.df.drop(self.df[self.df.County == 'UNKNOWN'].index, inplace=True)
        self.df.drop(self.df[self.df.Sex.isnull()].index, inplace=True)
        self.df.drop(self.df[self.df.Sex == 'UNKNOWN'].index, inplace=True)
        self.df.drop(['Admission Date', 'Date Of Death'], axis=1, inplace=True)
        self.df.drop(self.df[self.df.Sex.isnull()].index, inplace=True)
        self.df.drop(self.df[self.df['Onset Date'].isnull()].index, inplace=True)

        # Change type
        self.df = self.df.astype({'Onset Date': 'datetime64[ms]',
                                  'Case Count': 'float',
                                  'Hospitalized Count': 'float',
                                  'Death Due To Illness Count - County Of Residence': 'float'})

class Georgia(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        self.wrangle()
        super().save_file('georgia', 'json')

    def wrangle(self):

        # Drop records
        self.df.drop(['OBJECTID', 'C_NEW_PERCT_CHG', 'D_NEW_PERCT_CHG', 'C_NEW_7D_MEAN', 'D_NEW_7D_MEAN', 'C_NEW_7D_PERCT_CHG', 'D_NEW_7D_PERCT_CHG', 'GlobalID'], axis=1, inplace=True)
        self.df.drop(self.df[self.df.COUNTY == 'NON-GEORGIA RESIDENT'].index, inplace=True)
        self.df.drop(self.df[self.df.COUNTY == 'UNKNOWN'].index, inplace=True)

        # Change type
        self.df = self.df.astype({'DATESTAMP': 'datetime64[ms]', 'C_New': 'float', 'C_Cum': 'float', 'D_New': 'float', 'D_Cum': 'float', 'H_New': 'float', 'H_Cum': 'float', 'C_Rate': 'float', 'C_Female': 'float', 'C_Male': 'float', 'C_SexUnkn':'float', 'C_UCon_Yes': 'float', 'C_UCon_No': 'float', 'C_UCon_Unk': 'float', 'C_Age_0_4': 'float', 'C_Age_5_14': 'float', 'C_Age_15_24': 'float', 'C_Age_25_34': 'float', 'C_Age_35_44': 'float', 'C_Age_45_54': 'float', 'C_Age_55_64': 'float', 'C_Age_65_74': 'float',
        'C_Age_75_84': 'float', 'C_Age_85plus': 'float', 'C_Age_Unkn': 'float', 'C_Age_0': 'float',
        'C_Age_20': 'float', 'C_RaceWh': 'float', 'C_RaceBl': 'float', 'C_RaceAs': 'float', 'C_RaceOth': 'float', 'C_RaceUnk': 'float', 'C_His': 'float', 'C_NonHis': 'float', 'C_EthUnk': 'float'})

class Cases(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.columns = []
        self.set_column_names()
        self.wrangle()
        super().upper_case()
        super().drop_duplicates()
        super().save_file('cases', 'json')

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
            final_dict['County Name'] = row[1]
            final_dict['State'] = row[2]
            final_dict['StateFIPS'] = row[3]
            final_dict['Date'] = row[4]
            final_dict['Cases'] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

class Deaths(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        self.columns = []
        self.set_column_names()
        self.wrangle()
        super().upper_case()
        super().drop_duplicates()
        super().save_file('deaths', 'json')

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
            final_dict['County Name'] = row[1]
            final_dict['State'] = row[2]
            final_dict['StateFIPS'] = row[3]
            final_dict['Date'] = row[4]
            final_dict['Deaths'] = row[5]
            final_list.append(final_dict)

        self.df = pd.DataFrame(final_list)

class Population(CleanAndStore):

    def __init__(self, load, load_type, save_path):
        super().__init__(load, load_type, save_path)
        super().upper_case()
        super().drop_duplicates()
        super().save_file('population', 'json')