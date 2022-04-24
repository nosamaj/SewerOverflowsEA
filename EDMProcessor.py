#Example code to process known excel data into a more usable format using pandas 
#take the conset data from the EA, and EDM return data from the WaSCs and then merge into a single table
#save out has hd5 file for performance to be read in by other modules 

import pandas as pd
import sqlite3 as sqlite
import os
import gc

os.environ["MODIN_ENGINE"] = "dask"

def files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file
def create_data_file(df,filename):
    if filename in files('.'):
        print('base data of this type already exists')
    else:    
        df.to_hdf(filename+'.hd5', key ='df')



def append_2020(file_list, path_prefix):
    df_return=pd.DataFrame()
    list = []
    for file in file_list:
        if 'EAv' in file:
            df_temp = pd.read_excel(path_prefix+file)
            list.append(df_temp)
    df_stats = pd.concat(list)
    return df_stats
 
def read_excel_sheets(xls_path):
    """Read all sheets of an Excel workbook and return a single DataFrame"""
    print(f'Loading {xls_path} into pandas')
    xl = pd.ExcelFile(xls_path)
    df = pd.DataFrame()
    columns = None
    for idx, name in enumerate(xl.sheet_names):
        print(f'Reading sheet #{idx}: {name}')
        sheet = xl.parse(name)
        if idx == 0:
            # Save column names from the first sheet to match for append
            columns = sheet.columns
        sheet.columns = columns
        # Assume index of existing data frame when appended
        df = df.append(sheet, ignore_index=True)
    return df   


data_path = './input data/'




consents_df = pd.read_excel(data_path+'consents_active.xlsx')

 
  
#pd.read_xls('/input data')

overflows_df = consents_df[consents_df ['OUTLET_TYPE_DESCRIPTION'] == 'Sewage - water company']

#create_data_file(overflows_df,'overflows')

try:
    df_stats = append_2020(files(data_path),data_path)
except ValueError:
    print ("check for 2020 files in input data folder")

columns_to_add = ["Site Name", "Water Company Name" , 
                  "Total Duration (hours) of all spills prior to processing through 12-24 hour counting method",
                  "Counted spills using 12-24hr counting method",
                  "% of reporting period EDM operational", "Comments"]

columns_rename_2020 = ["Site Name", "WaSC", "Duration Hr 2020",
                       "Spills 2020", "Percent Monitor Working 2020",
                       "Comments 2020"]


dictionary = dict(zip(columns_to_add, columns_rename_2020))

df_stats_append = df_stats[columns_to_add]

df_stats_append.rename(dictionary, axis='columns', inplace=True)


#reduce overflows_df to minimum fields 
#df_stats_append.head()

overflows_df = pd.merge(overflows_df, df_stats_append, left_on = 'DISCHARGE_SITE_NAME', right_on = 'Site Name', how='left')


df_2021_data = read_excel_sheets(data_path+'EDM 2021 Storm Overflow Annual Return - all water and sewerage companies.xlsx')
df_2021_data.columns = [x.replace("\n", " ") for x in df_2021_data.columns.to_list()]

columns_2021 =["Site Name (EA Consents Database)", "Initial EDM Commission Date", "Total Duration (hrs) all spills prior to processing through 12-24h count method", "Counted spills using 12-24h count method", "EDM Operation - % of reporting period EDM operational", "EDM Operation - Reporting % - Primary Reason <90%", "EDM Operation - Action taken / planned - Status & timeframe", "High Spill Frequency - Operational Review - Primary Reason", "High Spill Frequency - Action taken / planned - Status & timeframe", "High Spill Frequency - Environmental Enhancement - Planning Position (Hydraulic capacity)"]
columns_2021_new = ["Site Name", "Commision Date", "Duration Hrs 2021", "Spills 2021", 
                    "Percent Montior Working 2021", "Monitor Failure Reason 2021", 
                    "Monitor Failure Action 2021", "High Spill Cause 2021",
                    "High Spill Action 2021", "High spill Planning 2021"]


dictionary = dict(zip(columns_2021, columns_2021_new))

df_stats_append = df_2021_data[columns_2021]

df_stats_append.rename(dictionary, axis='columns', inplace=True)

df_stats_append.head()

overflows_df = pd.merge(overflows_df, df_stats_append, left_on = 'DISCHARGE_SITE_NAME', right_on = 'Site Name', how='left')

overflows_df.head()

#add columns for 2020 return

#import 2021 return

#add columns for 2021 return
