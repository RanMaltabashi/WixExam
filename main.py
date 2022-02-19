# RAN MALTABASHI - Wix Exam

# IMPORTS
import requests
import json
import pandas as pd
import sqlalchemy as sa
import urllib.parse
from datetime import datetime

# CONSTS
API_LINK = 'https://randomuser.me/api/?results=4500'


# This function is used to get the data from the randomuser API, flatten into pandas dataframe and replace columns name
def get_data():
    try:
        response = requests.get(API_LINK)
        data = response.json()
        df = pd.json_normalize(data['results'])
        df.columns = df.columns.str.replace('.', '_', regex=False)
        print('get_data function finished:', datetime.now())
        return df
    except Exception as e:
        raise Exception(f'get_data failed: {e}')


# This function is used to split the main dataframe into 2 dataframes - male and female by gender column
def split_male_female(df_main, engine):
    try:
        df_male, df_female = df_main[df_main['gender'] == 'male'], df_main[df_main['gender'] == 'female']
        df_male.to_sql('RAN_MALTABASHI_test_male', con=engine, if_exists='replace', index=False)
        df_female.to_sql('RAN_MALTABASHI_test_female', con=engine, if_exists='replace', index=False)
        print('split_male_female function finished:', datetime.now())
    except Exception as e:
        raise Exception(f'split_male_female failed: {e}')


# This function is used to create the connection to the mysql DB and return it
def connection_maker():
    try:
        with open('connection_details.json', 'r') as f:
            connection_details = json.load(f)
        user = connection_details['user']
        password = urllib.parse.quote_plus(connection_details['password'])
        host = connection_details['host']
        port = connection_details['port']
        database = connection_details['database']
        engine = sa.create_engine(url=f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        print('connection_maker function finished:', datetime.now())
        return engine
    except Exception as e:
        raise Exception(f'connection_maker failed: {e}')


# This function is used to split the main dataframe into 10 subsets by age
def split_ten_groups(df_main, engine):
    try:
        for i in range(1, 11):
            df = df_main[(df_main['dob_age'] >= int((i*10))) & (df_main['dob_age'] <= int((i*10)+9))]
            df.to_sql(f'RAN_MALTABASHI_test_{str(i)}', con=engine, if_exists='replace', index=False)
        print('split_ten_groups function finished:', datetime.now())
    except Exception as e:
        raise Exception(f'split_ten_groups failed: {e}')


# This function is used to get the last 20 male and female by registered date
def top_20_by_registered_date(engine):
    try:
        sql_query = 'SELECT * ' \
                    'FROM (' \
                            'SELECT * FROM RAN_MALTABASHI_test_male ' \
                            'UNION ALL ' \
                            'SELECT * FROM RAN_MALTABASHI_test_female) T1 ' \
                    'ORDER BY registered_date DESC ' \
                    'LIMIT 20 '
        df = pd.read_sql(sql_query, engine)
        df.to_sql('RAN_MALTABASHI_test_20', con=engine, if_exists='replace', index=False)
        print('top_20_by_registered_date function finished:', datetime.now())
    except Exception as e:
        raise Exception(f'top_20_by_registered_date failed: {e}')


# This function is used to get and combine data from the RAN_MALTABASHI_test_20 and RAN_MALTABASHI_test_5 tables
# No duplicates
def union_20_and_5_no_duplicates(engine):
    try:
        sql_query = 'SELECT * FROM RAN_MALTABASHI_test_5 ' \
                    'UNION ' \
                    'SELECT * FROM RAN_MALTABASHI_test_20'
        df = pd.read_sql(sql_query, engine)
        json = df.to_json()
        with open('first.json', 'w') as f:
            f.write(json)
        print('union_20_and_5_no_duplicates function finished:', datetime.now())
    except Exception as e:
        raise Exception(f'union_20_and_5_no_duplicates failed: {e}')


# This function is used to get and combine data from RAN_MALTABASHI_test_20 and RAN_MALTABASHI_test_2 tables
# With duplicates (if has)
def union_20_and_2_with_duplicates(engine):
    try:
        sql_query = 'SELECT * FROM RAN_MALTABASHI_test_5 ' \
                    'UNION ALL ' \
                    'SELECT * FROM RAN_MALTABASHI_test_20'
        df = pd.read_sql(sql_query, engine)
        json = df.to_json()
        with open('second.json', 'w') as f:
            f.write(json)
        print('union_20_and_2_with_duplicates function finished:', datetime.now())
    except Exception as e:
        raise Exception(f'union_20_and_2_with_duplicates failed: {e}')


# main scope
if __name__ == '__main__':
    try:
        print('program started:', datetime.now())
        df_main = get_data()
        engine = connection_maker()
        split_male_female(df_main, engine)
        split_ten_groups(df_main, engine)
        top_20_by_registered_date(engine)
        union_20_and_5_no_duplicates(engine)
        union_20_and_2_with_duplicates(engine)
    except Exception as e:
        print(f'main stopped: {e}')
