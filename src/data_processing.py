import numpy as np
import pandas as pd
from src.utils import (
    clean_address1,
    clean_address2,
    clearDate2,
    remove_duplicates,
    concat,
    TRANS_TABLE
)
import config
import re

def address(df : pd.DataFrame):
    df['address']=df['address'].parallel_apply(clean_address2).apply(lambda row: re.sub(r'[/()+={}<>\-\\\s]+', '', row))
    df['address'] = df['address'].parallel_apply(lambda s: "".join(sorted(s.split(','))))

def phone(df : pd.DataFrame):
    df['phone'] = df['phone'].parallel_apply(lambda phone: re.sub(r'\D', '', phone)[-7:])

def date(df : pd.DataFrame):
    df['birthdate'] = df['birthdate'].parallel_apply(clearDate2)

def email(df : pd.DataFrame):
    df['email'] = df['email'].parallel_apply(lambda s: s.split('@')[0])

def full_name(df : pd.DataFrame):
    df['full_name'] = df['full_name'].parallel_apply(remove_duplicates)
    df['full_name'] = df['full_name'].parallel_apply(lambda name: name.translate(TRANS_TABLE))

def sex(df : pd.DataFrame):
    df['sex'] = df['sex'].parallel_apply(lambda s: [1.]*100 if s=="m" else [0.]*100)
    df['sex'] = df['sex'].parallel_apply(np.array)

def concat(row):
    row['full_name'] = row['first_name'] +" "+ row['middle_name'] +" "+ row['last_name']
    row = row.drop(['first_name', 'middle_name', 'last_name'])
    return row

def process_file1():
    df = pd.read_csv(config.FILE1_PATH)
    df = df.drop('uid', axis='columns')
    address(df)
    phone(df)
    date(df)
    email(df)
    full_name(df)
    sex(df)
    df.to_csv(config.PROCESSED_FILE1_PATH)

def process_file2():
    df = pd.read_csv(config.FILE2_PATH)
    df = df.drop('uid', axis='columns')
    phone(df)
    df = df.parallel_apply(concat, axis=1)
    date(df)
    address(df)
    email(df)
    full_name(df)
    df.to_csv(config.PROCESSED_FILE2_PATH)

def process_file3():
    df = pd.read_csv(config.FILE2_PATH)
    df = df.drop('uid', axis='columns')
    date(df)
    email(df)
    df['name'] = df['name'].parallel_apply(remove_duplicates)
    df['name'] = df['name'].parallel_apply(lambda name: name.translate(TRANS_TABLE))
    sex(df)
    df.to_csv(config.PROCESSED_FILE3_PATH)




    

    



