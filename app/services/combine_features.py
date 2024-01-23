import re
import random
import numpy as np

#from filter_contract import *
import pandas as pd
from operator import add
from app.utils.logger_utils import get_logger
from multithread_processing.base_job import BaseJob
from sklearn.metrics.pairwise import cosine_similarity
from IPython.utils import io

logger = get_logger('Combine from to')


def get_embedding_list(row):  # An ebedding vector in NodeEmbedding is not a list, this function help to turn that embedding vector to list
    numbers = re.findall(r'-?\d+\.\d+', row)
    number_list = [float(number) for number in numbers]
    return number_list


def get_time_histogram(row):  # get time histogram of row 0 when merge to_df and from_df
    if row == 0:
        row = [0]*24
    return str(row)


def get_time(row):  # Concat sending time and receiving time of a wallet
    return list(map(add, row['From_time'], row['To_time']))


def diff_cosine(row):  # Calculate the similarity between 2 embedding of 2 wallets
    vec1 = np.array(row['X_Diff2VecEmbedding']).reshape(1,-1)
    vec2 = np.array(row['SubX_Diff2VecEmbedding']).reshape(1,-1)
    cosine_sim = cosine_similarity(vec1, vec2)
    return cosine_sim[0][0]


def get_label(x):  # return True if label == True, return False if label are not True
    if x==True:
        return True
    else:
        return False


def combine_from_to(df_from,df_to,df_embedding) -> pd.DataFrame:
    """Combine from, to and embedding feature to profile a wallet
    """
    logger.info("Combining sending and receiving information ...")
    for cols in df_from.columns:
        if cols not in ['_id','address']:
            df_from.rename(columns = {f'{cols}':f'From_{cols}'}, inplace=True)
            
    for cols in df_to.columns:
        if cols not in ['_id','address']:
            df_to.rename(columns = {f'{cols}':f'To_{cols}'}, inplace=True)
    
    df = df_from.merge(df_to, how='outer',on=['_id','address'])
    logger.info("Combine sending and receiving information successful")

    df = df.fillna(0)
    df = df.drop_duplicates()
    df['To_time'] = df['To_time'].apply(get_time_histogram)
    df['From_time'] = df['From_time'].apply(get_time_histogram)
    df['To_time'] = df['To_time'].apply(lambda row: eval(row))
    df['From_time'] = df['From_time'].apply(lambda row: eval(row))
    df_embedding.rename(columns={"vertices": "address"}, inplace= True)
    logger.info("Merging embedding information ...")

    df = df.merge(df_embedding, on=['_id','address'], how = 'left').dropna()
    logger.info("Merge embedding information successful")

    df['Time'] = df.apply(lambda x: get_time(x), axis=1)
    return df


def generate_training_dataset(df, contract=None) -> pd.DataFrame:
    """this function turn the input wallets' profile to training dataset"""
    if contract != None:
        contract.drop("Unnamed: 0", axis=1, inplace=True)
        final_df = df.merge(contract, on='address', how='inner')
        final_df= final_df[final_df['IsContract']==False]
        final_df.drop("IsContract", axis=1, inplace=True)
    else:
        final_df = df.copy()

    X_df = final_df[final_df['_id'].apply(lambda x: x.split('_')[1])== final_df['address']]
    
    for cols in X_df.columns:
        if cols == '_id':
            continue
        X_df.rename(columns={f'{cols}': f'X_{cols}'}, inplace=True)
    X_lst = X_df['X_address'].unique().tolist()
    SubX_df =final_df[final_df['_id'].apply(lambda x: x.split('_')[1])!= final_df['address']]
    for cols in SubX_df.columns:
        if cols == '_id':
            continue
        SubX_df.rename(columns={f'{cols}': f'SubX_{cols}'}, inplace=True)

    pair_features  = SubX_df.merge(X_df, on='_id', how='outer')
    pair_features.dropna(inplace=True)
    return pair_features
