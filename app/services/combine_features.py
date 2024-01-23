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


def get_embedding_list(row):  #An ebedding vector in NodeEmbedding is not a list, this function help to turn that embedding vector to list
    numbers = re.findall(r'-?\d+\.\d+', row)
    number_list = [float(number) for number in numbers]
    return number_list

def get_time_histogram(row): #get time histogram of row 0 when merge to_df and from_df
    if row == 0:
        row = [0]*24
    return str(row)

def get_time(row): #Concat sending time and receiving time of a wallet 
    return list(map(add, row['From_time'], row['To_time']))


def diff_cosine(row): #Calculate the similarity between 2 embedding of 2 wallets
    vec1 = np.array(row['X_Diff2VecEmbedding']).reshape(1,-1)
    vec2 = np.array(row['SubX_Diff2VecEmbedding']).reshape(1,-1)
    cosine_sim = cosine_similarity(vec1, vec2)
    return cosine_sim[0][0]


def get_label(x): # return True if label == True, return False if label are not True
    if x==True:
        return True
    else:
        return False


def combine_from_to(df_from,df_to,df_embedding) -> pd.DataFrame: #Combine from, to and embedding feature to profile a wallet
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
    df['To_time'] =df['To_time'].apply(get_time_histogram)
    df['From_time'] = df['From_time'].apply(get_time_histogram)
    df['To_time'] = df['To_time'].apply(lambda row: eval(row))
    df['From_time'] = df['From_time'].apply(lambda row: eval(row))
    df_embedding.rename(columns = {"vertices":"address"}, inplace= True)
    logger.info("Merging embedding information ...")

    df = df.merge(df_embedding, on=['_id','address'], how = 'left').dropna()
    logger.info("Merge embedding information successful")

    df['Time'] = df.apply(lambda x: get_time(x), axis=1)
    return df


def generate_training_dataset(df, contract=None) -> pd.DataFrame: #this function turn the input wallets' profile to training dataset
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
    # pair_filters = pairs[pairs['X_address'].isin(X_lst)]
    # pair_filters["Label"] = True
    # final_pair = pair_features.merge(pair_filters, on=['X_address','SubX_address'],how="left")
    # final_pair['Label'] = final_pair['Label'].apply(get_label)
    return pair_features


class processTrainingDataset(BaseJob): #preprocess training dataset
    def __init__(self, training_dataset: pd.DataFrame, saving_path: str, max_workers= 2, batch_size=1):
        self.training_dataset = training_dataset
        self.prep_df_lst = list() #List of preprocessed dataset 
        self.logger = logger
        self.saving_path = saving_path
        super().__init__(work_iterable=list(training_dataset.index),
                         max_workers=max_workers,
                         batch_size=batch_size)

    def _execute_batch(self, works):
        try:
            with io.capture_output() as captured:
                sub = self.training_dataset.copy()
                sub_df = sub.iloc[works]
                sub_df['X_Diff2VecEmbedding'] = sub_df['X_Diff2VecEmbedding'].apply(lambda x: get_embedding_list(x))
                sub_df['SubX_Diff2VecEmbedding'] = sub_df['SubX_Diff2VecEmbedding'].apply(lambda x: get_embedding_list(x))
                sub_df['Diff2_Vec_Simi'] = sub_df.apply(lambda x: diff_cosine(x), axis=1)
                sub_df[[f"X_Time{i}" for i in range(24)]] = sub_df.X_Time.apply(pd.Series)
                sub_df[[f"SubX_Time{i}" for i in range(24)]] = sub_df.SubX_Time.apply(pd.Series)
                self.prep_df_lst.append(sub_df)
        except Exception as e:
            self.logger.exception(f"Can't process df: {e}")

    def _end(self):
        super()._end()
        self.logger.info("Generate time columns and embedding similarity successful")
        prep_df = pd.concat(self.prep_df_lst)
        df_analyzing =prep_df.pivot_table(index='_id', columns='Label', aggfunc='size', fill_value=0).reset_index()
        lst_id = df_analyzing[df_analyzing[True]>=1]._id.unique().tolist()
        dfPrep =prep_df[prep_df['_id'].isin(lst_id)]
        usr_lst = prep_df['X_address'].unique().tolist()
        self.logger.info("Spliting and saving dataset ...")
        random_elements = random.sample(usr_lst, int(len(usr_lst)*0.9))
        train_data = dfPrep[dfPrep['X_address'].isin(random_elements)].reset_index(drop=True)
        test_data = dfPrep[~dfPrep['X_address'].isin(random_elements)].reset_index(drop=True)
        train_data.drop(["SubX_Time","SubX_From_time","SubX_To_time","X_From_time","X_To_time","X_Time", "X_Diff2VecEmbedding", "SubX_Diff2VecEmbedding", "X_address", "SubX_address"], axis=1,inplace =True)
        test_data.drop(["SubX_Time","SubX_From_time","SubX_To_time","X_From_time","X_To_time","X_Time", "X_Diff2VecEmbedding", "SubX_Diff2VecEmbedding", "X_address", "SubX_address"], axis=1,inplace =True)

        train_data.to_csv(f"{self.saving_path}/train_data.csv", index=False)
        test_data.to_csv(f"{self.saving_path}/test_data.csv", index=False)
        self.logger.info("Successful")