import pickle
import pandas as pd


MODELS_DIR = './data/models'


class PredictModel:
    def __init__(self):
        self.model_by_chain = dict()
        with open(f'{MODELS_DIR}/eth_model.pickle', 'rb') as f:
            self.model_by_chain['0x1'] = pickle.load(f)
        with open(f'{MODELS_DIR}/bsc_model.pickle', 'rb') as f:
            self.model_by_chain['0x38'] = pickle.load(f)

    def predict(self, chain_id, data: pd.DataFrame) -> list[bool]:
        results: list[bool] = list()
        for row in data:
            results.append(self.model_by_chain[chain_id].predict(row))
        return results
