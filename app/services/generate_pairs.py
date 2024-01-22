import json
from collections import defaultdict
import pandas as pd
from statistics import median

from app.databases.mongodb import MongoDB
from app.databases.mongodb_entity import MongoDBEntity
from app.databases.arangodb_klg import ArangoDB
from app.utils.logger_utils import get_logger
from app.models.graph.edge import Edge
from app.models.graph.address_training import AddressTraining
from app.constants.network_constants import NATIVE_TOKEN
from app.services.diff2vec.query_subgraph import query_subgraph
from app.services.diff2vec.diffusion_2_vec import *
from app.services.diff2vec.helper import *
from itertools import chain
from IPython.utils import io

SAVING_DIR = '../../data'


class PairsGenerator:
    def __init__(self, chain_id):
        self.chain_id = chain_id
        if chain_id == '0x38':
            self.chain_name = 'bsc'
        elif chain_id == '0x1':
            self.chain_name = 'ethereum'
        else:
            raise ValueError('Chain id must be 0x38 or 0x1')

        self.arango = ArangoDB(prefix=self.chain_name)
        self.mongo = MongoDB()
        self.mongo_entity = MongoDBEntity()

        self.prominent_tokens: set[str] = set()

        self._logger = get_logger('Pairs Generator')

    def get_prominent_tokens(self) -> set[str]:
        # top tokens
        top_tokens = {}
        with open(f'{SAVING_DIR}/token_data/top_token.json', 'r') as f:
            top_tokens_all_chains = json.load(f)
        for doc in top_tokens_all_chains:
            if doc['_id'] == f'top_tokens_{self.chain_id}':
                top_tokens = doc
        top_tokens_addresses = [datum['address'] for datum in top_tokens['tokens']]

        # stable coins
        with open(f'{SAVING_DIR}/token_data/stable_token.json', 'r') as f:
            stablecoins_all_chains = json.load(f)
        stablecoin_addresses = [doc['address'] for doc in stablecoins_all_chains if doc['chainId'] == self.chain_id]

        # merge 2 types of coins
        top_tokens_addresses.extend(stablecoin_addresses)
        return set(top_tokens_addresses)

    def get_time_amount_feature(self, address) -> (pd.DataFrame, pd.DataFrame):
        prominent_tokens: set[str] = self.get_prominent_tokens()
        prominent_tokens.add(NATIVE_TOKEN)
        addresses_dict: dict[str, AddressTraining] = dict()  # {address: AddressTraining}
        subgraph_edges: list[Edge] = self.arango.get_subgraph_edges(address=address, depth=2)
        for e in subgraph_edges:
            addresses_dict[e.from_address] = AddressTraining(chain_id=self.chain_id,
                                                             address=e.from_address,
                                                             prominent_tokens=prominent_tokens)
            if e.to_address not in addresses_dict.keys():
                addresses_dict[e.to_address] = AddressTraining(chain_id=self.chain_id,
                                                               address=e.to_address,
                                                               prominent_tokens=prominent_tokens)

        for e in subgraph_edges:
            addresses_dict[e.from_address].set_from_vertex_data(edge=e, prominent_tokens=prominent_tokens)
            addresses_dict[e.to_address].set_to_vertex_data(edge=e, prominent_tokens=prominent_tokens)

        from_dfs_list: list[pd.DataFrame] = list()
        to_dfs_list: list[pd.DataFrame] = list()
        for addr, address_obj in addresses_dict.items():
            # mark all tokens with no transfer: as 0
            for token_addr, values in address_obj.from_amount.items():
                if not values:
                    values.append(0)
            for token_addr, values in address_obj.to_amount.items():
                if not values:
                    values.append(0)

            _from_dict = {token_addr: [median(values)] for token_addr, values in address_obj.from_amount.items()}
            _from_dict['_id'] = [f'{self.chain_id}_{address}']
            _from_dict['address'] = [addr]
            _from_dict['time'] = [address_obj.time_histogram]
            _from_df = pd.DataFrame(_from_dict)
            from_dfs_list.append(_from_df)

            _to_dict = {token_addr: [median(values)] for token_addr, values in address_obj.to_amount.items()}
            _to_dict['_id'] = [f'{self.chain_id}_{address}']
            _to_dict['address'] = [addr]
            _to_dict['time'] = [address_obj.time_histogram]
            _to_df = pd.DataFrame(_to_dict)
            to_dfs_list.append(_to_df)

        from_df = pd.concat(from_dfs_list)
        to_df = pd.concat(to_dfs_list)

        return from_df, to_df

    def get_node_embedding_feature(self, address) -> pd.DataFrame:
        subgraph_edges: list[Edge] = self.arango.get_subgraph_edges(address=address, depth=2)
        subgraph_df = query_subgraph(chain_id=self.chain_id,
                                     address=address,
                                     edges=subgraph_edges)
        subgraph_df['Diff2VecEmbedding'] = subgraph_df.apply(lambda row: getDiff2VecEmbedding(row), axis=1)
        subgraph_df = subgraph_df.explode(['vertices', 'Diff2VecEmbedding'])
        subgraph_df = subgraph_df[['_id', 'vertices', 'Diff2VecEmbedding']]
        return subgraph_df


def getDiff2VecEmbedding(row):
    with io.capture_output() as captured:
        walks, counts = run_parallel_feature_creation(row['edges'],
                                                      16,
                                                      4,
                                                      4)
        model = learn_pooled_embeddings(walks, counts)
        embedding_row = list(map(lambda x: model.wv.get_vector(x), row['vertices']))
    return embedding_row


if __name__ == '__main__':
    pair_generator = PairsGenerator(chain_id='0x1')
    _address = '0x6d6ea51d6ef6cfc9671b362da3b6068a126eee25'
    # node_embedding_df = pair_generator.get_node_embedding_feature(_address)
    # node_embedding_df.to_csv(f'{SAVING_DIR}/node_embedding.csv')
    # from_df, to_df = pair_generator.get_time_amount_feature(_address)
    # from_df.to_csv(f'{SAVING_DIR}/from_df.csv')
    # to_df.to_csv(f'{SAVING_DIR}/to_df.csv')
