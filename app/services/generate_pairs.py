from collections import defaultdict
import pandas as pd

from app.databases.mongodb import MongoDB
from app.databases.mongodb_entity import MongoDBEntity
from app.databases.arangodb_klg import ArangoDB
from app.utils.logger_utils import get_logger
from app.models.graph.edge import Edge
from app.models.graph.address_training import AddressTraining
from app.constants.network_constants import NATIVE_TOKEN


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

        # self.allFrom_lst: list = []
        # self.allTo_lst: list = []
        # self.subgraph: list = []
        # self.EdgeQuery: set = set()

        self.prominent_tokens: set[str] = set()

        self._logger = get_logger('Pairs Generator')

    def get_prominent_tokens(self) -> set[str]:
        top_tokens = self.mongo_entity.get_top_tokens(chain_id=self.chain_id)
        top_tokens_addresses = [datum['address'] for datum in top_tokens['tokens']]

        stablecoins = self.mongo_entity.get_stablecoins(chain_id=self.chain_id)
        stablecoin_addresses = [datum['address'] for datum in stablecoins]

        top_tokens_addresses.extend(stablecoin_addresses)
        return set(top_tokens_addresses)

    def construct_subgraph(self, address):
        prominent_tokens: set[str] = self.get_prominent_tokens()
        prominent_tokens.add(NATIVE_TOKEN)
        addresses_dict: dict[str, AddressTraining] = dict()  # {address: AddressTraining}
        subgraph_edges: list[Edge] = self.arango.get_subgraph_edges(address=address, depth=2)
        for e in subgraph_edges:
            addresses_dict[e.from_address] = AddressTraining(chain_id=self.chain_id,
                                                             address=e.from_address)
            if e.to_address not in addresses_dict.keys():
                addresses_dict[e.to_address] = AddressTraining(chain_id=self.chain_id,
                                                               address=e.to_address)

        for e in subgraph_edges:
            addresses_dict[e.from_address].set_from_vertex_data(edge=e, prominent_tokens=prominent_tokens)
            addresses_dict[e.to_address].set_to_vertex_data(edge=e, prominent_tokens=prominent_tokens)


if __name__ == '__main__':
    pair_generator = PairsGenerator(chain_id='0x1')
