import sys
import os

sys.path.append(os.path.dirname(sys.path[0]))

from arango import ArangoClient
from arango.database import StandardDatabase
from arango.http import DefaultHTTPClient
from arango.result import Result
from typing import List, Dict
from arango.cursor import Cursor

from app.models.graph.edge import Edge

from config import ArangoDBConfig
from app.utils.logger_utils import get_logger
from app.utils.parser import get_connection_elements


logger = get_logger('ArangoDB')


class ArangoDB:
    def __init__(self, connection_url=None, database=ArangoDBConfig.DATABASE, prefix: str = None):
        if not connection_url:
            connection_url = ArangoDBConfig.CONNECTION_URL
        _username, _password, _connection_url = get_connection_elements(connection_url)

        http_client = DefaultHTTPClient()
        http_client.REQUEST_TIMEOUT = 1000

        try:
            self.client = ArangoClient(hosts=_connection_url, http_client=http_client)
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {_connection_url}: {e}")
            sys.exit(1)

        self._db = self._get_db(database, _username, _password)
        if prefix == 'bsc':
            self._chain_id = '0x38'
        elif prefix == 'ethereum':
            self._chain_id = '0x1'
        else:
            raise ValueError(f"Prefix must be ethereum or bsc")

        self._addresses_col_name = f'{prefix}_addresses'
        self._transfers_col_name = f'{prefix}_transfers'
        self._transfers_graph_name = f'{prefix}_transfers_graph'

        self._addresses_col = self._get_collections(self._addresses_col_name)
        self._transfers_col = self._get_collections(self._transfers_col_name, edge=True)
        _transfers_graph_edge_definitions = [{
                'edge_collection': self._transfers_col_name,
                'from_vertex_collections': [self._addresses_col_name],
                'to_vertex_collections': [self._addresses_col_name]
            }]
        self._transfers_graph = self._get_graph(graph_name=self._transfers_graph_name,
                                                edge_definitions=_transfers_graph_edge_definitions)

    def _get_db(self, db_name, username, password):
        return self.client.db(db_name, username=username, password=password)

    def _get_collections(self, collection_name, database: StandardDatabase = None, edge=False):
        if not database:
            database = self._db
        if not database.has_collection(collection_name):
            database.create_collection(collection_name, shard_count=20, edge=edge)
        return database.collection(collection_name)

    def _get_graph(self, graph_name, edge_definitions,
                   database: StandardDatabase = None):
        if not database:
            database = self._db
        if not database.has_graph(graph_name):
            database.create_graph(graph_name, edge_definitions=edge_definitions)
        return database.graph(graph_name)

    #####################
    #     Retrieve      #
    #####################

    def get_subgraph_edges(self, address, depth=2) -> list[Edge]:
        query = f"""
                FOR v, e, p in 1..{depth} ANY '{self._addresses_col_name}/{self._chain_id}_{address}'
                GRAPH {self._transfers_graph_name}
                PRUNE v.wallet.hotWallet
                || v.numberSent >100
                || v.numberReceived >100
                FILTER NOT v.wallet.hotWallet
                RETURN e 
            """
        cursor = self._db.aql.execute(query, batch_size=100, count=True)
        edges: list[Edge] = list()
        for _edge in cursor:
            _chain_id, _from_address = self._parse_id_to_key(_edge['_from']).split('_')
            _to_address = self._parse_id_to_key(_edge['_to']).split('_')[-1]
            new_edge = Edge(
                chain_id=_chain_id,
                from_address=_from_address,
                to_address=_to_address
            )
            new_edge.transfer_logs = _edge['tokenTransferLogs']
            edges.append(new_edge)
        return edges

    @staticmethod
    def _parse_id_to_key(id_string):
        """Remove prefix from _id"""
        return id_string.split('/')[-1]
    
    def query(self, query: str, batch_size=1000) -> Result:
        return self._db.aql.execute(query=query, batch_size=batch_size)

    def check_has_address(self, address):
        try:
            assert self._addresses_col.has(f'{self._chain_id}_{address}')
            return True
        except AssertionError:
            return False


if __name__ == '__main__':
    arango = ArangoDB(prefix='bsc')
    a = arango.check_has_address(chain_id='0x38',
                                 address='0x28590d49ab3d3677ffee8c5dd842c9863d4fd21a')
    print(a)
