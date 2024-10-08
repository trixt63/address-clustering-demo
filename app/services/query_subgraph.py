import pandas as pd
import os
import sys
from itertools import chain
# sys.path.append(os.path.dirname(sys.path[0]))

# from app.databases.mongodb import MongoDB
from app.databases.arangodb_klg import ArangoDB
from app.models.graph.edge import Edge


def get_num_add(list_of_edges):
    all_values = list(chain.from_iterable(map(lambda d: (d['from'], d['to']), list_of_edges)))
    unique_values = set(all_values)
    total_unique_values = len(unique_values)
    return total_unique_values


def get_vertices(edges):
    unique_values = set()
    for dictionary in edges:
        unique_values.update(dictionary.values())
    unique_values_list = list(unique_values)
    return unique_values_list


def preprocess_subgraph(subgraph): #preprocess subgraph: get subgraph <= 100 vertices
    subgraph.rename(columns={"address":"X_address"}, inplace=True)
    subgraph['NumAddress'] = subgraph['edges'].apply(get_num_add)
    filterr = subgraph[subgraph["NumAddress"]<=200]
    filterr['vertices'] = filterr['edges'].apply(get_vertices)
    filterr.drop_duplicates('X_address',inplace=True)
    filterr.reset_index(inplace=True)
    filterr.drop('index', axis=1, inplace=True)
    return filterr


def query_subgraph(chain_id, address, graph_db: ArangoDB) -> pd.DataFrame:
    """query and preprocess the subgraph"""
    edges: list[Edge] = graph_db.get_subgraph_edges(address=address, depth=2)
    edge_dicts = [{'from': e.from_address, 'to': e.to_address} for e in edges]
    subgraph_df = pd.DataFrame([{'_id': f'{chain_id}_{address}',
                                 'address': address,
                                 'chainId': chain_id,
                                 'edges': edge_dicts,
                                }])
    prep_subgraph = preprocess_subgraph(subgraph_df)
    return prep_subgraph
