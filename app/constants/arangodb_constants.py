from app.constants.network_constants import Chains


class ArangoDBPrefix:
    mapping = {
        Chains.bsc: 'bsc',
        Chains.ethereum: 'ethereum',
        Chains.fantom: 'ftm',
        Chains.polygon: 'polygon'
    }

    reversed_mapping = {v: k for k, v in mapping.items()}


# class ArangoDBCollectionsByChain:
#     def __init__(self, prefix):
#         self.configs = 'configs'
#         self.transfers = f'{prefix}_transfers'
#         self.addresses = f'{prefix}_addresses'
#
#
# class ArangoDBGraphsByChain:
#     def __init__(self, prefix):
#         self.transfers_graph = f'{prefix}_transfers_graph'


# class KnowledgeGraphModelByChain:
#     def __init__(self, prefix):
#         self.edge_definitions = [
#             {
#                 'edge_collection': ArangoDBCollectionsByChain(prefix).transfers,
#                 'from_vertex_collections': [
#                     ArangoDBCollectionsByChain(prefix).addresses,
#                 ],
#                 'to_vertex_collections': [
#                     ArangoDBCollectionsByChain(prefix).addresses,
#                 ],
#             }
#         ]


class ArangoDBKeys:
    pass


class ArangoDBIndex:
    pass


if __name__ == '__main__':
    print(ArangoDBPrefix.reversed_mapping)