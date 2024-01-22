from app.models.graph.addressobj import AddressObj
from app.models.graph.edge import Edge

class AddressTraining(AddressObj):
    def __init__(self,
                 chain_id,
                 address):
        super().__init__(chain_id=chain_id, address=address)
        self.chain_id = chain_id
        if chain_id == '0x38':
            self.chain_name = 'bsc'
        elif chain_id == '0x1':
            self.chain_name = 'ethereum'
        self.address = address
        self._id_arango = f'{self.chain_name}_addresses/{self.key}'

        self.from_amount: dict[str, list] = {'other_token': []}  # tokenAddresses: [amount]
        self.to_amount: dict[str, list] = {'other_token': []}  # tokenAddresses: [amount]
        self.time_histogram = [0] * 24
        self.node_embedded = []

    def set_from_vertex_data(self, edge: Edge, prominent_tokens: set[str]):
        """Set data for _from vertex in an edge"""
        if edge.from_address != self.address:
            raise ValueError('Address must be the from_address of Edge')
        for token_addr, token_transfer_log in edge.transfer_logs.items():
            for timestamp, datum in token_transfer_log.items():
                # time
                self.time_histogram[ int(timestamp % 24) ] += 1
                # amount:
                if token_addr not in prominent_tokens:
                    self.to_amount['other_token'].append(datum['valueInUSD'])
                if token_addr in self.from_amount:
                    self.from_amount[token_addr].append(datum['valueInUSD'])
                else:
                    self.from_amount[token_addr] = [datum['valueInUSD']]

    def set_to_vertex_data(self, edge: Edge, prominent_tokens: set[str]):
        """Set data for _to vertex in an edge"""
        if edge.to_address != self.address:
            raise ValueError('Address must be the to_address of Edge')
        for token_addr, token_transfer_log in edge.transfer_logs.items():
            for timestamp, datum in token_transfer_log.items():
                # time
                self.time_histogram[int(timestamp % 24)] += 1
                # amount:
                if token_addr not in prominent_tokens:
                    self.to_amount['other_token'].append(datum['valueInUSD'])
                elif token_addr in self.from_amount:
                    self.to_amount[token_addr].append(datum['valueInUSD'])
                else:
                    self.to_amount[token_addr] = [datum['valueInUSD']]

    def set_node_embedding(self, edges: list[Edge]):
        pass
