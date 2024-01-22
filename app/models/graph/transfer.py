class Transfer:
    def __init__(self, chain_id: str, from_addr: str, to_addr: str, coin_addr: str,
                 amount, timestamp):
        self.chain_id = chain_id
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.coin_addr = coin_addr
        self.amount = amount
        self.timestamp = timestamp
