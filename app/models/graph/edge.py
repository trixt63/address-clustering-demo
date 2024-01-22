from typing import Dict
from app.models.graph.transfer import Transfer


class Edge:
    def __init__(self, chain_id, from_address, to_address):
        """Relationship between 2 address"""
        self.chain_id = chain_id
        self.from_address = from_address
        self.to_address = to_address
        self.key = f'{chain_id}_{from_address}_{to_address}'

        self.transfer_logs: Dict[str, Dict] = dict()

    def update_transfer_logs(self, transfer: Transfer, value_in_usd: float):
        if transfer.coin_addr in self.transfer_logs:
            self.transfer_logs[transfer.coin_addr].update({
                transfer.timestamp: {
                    'amount': transfer.amount,
                    'valueInUSD': value_in_usd
                }
            })
        else:
            self.transfer_logs.update({
                transfer.coin_addr: {
                    transfer.timestamp: {
                        'amount': transfer.amount,
                        'valueInUSD': value_in_usd
                    }
                }
            })

    def get_transfer_logs(self) -> Dict[str, Dict]:
        return self.transfer_logs
