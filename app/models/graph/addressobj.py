from typing import Dict


class AddressObj:
    def __init__(self, chain_id, address):
        self._chain_id = chain_id
        self._address = address
        self.key = f"{self._chain_id}_{self._address}"
        self.last_transfer_at: int = 0

    def __eq__(self, other):
        return self.key == other.id

    def __hash__(self):
        return hash(self.key)

    def to_json_dict(self):
        return {
            'chainId': self._chain_id,
            'address': self._address,
            'lastTransferAt': self.last_transfer_at
        }
