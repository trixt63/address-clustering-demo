from pymongo import MongoClient

from config import MongoDBEntityConfig
from app.utils.logger_utils import get_logger

logger = get_logger('MongoDB Entity')


class MongoDBEntity:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBEntityConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBEntityConfig.DATABASE]
        self._config_col = self._db['configs']
        self._multichain_wallets_col = self._db['multichain_wallets']
        self._smart_contracts_col = self._db['smart_contracts']

    # def get_native_token_price_change_logs(self, chain_id) -> Dict:
    #     _filter = {'_id': f"{chain_id}_{NATIVE_TOKENS[chain_id]}"}
    #     _projection = ['priceChangeLogs']
    #     return self._smart_contracts_col.find_one(filter=_filter, projection=_projection)

    def get_top_tokens(self, chain_id):
        return self._config_col.find_one({'_id': f'top_tokens_v2_{chain_id}'})

    def get_stablecoins(self, chain_id):
        return self._smart_contracts_col.find({'chainId': chain_id, 'categories': 'Stablecoins'})

    def get_price_change_logs(self, chain_id, token_addresses):
        _token_ids = [f"{chain_id}_{address}" for address in token_addresses]
        _filter = {'_id': {'$in': _token_ids}, 'priceChangeLogs': {'$exists': 1}}
        _projection = ['priceChangeLogs']
        return self._smart_contracts_col.find(filter=_filter, projection=_projection)


if __name__ == '__main__':
    klg = MongoDBEntity()
