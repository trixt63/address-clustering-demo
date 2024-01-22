import pymongo.errors
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection

from config import MongoDBLabelConfig
from app.utils.logger_utils import get_logger

logger = get_logger('MongoDB Label')
NO_OPERATION_TO_EXECUTE = 'No operations to execute'


class MongoDBLabel:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBLabelConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBLabelConfig.DATABASE]

        self._bots_col = self._db['bots']

    # def _create_index(self):
    #     if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
    #         self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    #######################
    #        Update       #
    #######################
    @staticmethod
    def _update_collection(collection: Collection, data: list[dict], upsert: bool = False):
        bulk_updates = [
            UpdateOne(
                {'_id': datum['_id']},
                {'$set': datum},
                upsert=upsert)
            for datum in data
        ]
        try:
            collection.bulk_write(bulk_updates)
        except pymongo.errors.InvalidOperation as ex:
            if ex.args[0] != 'No operations to execute':
                logger.exception(ex)
                raise ex
        except Exception as ex:
            logger.exception(ex)
            raise ex

    def insert_bots(self, data: list[dict]):
        for datum in data:
            datum['_id'] = f"{datum['chainId']}_{datum['address']}"
        self._bots_col.insert_many(data)



if __name__ == '__main__':
    pass
