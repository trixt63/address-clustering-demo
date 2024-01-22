import time
from typing import List, Dict

import pymongo.errors
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import InvalidOperation

from config import MongoDBConfig
from app.utils.logger_utils import get_logger
from app.utils.time_utils import human_readable_time
from app.utils.file_utils import write_to_file

logger = get_logger('MongoDB')
ERROR_LOG_FILE = '.data/mongodb_errors.txt'
NO_OPERATION_TO_EXECUTE = 'No operations to execute'


class MongoDB:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBConfig.DATABASE]

        self._deposit_wallets_col = self._db['depositWallets']
        self._subgraphs_col = self._db['subgraphs']
        self._groups_col = self._db['groups']

        self._deposit_users_col = self._db['depositUsers']
        self._user_deposits_col = self._db['userDeposits']

        self._names_col = self._db['names']
        # self._create_index()

    # def _create_index(self):
    #     if 'wallets_number_of_txs_index_1' not in self.wallets_col.index_information():
    #         self.wallets_col.create_index([('number_of_txs', 1)], name='wallets_number_of_txs_index_1')

    #######################
    #   Aggregate groups  #
    #######################

    def get_min(self, col_name, field_name, filter_={}):
        cursor = self._db[col_name].find(filter_).sort(field_name, 1).limit(1)
        return cursor[0][field_name]

    def get_max(self, col_name, field_name, filter_={}):
        cursor = self._db[col_name].find(filter_).sort(field_name, -1).limit(1)
        return cursor[0][field_name]

    def get_number_of_docs(self, collection_name):
        col = self._db[collection_name]
        return col.estimated_document_count()

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

    def update_deposit_wallets_single_chain(self, wallets: List[dict]):
        """For new schema of deposit wallets (with chainId only"""
        try:
            wallet_updates_bulk = []
            for wallet in wallets:
                wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"

                # pop all basic information besides data about CEXs
                wallet_base_data = {
                    '_id': wallet.pop('_id'),
                    'chainId': wallet.pop('chainId'),
                    'address': wallet.pop('address'),
                    'lastUpdatedAt': wallet.pop('lastUpdatedAt')
                }
                tags = wallet.pop('tags')
                # update nested documents
                _mongo_add_to_set_query = {"depositedExchanges": {"$each": wallet['depositedExchanges']},
                                           "tags": {'$each': tags}}
                # add update query into bulk
                _filter = {'_id': wallet_base_data['_id']}
                _update = {
                    '$set': wallet_base_data,
                    '$addToSet': _mongo_add_to_set_query
                }
                wallet_updates_bulk.append(UpdateOne(filter=_filter, update=_update, upsert=True))

            self._deposit_wallets_col.bulk_write(wallet_updates_bulk)
        except Exception as ex:
            logger.exception(ex)

    def update_subgraphs(self, data: List[Dict], chain_name: str,radius: int = None):
        try:
            bulk_updates = list()
            for subgraph in data:
                subgraph['_id'] = f"{subgraph['chainId']}_{subgraph['address']}"
                subgraph_new_edges: List = subgraph.pop('edges', None)
                bulk_updates.append(UpdateOne(
                    filter={'_id': subgraph['_id']},
                    update={
                        '$set': subgraph,
                        '$addToSet': {"edges": {'$each': subgraph_new_edges}}
                    },
                    upsert=True
                ))
            if not radius:
                self._subgraphs_col.bulk_write(bulk_updates)
            else:
                self._db[f"subgraph_{chain_name}_{radius}"].bulk_write(bulk_updates)
        except Exception as ex:
            logger.exception(ex)

    def update_transactions(self, chain_id, data: List[Dict]):
        bulk_updates = [
            UpdateOne({'_id': datum['_id']}, {'$set': datum}, upsert=True)
            for datum in data
        ]
        try:
            self._db[f"{chain_id}_transactions"].bulk_write(bulk_updates)
        except Exception as ex:
            logger.exception(ex)

    # deposits & users job
    def get_number_of_deposit_wallets(self):
        return self._deposit_wallets_col.estimated_document_count()

    def get_deposit_wallets(self, skip, limit):
        return self._deposit_wallets_col.find(filter={}).skip(skip=skip).limit(limit=limit)

    def update_deposit_users(self, data: List[Dict]):
        bulk_updates = list()
        for datum in data:
            _id = f"{datum['chainId']}_{datum['address']}"
            datum['_id'] = _id
            user_wallets = datum.pop('userWallets', '')
            if user_wallets:
                bulk_updates.append(UpdateOne(
                    filter={'_id': _id},
                    update={
                        '$set': datum,
                        '$addToSet': {'userWallets': {'$each': user_wallets}}
                    },
                    upsert=True
                ))
        try:
            self._deposit_users_col.bulk_write(bulk_updates)
        except InvalidOperation as ex:
            _message = ex.args[0]
            if _message == NO_OPERATION_TO_EXECUTE:
                logger.debug(_message)
            else:
                logger.exception(f"Error: {_message}")
                write_to_file(ERROR_LOG_FILE, ex)
        except Exception as ex:
            _full_error_log = f"{human_readable_time(int(time.time()))}: {ex} \n"
            write_to_file(ERROR_LOG_FILE, _full_error_log)
            _message = ex.args[0]
            logger.exception(f"Error (Not No operations): {_message}")

    def update_user_deposits(self, data: List[Dict]):
        bulk_updates = list()
        for datum in data:
            _id = f"{datum['chainId']}_{datum['address']}"
            datum['_id'] = _id
            deposit_wallets = datum.pop('depositWallets', '')
            if deposit_wallets:
                bulk_updates.append(UpdateOne(
                    filter={'_id': _id},
                    update={
                        '$set': datum,
                        '$addToSet': {'depositWallets': {'$each': deposit_wallets}}
                    },
                    upsert=True
                ))
        try:
            self._user_deposits_col.bulk_write(bulk_updates)
        except InvalidOperation as ex:
            _message = ex.args[0]
            if _message == NO_OPERATION_TO_EXECUTE:
                logger.debug(_message)
            else:
                logger.exception(f"Error: {_message}")
                write_to_file(ERROR_LOG_FILE, ex)
        except Exception as ex:
            _full_error_log = f"{human_readable_time(int(time.time()))}: {ex} \n"
            write_to_file(ERROR_LOG_FILE, _full_error_log)
            _message = ex.args[0]
            logger.exception(f"Error (Not No operations): {_message}")

    # for APIs
    def get_deposit_wallet_with_users(self, chain_id, address):
        _filter = {'_id': f"{chain_id}_{address}"}
        return self._deposit_users_col.find_one(filter=_filter)

    def get_user_with_deposit_wallets(self, chain_id, address):
        _filter = {'_id': f"{chain_id}_{address}"}
        return self._user_deposits_col.find_one(filter=_filter)

    #######################
    #      Analysis       #
    #######################
    def count_wallets(self, _filter):
        _count = self._deposit_wallets_col.count_documents(_filter)
        return _count

    def count_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Count number of wallets of each project on each chain"""
        _filter = {f"{field_id}.{project_id}": {"$exists": 1}}
        _projection = {f"{field_id}.{project_id}": 1}
        deployments = self._deposit_wallets_col.find(_filter, _projection)
        _count = 0
        for _depl in deployments:
            for project in _depl[field_id][project_id]:
                if project['chainId'] == chain_id:
                    _count += 1
        return _count

    def count_exchange_deposit_wallets_each_chain(self, field_id, project_id, chain_id='0x38'):
        """Each CEX project stores a list of chain_ids, instead a list of objects like other type of project,
        so I need a separate function to handle this"""
        _filter = {f"{field_id}.{project_id}": chain_id}
        _count = self._deposit_wallets_col.count_documents(_filter)
        return _count

    def _get_duplicated_wallets(self, input_wallets: list, collection_name: str):
        col = self._db[collection_name]
        _filter = {
            '_id': {'$in': input_wallets}
        }
        _project = {
            'address': 1
        }
        duplicated_wallets = col.find(_filter, _project)
        return duplicated_wallets

    def _delete_wallets(self, collection_name: str, ids: list):
        _filter = {'_id': {'$in': ids}}
        col = self._db[collection_name]
        col.delete_many(_filter)

    #######################
    #     Name Service    #
    #######################
    def get_events_by_blocks_range(self, collection_name: str,
                                   from_block: int,
                                   to_block: int,
                                   sort: int = 1):
        """
        The get_events_by_blocks_range function returns a list of events from the specified collection and block range.
        The sort argument can be used to specify whether the results should be sorted by block number (ascending or descending).

        Args:
            collection_name: Specify the collection in which to search for events
            from_block: Specify the starting block number for the query
            to_block: Specify the upper bound of the block range
            sort: Sort the results by block number
        """
        if sort not in {1, 0, -1}:
            raise ValueError("sort argument must be -1 (descending), 1 (ascending) or 0 (not sort)")
        col = self._db[collection_name]
        _filter = {
            'block_number': {'$gte': from_block, '$lt': to_block},
        }
        if sort:
            return col.find(_filter).sort('block_number', sort)
        else:
            return col.find(_filter)

    def get_addrchanged_event(self, chain_id: str, hashed_name: str) -> dict | None:
        _filter = {
            'node': hashed_name,
            'event_type': 'ADDRCHANGED'
        }
        addr_changed_event = list(self._db[f'name_events_{chain_id}'].find(_filter).sort('block_number', -1).limit(1))
        if addr_changed_event:
            return addr_changed_event[0]
        else:
            return None

    def update_names(self, data: list[dict]):
        """
        Update names collection on Mongo (only for old jobs on 0x38)
        """
        bulk_updates = [
            UpdateOne(
                {'_id': f"{datum['chainId']}_{datum['name']}"},
                {'$set': datum},
                upsert=True)
            for datum in data
        ]
        try:
            self._names_col.bulk_write(bulk_updates)
        except pymongo.errors.InvalidOperation as ex:
            if ex.args[0] != NO_OPERATION_TO_EXECUTE:
                logger.exception(ex)
                raise ex
        except Exception as ex:
            logger.exception(ex)
            raise ex

    def update_registered_names(self, data: list[dict]):
        self._update_collection(self._names_col, data=data, upsert=True)

    def update_addresschanged_names(self, data: list[dict]):
        self._update_collection(self._names_col, data=data, upsert=False)

    def get_names(self, timestamp_range: tuple = tuple()):
        """
        The get_names function returns a list of names from the database.
        Args:
            timestamp_range: tuple of 2: Set the default value of timestamp_range for lastUpdatedAt
            skip: number of names to skip
        Returns:
            A cursor object
        """
        if timestamp_range:
            _filter = {'lastUpdatedAt': {'$gte': timestamp_range[0], '$lt': timestamp_range[1]}}
        else:
            _filter = {}
        return self._names_col.find(_filter)


if __name__ == '__main__':
    pass
