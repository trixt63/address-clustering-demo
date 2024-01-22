from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict

from config import PostgresDBConfig
from app.utils.logger_utils import get_logger

logger = get_logger('PostgreSQL thread safe')

Session = sessionmaker()


class PostgresDB:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresDBConfig.CONNECTION_URL
        Session.configure(bind=create_engine(connection_url))

    # @sync_log_time_exe(tag=TimeExeTag.database)
    @staticmethod
    def get_event_transfers(from_block, to_block, chain_id: str=None) -> List[Dict]:
        if not chain_id:
            schema = PostgresDBConfig.SCHEMA
        else:
            schema = f"chain_{chain_id}"
        query = f"""
            SELECT * 
            FROM {schema}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
            WHERE block_number BETWEEN {from_block} AND {to_block}
        """
        with Session.begin() as session:
            _event_transfers = session.execute(query).all()
        return _event_transfers

    @staticmethod
    def get_from_addresses_by_to_addresses(to_addresses, from_block, to_block):
        with Session.begin() as session:
            query = f"""
                SELECT from_address
                FROM {PostgresDBConfig.SCHEMA}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
                WHERE to_address = ANY (ARRAY{to_addresses})
                AND block_number BETWEEN {from_block} AND {to_block}
                GROUP BY from_address
            """
            event_transfer = session.execute(query).all()
        return event_transfer

    @staticmethod
    def get_all_event_transfer_to_addresses(to_addresses, from_block, to_block):
        query = f"""
            SELECT from_address, to_address
            FROM {PostgresDBConfig.SCHEMA}.{PostgresDBConfig.TRANSFER_EVENT_TABLE}
            WHERE to_address = ANY (ARRAY{to_addresses})
            AND block_number BETWEEN {from_block} AND {to_block}
            GROUP BY from_address, to_address
        """
        with Session.begin() as session:
            event_transfer = session.execute(query).all()
        return event_transfer


if __name__ == '__main__':
    db = PostgresDB()
    data = db.get_event_transfers(from_block=30817962, to_block=30817992)
    d = dict()
    for datum in data:
        addr = datum['contract_address']
        hash = datum['transaction_hash']
        d[hash] = addr
    print(len(d))
