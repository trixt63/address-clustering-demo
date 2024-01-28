import logging

from sanic.response import json
from sanic_ext import openapi, validate
from sanic import Blueprint
from sanic.response import json
import random
import pandas as pd
from rapidfuzz.distance import JaroWinkler
from itertools import product

from app.decorators.json_validator import validate_with_jsonschema
from app.models.address import AddressQuery
from app.services.generate_pairs import PairsGenerator
from app.databases.arangodb_klg import ArangoDB

bp = Blueprint('get_same_owner', url_prefix='/')

pairs_generators = {
    '0x38': PairsGenerator(chain_id='0x38'),
    '0x1': PairsGenerator(chain_id='0x1')
}

arangos = {
    '0x38': ArangoDB(prefix='bsc'),
    '0x1': ArangoDB(prefix='ethereum')
}


@bp.get('/same-owner')
@openapi.tag("Get")
@openapi.summary("Get addresses with the same owner")
@openapi.parameter(name="chain", schema=str, description="Chain ID", location="query")
@openapi.parameter(name="address", schema=str, description="Address", required=True, location="query")
@validate(query=AddressQuery)
async def get_courses(request, query: AddressQuery):
    chain_id = query.chain
    address = str(query.address)

    returned_result = {
        'data': {},
        'message': '',
        'error': 0
    }

    # Heuristics

    input_wallet = arangos[chain_id].get_address(address)
    if not input_wallet:
        returned_result['data']['heuristic'] = []
        returned_result['message'] = 'Address not present in graph'
    elif input_wallet.get('numberSent', 0) > 500:
        returned_result['data']['heuristic'] = []
        returned_result['message'] = 'Address is likely a bot'
    elif input_wallet.get('wallet', {}):
        returned_result['data']['heuristic'] = []
        returned_result['message'] = 'Address is hot wallet or deposit wallet'
    else:
        # deposit reuse
        deposit_wallets = list(arangos[chain_id].get_deposit_address(input_wallet['address']))
        if not deposit_wallets:
            returned_result['data']['heuristic'] = []
            returned_result['message'] = 'Address does not use CEX'
        else:
            deposit_addresses = [w['address'] for w in deposit_wallets]
            same_users = arangos[chain_id].get_user_addresses(deposit_addresses)
            same_users = set(same_users)
            same_users.remove(address)
            returned_result['data']['heuristic'] = same_users
            returned_result['message'] = 'Successfully retrieve wallets'

        # BNS
        same_users_by_bns: set[str] = set()
        if input_wallet.get('names'):
            neighbors_with_names = list(arangos[chain_id].get_neighbors_with_names(address=address))
            if neighbors_with_names:
                for neighbor in neighbors_with_names:
                    name_similarity, _ = calculate_max_similarity(input_wallet['names'], neighbor['names'])
                    if name_similarity > 0.8:
                        same_users_by_bns.add(neighbor['address'])

        returned_result['data']['heuristic'].extend(list(same_users_by_bns))
        returned_result['data']['heuristic'] = list(set(returned_result['data']['heuristic']))

    return json(returned_result)

    # # check bot
    #
    # #
    # # _n_addresses = random.randint(1, 5)
    # addresses = [address] * _n_addresses
    # return json(addresses)


def calculate_max_similarity(names1: list, names2: list) -> tuple[float, list]:
    paired_names = list(product(names1, names2))
    pairs_sim = [JaroWinkler.normalized_similarity(pair[0], pair[1])
                 for pair in paired_names]
    try:
        max_value, max_index = max((value, index) for index, value in enumerate(pairs_sim))
        return max_value, paired_names[max_index]
    except ValueError as ex:
        logging.exception(ex)
        return 0, []
