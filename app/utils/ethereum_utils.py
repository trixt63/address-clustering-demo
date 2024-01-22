import json
import itertools
from typing import List
import time

from app.constants.network_constants import Chains, Networks
from app.utils.utils import chunks
from app.services.blockchain.providers.rpc import BatchHTTPProvider
from app.services.blockchain.json_rpc_requests import generate_get_code_json_rpc
from app.services.blockchain.utils import rpc_response_to_result


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def filter_out_contracts(addresses: List[str], chain_id: str = None, provider_url: str = None,
                         batch_size: int = 100, sleep_time: float = 5) -> List[str]:
    """Filter contracts in a list of addresses"""
    if not provider_url:
        _chain_name = Chains.names[chain_id]
        provider_url = Networks.providers[_chain_name]
    _batch_provider = BatchHTTPProvider(provider_url)

    address_is_contract = {addr: True for addr in addresses}
    for addresses_chunk in chunks(input_list=addresses, size=batch_size):
        addresses_code_rpc = list(generate_get_code_json_rpc(addresses_chunk))
        response_batch = _batch_provider.make_batch_request(json.dumps(addresses_code_rpc))

        for response in response_batch:
            request_id = response['id']  # request id is the index of the address in contracts list
            address = addresses_chunk[request_id]
            if not response.get('result'):
                address_is_contract[address] = False
            bytecode = rpc_response_to_result(response)
            if bytecode == "0x":
                address_is_contract[address] = False
        # sleep to avoid exceeding public RPC limit
        time.sleep(sleep_time)

    return [addr for addr in address_is_contract.keys()
            if address_is_contract[addr] is False]
