import json
import pandas as pd

from web3 import HTTPProvider
from web3._utils.request import make_post_request
from multithread_processing.base_job import BaseJob


def _generate_get_code_json_rpc(contract_addresses, block='latest'):
    for idx, contract_address in enumerate(contract_addresses):
        yield _generate_json_rpc(
            method='eth_getCode',
            params=[contract_address, hex(block) if isinstance(block, int) else block],
            request_id=idx
        )


def _generate_json_rpc(method, params, request_id=1):
    return {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': request_id,
    }


class _BatchHTTPProvider(HTTPProvider):
    def make_batch_request(self, text):
        self.logger.debug("Making request HTTP. URI: %s, Request: %s",
                          self.endpoint_uri, text)
        request_data = text.encode('utf-8')
        raw_response = make_post_request(
            self.endpoint_uri,
            request_data,
            **self.get_request_kwargs()
        )
        response = self.decode_rpc_response(raw_response)
        self.logger.debug("Getting response HTTP. URI: %s, "
                          "Request: %s, Response: %s",
                          self.endpoint_uri, text, response)
        return response


def _rpc_response_to_result(response):
    try:
        result = response.get('result')
        return result
    except Exception as e:
        print(e)


def check_if_contracts(addresses: list[str],
                       provider_url: str='https://bsc-dataseed3.ninicoin.io/') -> dict[str, bool]:
    """
    The check_if_contracts function takes a list of addresses and returns a dictionary with the same keys as the input
    list, but with values that are booleans indicating whether or not each address is a contract.
    Args:
        addresses: list[str]: Specify the list of addresses to check
        provider_url: str: Specify the url of the node to connect to
    Returns:
        A dictionary with the address as the key and a boolean as value
    Doc Author:
        T
    """
    returned_dict = {addr.lower(): True for addr in addresses}
    json_rpc = list(_generate_get_code_json_rpc(returned_dict.keys()))

    _batch_provider = _BatchHTTPProvider(provider_url)
    response_batch = _batch_provider.make_batch_request(json.dumps(json_rpc))

    for response in response_batch:
        request_id = response['id']  # request id is the index of the contract in contracts list
        if not response.get('result'):
            returned_dict[addresses[request_id]] = False
        bytecode = _rpc_response_to_result(response)
        if bytecode == "0x":
            returned_dict[addresses[request_id]] = False

    return returned_dict



class detectContract(BaseJob):
    def __init__(self, output_path, listIndex: list,
                 max_workers= 4, batch_size=1000):

        self.isContract = dict()
        self.output = output_path
        #self.subgraph = list()

        
        super().__init__(work_iterable=listIndex,
                         max_workers=max_workers,
                         batch_size=batch_size)

    def _execute_batch(self, works):
        try: 
            contract = check_if_contracts(addresses=works)
        except Exception as e:
            print(e)
        self.isContract.update(contract)

    def _end(self):
        super()._end()
        df = pd.DataFrame(list(self.isContract.items()), columns=['address', 'IsContract'])

        df.to_csv(f"{self.output}", index = 'False')     
