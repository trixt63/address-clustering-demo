from typing import Optional
from pydantic import BaseModel


class AddressQuery(BaseModel):
    chain: Optional[str] = None
    address: str

#
# address_json_schema = {
#     'type': 'object',
#     'properties': {
#         'chainId': {'type': 'string'},
#         'address': {'type': 'string'},
#     },
#     'required': ['chainId', 'address']
# }
