from sanic.response import json
from sanic_ext import openapi, validate
from sanic import Blueprint
from sanic.response import json
import random

from app.decorators.json_validator import validate_with_jsonschema
from app.models.address import AddressQuery

bp = Blueprint('get_same_owner', url_prefix='/')


@bp.get('/same-owner')
@openapi.tag("Get")
@openapi.summary("Get addresses with the same owner")
@openapi.parameter(name="chain", schema=str, description=f"Chain ID", location="query")
@openapi.parameter(name="address", schema=str, description=f"Address", required=True, location="query")
@validate(query=AddressQuery)
async def get_courses(request, query: AddressQuery):
    chain_id = query.chain
    address = str(query.address)

    _n_addresses = random.randint(1, 5)
    addresses = [address] * _n_addresses
    return json(addresses)
