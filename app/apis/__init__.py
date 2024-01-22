from sanic import Blueprint

from app.apis.example_blueprint import example
from app.apis.get_blueprint import bp as get_blueprint

api = Blueprint.group(example, get_blueprint)