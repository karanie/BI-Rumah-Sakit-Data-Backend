from flask import Blueprint
from ..data import dataset as d
import config
from datastore.rdbms import DatastoreDB

routes_test = Blueprint("routes_test", __name__)
@routes_test.route("/api/test/seeddb", methods=["POST"])
def seed_ma_db_in_wet_rice_field():
    dt = DatastoreDB(backend="pandas")
    dt.write_database(d, config.DB_TABLE, config.DB_CONNECTION)
    return "success"
