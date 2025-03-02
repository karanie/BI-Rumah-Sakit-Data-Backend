from flask import Blueprint
from ..data import dataset as d
import config
from datastore.rdbms import upsert_df

routes_test = Blueprint("routes_test", __name__)
@routes_test.route("/api/test/seeddb", methods=["POST"])
def seed_ma_db_in_wet_rice_field():
    upsert_df(d.dataset, config.DB_TABLE, config.DB_CONNECTION)
    return "success"
