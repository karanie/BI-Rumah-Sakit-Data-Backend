import gc
from flask import Flask
from flask_cors import CORS
from .utils.optimization import malloc_trim
from config import UPLOAD_FOLDER

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
CORS(app)

@app.after_request
def malloc_trim_after_request(res):
    malloc_trim(0)
    gc.collect()
    return res

from .cache import cache
cache.init_app(app)

from .routes import init_app
init_app(app)
