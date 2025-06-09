import os

ALLOWED_EXTENSIONS = os.environ.get("ALLOWED_EXTENSIONS", "csv;xlsx").split(";")
INIT_SOURCE_PATH = os.path.join(os.getcwd(), os.environ.get("INIT_SOURCE_PATH", "data/dataset/DC1.csv.gz"))
DATASTORE_FILE_PATH = os.path.join(os.getcwd(), os.environ.get("DATASTORE_FILE_PATH", "data/dataset/DC1.pkl.gz"))
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "data/dataset/uploaded/")
DB_TABLE = os.environ.get("DB_TABLE", "dataset")
DB_CONNECTION = os.environ.get("DB_CONNECTION", "sqlite://")
PORT = int(os.environ.get("PORT", "5000"))
API_TO_POLL = os.environ.get("API_TO_POLL", "http://127.0.0.1:8000/api/data/dummy")
API_KEY = os.environ.get("API_KEY", "")
